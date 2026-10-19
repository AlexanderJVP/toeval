"""The connector interface. See docs/decisions/0004-connector-interface.md.

Subclasses describe a source (`spec`) and implement three source-specific steps:

    plan()           what to request this run (time windows, stations, files, ...)
    fetch()          one request -> Arrow table, or "not modified"
    next_watermark() how a fetched batch moves the stream's progress marker

Everything else (HTTP retries/backoff/rate limiting, auth, conditional GET, raw writes, quality
gates, run logging and the all-or-nothing state commit) lives in `Connector.run`.
"""

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar

import httpx
import pyarrow as pa

from toeval.config import Settings
from toeval.connectors.http import HttpClient
from toeval.connectors.spec import SourceSpec, StreamSpec
from toeval.events import NULL_SINK, EventSink
from toeval.state import RunStats, StateStore, StreamState, Watermark
from toeval.warehouse.raw import RawWriter

log = logging.getLogger(__name__)


class RunMode(StrEnum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class UnsupportedModeError(ValueError):
    pass


class QualityGateError(RuntimeError):
    """A batch failed a hard check; nothing from the run is committed."""


@dataclass(frozen=True)
class RunContext:
    now: datetime
    mode: RunMode
    deep_refetch: bool = False
    backfill_from: datetime | None = None
    backfill_to: datetime | None = None


@dataclass(frozen=True)
class FetchRequest:
    """One unit of fetching. `key` must be stable across runs for the same logical request
    (it identifies conditional-GET validators and raw-file versions). Requests within one run
    must cover disjoint natural keys."""

    key: str
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FetchResult:
    table: pa.Table | None  # None: the server said nothing changed (304)

    @classmethod
    def not_modified(cls) -> "FetchResult":
        return cls(None)


@dataclass(frozen=True)
class StreamRunResult:
    source: str
    stream: str
    run_id: str
    succeeded: bool
    stats: RunStats
    watermark_before: Watermark | None
    watermark_after: Watermark | None
    error: str | None = None


Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Connector(ABC):
    spec: ClassVar[SourceSpec]

    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        *,
        spec: SourceSpec | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Clock = _utcnow,
        http_options: Mapping[str, Any] | None = None,
        events: EventSink = NULL_SINK,
    ) -> None:
        self.settings = settings
        self.store = store
        self.source = spec or type(self).spec  # instance copy may carry sources.toml overrides
        self._raw = RawWriter(settings.raw_path, settings.quarantine_path, store)
        self._transport = transport
        self._clock = clock
        self._http_options = dict(http_options or {})  # test hooks: sleep, clock, rng, environ
        self._events = events

    # -- source-specific steps ----------------------------------------------------------------

    @abstractmethod
    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        """Requests for this run. Use `planning.time_window_plan` for time-series streams."""

    @abstractmethod
    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        """Perform one request through `http` and return its rows as an Arrow table."""

    @abstractmethod
    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        """Watermark implied by a batch (the base class never lets it move backwards)."""

    def validate(self, stream: StreamSpec, table: pa.Table) -> list[str]:
        """Source-specific hard checks; return failure messages. Generic checks always run."""
        return []

    # -- the run loop --------------------------------------------------------------------------

    def run(
        self,
        stream_name: str,
        mode: RunMode = RunMode.INCREMENTAL,
        *,
        backfill_from: datetime | None = None,
        backfill_to: datetime | None = None,
    ) -> StreamRunResult:
        stream = self.source.stream(stream_name)
        if mode is RunMode.BACKFILL and stream.backfill_start is None:
            raise UnsupportedModeError(
                f"{self.source.name}/{stream.name} has no queryable history; "
                "an incremental run already fetches everything the source offers"
            )

        now = self._clock()
        state = self.store.get_stream_state(self.source.name, stream.name)
        deep = self._deep_refetch_due(stream, state, now, mode)
        ctx = RunContext(now, mode, deep, backfill_from, backfill_to)
        run_id = self.store.start_run(self.source.name, stream.name, mode.value, now)
        before = state.watermark
        log.info(
            "%s/%s: %s run %s (watermark %s%s)",
            self.source.name,
            stream.name,
            mode.value,
            run_id[:8],
            before.serialize() if before else "none",
            ", deep re-fetch" if deep else "",
        )

        started = time.monotonic()
        ids = {"run": run_id, "source": self.source.name, "stream": stream.name}
        self._events.emit(
            "run_started",
            **ids,
            mode=mode.value,
            deep=deep,
            watermark=before.serialize() if before else None,
        )
        stats = _MutableStats()
        validators = {k: dict(v) for k, v in state.http_validators.items()}
        http: HttpClient | None = None
        # Everything after start_run is inside the try, so no run is ever left 'running'.
        try:
            http = HttpClient(
                self.source.http,
                self.source.auth,
                user_agent=self.settings.user_agent,
                validators=validators,
                transport=self._transport,
                **self._http_options,
            )
            watermark = before
            requests = self.plan(stream, state, ctx)
            self._events.emit("planned", **ids, requests=len(requests))
            for index, request in enumerate(requests, 1):
                progress = {**ids, "key": request.key, "index": index, "total": len(requests)}
                self._events.emit("request_started", **progress)
                result = self.fetch(stream, request, http)
                if result.table is None:
                    stats.not_modified += 1
                    self._events.emit("request_done", **progress, rows=0, outcome="not_modified")
                    continue
                table = result.table
                stats.rows_fetched += table.num_rows
                failures = _generic_checks(stream, table) + self.validate(stream, table)
                if failures:
                    self._events.emit(
                        "request_done", **progress, rows=table.num_rows, outcome="quarantined"
                    )
                    path = self._raw.quarantine(
                        self.source.name,
                        stream.name,
                        table,
                        run_id=run_id,
                        request_key=request.key,
                        ingested_at=now,
                    )
                    raise QualityGateError(
                        f"request {request.key}: " + "; ".join(failures) + f" (quarantined: {path})"
                    )
                written = self._raw.write(
                    self.source.name,
                    stream.name,
                    table,
                    run_id=run_id,
                    request_key=request.key,
                    ingested_at=now,
                )
                stats.files_written += written is not None
                self._events.emit(
                    "request_done",
                    **progress,
                    rows=table.num_rows,
                    outcome="written" if written else "unchanged",
                    file=written.path if written else None,
                )
                if table.num_rows:
                    watermark = _max_watermark(
                        watermark, self.next_watermark(stream, watermark, table)
                    )
        except BaseException as exc:
            stats.requests = http.requests_sent if http else 0
            return self._fail(stream, run_id, stats, before, exc, started=started)
        finally:
            if http is not None:
                http.close()

        stats.requests = http.requests_sent
        # A backfill of an old range must not make the scheduled run look done, nor clear the
        # retry backoff of a stream whose incremental runs keep failing.
        incremental = mode is RunMode.INCREMENTAL
        new_state = StreamState(
            source=self.source.name,
            stream=stream.name,
            watermark=watermark,
            last_success_at=now if incremental else state.last_success_at,
            last_deep_refetch_at=now if deep else state.last_deep_refetch_at,
            http_validators=validators,
            consecutive_failures=0 if incremental else state.consecutive_failures,
            last_failure_at=None if incremental else state.last_failure_at,
        )
        try:
            self.store.finish_run_success(run_id, new_state, stats.freeze(), self._clock(), before)
        except BaseException as exc:  # the store rolled back; record the run as failed instead
            return self._fail(
                stream, run_id, stats, before, exc, "state commit failed: ", started=started
            )
        log.info(
            "%s/%s: run %s ok: %d requests (%d not modified), %d rows, %d files, watermark %s",
            self.source.name,
            stream.name,
            run_id[:8],
            stats.requests,
            stats.not_modified,
            stats.rows_fetched,
            stats.files_written,
            watermark.serialize() if watermark else "none",
        )
        self._finished(stream, run_id, stats, started, "success", watermark=watermark)
        return StreamRunResult(
            self.source.name, stream.name, run_id, True, stats.freeze(), before, watermark
        )

    def _fail(
        self,
        stream: StreamSpec,
        run_id: str,
        stats: "_MutableStats",
        before: Watermark | None,
        exc: BaseException,
        prefix: str = "",
        *,
        started: float,
    ) -> StreamRunResult:
        """Mark the run failed. KeyboardInterrupt / SystemExit are recorded, then re-raised."""
        error = f"{prefix}{type(exc).__name__}: {exc}"
        log.exception("%s/%s: run %s failed", self.source.name, stream.name, run_id[:8])
        self.store.finish_run_failed(run_id, stats.freeze(), self._clock(), before, error)
        self._finished(stream, run_id, stats, started, "failed", watermark=before, error=error)
        if not isinstance(exc, Exception):
            raise exc
        return StreamRunResult(
            self.source.name, stream.name, run_id, False, stats.freeze(), before, before, error
        )

    def _finished(
        self,
        stream: StreamSpec,
        run_id: str,
        stats: "_MutableStats",
        started: float,
        status: str,
        *,
        watermark: Watermark | None,
        error: str | None = None,
    ) -> None:
        self._events.emit(
            "run_finished",
            run=run_id,
            source=self.source.name,
            stream=stream.name,
            status=status,
            rows=stats.rows_fetched,
            files=stats.files_written,
            duration_s=round(time.monotonic() - started, 1),
            watermark=watermark.serialize() if watermark else None,
            error=error,
        )

    @staticmethod
    def _deep_refetch_due(
        stream: StreamSpec, state: StreamState, now: datetime, mode: RunMode
    ) -> bool:
        if mode is not RunMode.INCREMENTAL or stream.deep_refetch is None:
            return False
        if state.watermark is None:  # first run fetches full history anyway; count it as deep
            return True
        last = state.last_deep_refetch_at
        return last is None or now - last >= stream.deep_refetch.every


@dataclass
class _MutableStats:
    requests: int = 0
    not_modified: int = 0
    rows_fetched: int = 0
    files_written: int = 0

    def freeze(self) -> RunStats:
        return RunStats(self.requests, self.not_modified, self.rows_fetched, self.files_written)


def _max_watermark(a: Watermark | None, b: Watermark | None) -> Watermark | None:
    if a is None or b is None:
        return a or b
    if a.kind is not b.kind:
        raise ValueError(f"watermark kind changed from {a.kind} to {b.kind}")
    return b if b.value > a.value else a  # type: ignore[operator]


def _generic_checks(stream: StreamSpec, table: pa.Table) -> list[str]:
    """Checks that hold for every stream. Fuller, logged quality checks arrive in step 5."""
    missing = [c for c in stream.natural_key if c not in table.column_names]
    if missing:
        return [f"natural key columns missing: {missing}"]
    if table.num_rows == 0:
        return []
    failures = []
    for col in stream.natural_key:
        nulls = table[col].null_count
        if nulls:
            failures.append(f"{nulls} null values in key column {col!r}")
    keys = table.select(list(stream.natural_key))
    distinct = keys.group_by(list(stream.natural_key)).aggregate([]).num_rows
    if distinct != table.num_rows:
        failures.append(f"{table.num_rows - distinct} duplicate natural keys")
    return failures


__all__ = [
    "Connector",
    "FetchRequest",
    "FetchResult",
    "QualityGateError",
    "RunContext",
    "RunMode",
    "StreamRunResult",
    "UnsupportedModeError",
]
