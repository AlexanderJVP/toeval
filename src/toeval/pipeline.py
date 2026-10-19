"""Runs connectors: which streams are due, sources.toml overrides, and the retry backoff.

The CLI wraps this with the single-writer lock and logging; this module never takes the lock.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from toeval.config import Settings
from toeval.connectors import registry
from toeval.connectors.base import Connector, RunMode, StreamRunResult
from toeval.connectors.spec import SourceSpec, StreamSpec, load_overrides
from toeval.events import NULL_SINK, EventSink
from toeval.state import StateStore, StreamState
from toeval.warehouse.transform import (
    TransformResult,
    default_limits,
    outputs_missing,
    quality_limits,
    transform,
)

log = logging.getLogger(__name__)

# After a failed incremental run, wait 5 min, 10, 20, ... up to 6 h before retrying, so a broken
# source (or a failing quality gate on a full-history load) isn't re-fetched every 5 minutes.
RETRY_BASE = timedelta(minutes=5)
RETRY_MAX = timedelta(hours=6)

Clock = Callable[[], datetime]


def retry_delay(consecutive_failures: int) -> timedelta:
    if consecutive_failures <= 0:
        return timedelta(0)
    return min(RETRY_BASE * 2 ** min(consecutive_failures - 1, 16), RETRY_MAX)


def next_due(stream: StreamSpec, state: StreamState) -> datetime | None:
    """When the stream is next due; None means right away (it has never succeeded or failed).

    Due = the schedule's next moment after the last success, but not before the retry backoff
    after the last failure has passed.
    """
    due = None
    if state.last_success_at is not None:
        due = stream.schedule.next_after(state.last_success_at)
    if state.consecutive_failures and state.last_failure_at is not None:
        retry = state.last_failure_at + retry_delay(state.consecutive_failures)
        due = retry if due is None else max(due, retry)
    return due


def is_stream_due(stream: StreamSpec, state: StreamState, now: datetime) -> bool:
    if stream.paused:
        return False
    due = next_due(stream, state)
    return due is None or now >= due


def resolve_specs(
    overrides: Mapping[str, Any], connectors: Mapping[str, type[Connector]], origin: object
) -> dict[str, SourceSpec]:
    """Every source's spec with its sources.toml overrides applied. Raises ValueError for unknown
    sources or invalid values, so a typo fails before any run starts."""
    unknown = set(overrides) - set(connectors)
    if unknown:
        raise ValueError(f"{origin}: unknown sources {sorted(unknown)}")
    specs = {}
    for name, cls in connectors.items():
        source_overrides = overrides.get(name)
        if source_overrides is not None and not isinstance(source_overrides, Mapping):
            raise ValueError(f"{origin}: [{name}] must be a table")
        specs[name] = (
            cls.spec.with_overrides(dict(source_overrides)) if source_overrides else cls.spec
        )
    return specs


@dataclass(frozen=True)
class StreamStatus:
    source: str
    stream: str
    state: StreamState
    next_due: datetime | None


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Pipeline:
    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        *,
        clock: Clock = _utcnow,
        connectors: Mapping[str, type[Connector]] | None = None,
        events: EventSink = NULL_SINK,
    ) -> None:
        self._settings = settings
        self._store = store
        self._clock = clock
        self._events = events
        self._connectors = dict(registry.CONNECTORS if connectors is None else connectors)
        self._specs = resolve_specs(
            load_overrides(settings.sources_path), self._connectors, settings.sources_path
        )

    @property
    def sources(self) -> list[str]:
        return list(self._connectors)

    def spec(self, source: str) -> SourceSpec:
        try:
            return self._specs[source]
        except KeyError:
            raise KeyError(f"unknown source {source!r}; known: {self.sources}") from None

    def default_spec(self, source: str) -> SourceSpec:
        """The connector's own spec, before sources.toml overrides."""
        self.spec(source)  # KeyError for an unknown source
        return self._connectors[source].spec

    def connector(self, source: str) -> Connector:
        spec = self.spec(source)
        return self._connectors[source](
            self._settings, self._store, spec=spec, clock=self._clock, events=self._events
        )

    def run(
        self,
        source: str,
        streams: Sequence[str] | None = None,
        mode: RunMode = RunMode.INCREMENTAL,
        *,
        backfill_from: datetime | None = None,
        backfill_to: datetime | None = None,
    ) -> list[StreamRunResult]:
        spec = self.spec(source)
        names = list(streams) if streams else [s.name for s in spec.streams]
        for name in names:
            spec.stream(name)  # unknown stream names fail before anything runs
        connector = self.connector(source)
        return [
            connector.run(name, mode, backfill_from=backfill_from, backfill_to=backfill_to)
            for name in names
        ]

    def due(self) -> list[tuple[str, str]]:
        now = self._clock()
        return [
            (source, stream.name)
            for source in self.sources
            for stream in self.spec(source).streams
            if is_stream_due(stream, self._store.get_stream_state(source, stream.name), now)
        ]

    def run_due(self) -> list[StreamRunResult]:
        results = []
        due = self.due()
        if not due:
            log.debug("nothing due")
        for source in self.sources:
            streams = [stream for src, stream in due if src == source]
            if streams:
                results.extend(self.run(source, streams))
        return results

    def needs_transform(self, results: Sequence[StreamRunResult]) -> bool:
        """Rebuild when this run wrote raw files, or when the warehouse is behind raw: an output
        is missing, or raw changed since the last *successful* transform (e.g. it failed).
        Retrying a failed transform follows the same backoff as failing streams."""
        if any(r.stats.files_written for r in results):
            return True
        status = self._store.transform_status()
        latest_raw = self._store.latest_raw_ingest()
        behind = outputs_missing(self._settings) or (
            latest_raw is not None
            and (
                status.last_success_started_at is None
                or latest_raw > status.last_success_started_at
            )
        )
        if not behind:
            return False
        if status.consecutive_failures and status.last_failure_at is not None:
            retry_at = status.last_failure_at + retry_delay(status.consecutive_failures)
            return self._clock() >= retry_at
        return True

    def transform(self) -> TransformResult:
        # The quality thresholds follow the effective specs, so sources.toml overrides count.
        limits = default_limits()
        own = quality_limits(
            {f"{source}/{s.name}": s for source in self.sources for s in self.spec(source).streams}
        )
        for check, per_subject in own.items():
            limits[check] = limits.get(check, {}) | per_subject
        return transform(
            self._settings, self._store, now=self._clock(), events=self._events, limits=limits
        )

    def status(self) -> list[StreamStatus]:
        out = []
        for source in self.sources:
            for stream in self.spec(source).streams:
                state = self._store.get_stream_state(source, stream.name)
                out.append(StreamStatus(source, stream.name, state, next_due(stream, state)))
        return out


__all__ = [
    "Pipeline",
    "StreamStatus",
    "is_stream_due",
    "next_due",
    "resolve_specs",
    "retry_delay",
]
