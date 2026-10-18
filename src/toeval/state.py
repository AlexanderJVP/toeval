"""Pipeline state: per-stream watermarks, the run log and the raw-file manifest.

Stored in its own DuckDB file so the warehouse can be rebuilt without losing ingestion history.
All timestamps are stored as naive UTC and converted to aware UTC at this boundary.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb


class WatermarkKind(StrEnum):
    TIMESTAMP = "timestamp"  # time-series sources (Elia, Open-Meteo, Telraam)
    PERIOD = "period"  # reference periods like "2026-09" (Statbel); compared as strings
    NONE = "none"  # live snapshots without a progress marker (iRail)


@dataclass(frozen=True)
class Watermark:
    kind: WatermarkKind
    value: datetime | str

    def __post_init__(self) -> None:
        if self.kind is WatermarkKind.TIMESTAMP:
            if not isinstance(self.value, datetime) or self.value.tzinfo is None:
                raise ValueError("timestamp watermark needs a timezone-aware datetime")
        elif self.kind is WatermarkKind.PERIOD:
            if not isinstance(self.value, str):
                raise ValueError("period watermark needs a string value")
        else:
            raise ValueError("WatermarkKind.NONE streams have no watermark; use None")

    @classmethod
    def timestamp(cls, value: datetime) -> Self:
        return cls(WatermarkKind.TIMESTAMP, value.astimezone(UTC))

    @classmethod
    def period(cls, value: str) -> Self:
        return cls(WatermarkKind.PERIOD, value)

    def serialize(self) -> str:
        return self.value.isoformat() if isinstance(self.value, datetime) else self.value

    @classmethod
    def deserialize(cls, kind: str, value: str) -> Self:
        k = WatermarkKind(kind)
        if k is WatermarkKind.TIMESTAMP:
            return cls(k, datetime.fromisoformat(value).astimezone(UTC))
        return cls(k, value)


@dataclass
class StreamState:
    source: str
    stream: str
    watermark: Watermark | None = None
    last_success_at: datetime | None = None
    last_deep_refetch_at: datetime | None = None
    # Per request key: {"etag": ..., "last_modified": ...} for conditional GETs.
    http_validators: dict[str, dict[str, str]] = field(default_factory=dict)
    # Failed incremental runs since the last success; drives the retry backoff in run-due.
    consecutive_failures: int = 0
    last_failure_at: datetime | None = None


@dataclass(frozen=True)
class RawFileEntry:
    path: str
    source: str
    stream: str
    run_id: str
    request_key: str
    ingested_at: datetime
    content_hash: str
    row_count: int


@dataclass(frozen=True)
class QualityResult:
    """One quality check on one subject (e.g. `grid_gaps` on `elia/wind`) at one transform."""

    checked_at: datetime
    check: str
    subject: str
    value: float | None
    unit: str
    ok: bool
    details: str = ""

    @property
    def summary(self) -> str:
        value = "n/a" if self.value is None else f"{self.value:g} {self.unit}"
        status = "ok" if self.ok else "WARN"
        return f"{status}, {value}" + (f" ({self.details})" if self.details else "")


# Ordered schema migrations. Each runs once, in a transaction, and bumps `schema_version`.
# Never edit a released step; append a new one. Steps are idempotent (IF NOT EXISTS) because
# databases created before versioning existed already contain some of them.
MIGRATIONS: tuple[tuple[int, str, str], ...] = (
    (
        1,
        "baseline: stream state, run log, raw manifest",
        """
        CREATE TABLE IF NOT EXISTS stream_state (
            source VARCHAR NOT NULL,
            stream VARCHAR NOT NULL,
            watermark_kind VARCHAR,
            watermark_value VARCHAR,
            last_success_at TIMESTAMP,
            last_deep_refetch_at TIMESTAMP,
            http_validators JSON,
            PRIMARY KEY (source, stream)
        );
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            stream VARCHAR NOT NULL,
            mode VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR NOT NULL,  -- running | success | failed
            requests INTEGER,
            not_modified INTEGER,
            rows_fetched BIGINT,
            files_written INTEGER,
            watermark_before VARCHAR,
            watermark_after VARCHAR,
            error VARCHAR
        );
        CREATE TABLE IF NOT EXISTS raw_files (
            path VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            stream VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            request_key VARCHAR NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            content_hash VARCHAR NOT NULL,
            row_count BIGINT NOT NULL
        );
        """,
    ),
    (
        2,
        "retry backoff (ADR 0005)",
        """
        ALTER TABLE stream_state ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER DEFAULT 0;
        ALTER TABLE stream_state ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMP;
        """,
    ),
    (
        3,
        "quality results (ADR 0006)",
        """
        CREATE TABLE IF NOT EXISTS quality_results (
            checked_at TIMESTAMP NOT NULL,
            check_name VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            value DOUBLE,
            unit VARCHAR NOT NULL,
            ok BOOLEAN NOT NULL,
            details VARCHAR NOT NULL
        );
        """,
    ),
    (
        4,
        "transform log",
        """
        CREATE TABLE IF NOT EXISTS transforms (
            transform_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR NOT NULL,  -- running | success | failed
            models_built INTEGER,
            error VARCHAR
        );
        """,
    ),
)
SCHEMA_VERSION = MIGRATIONS[-1][0]

# A run or transform still marked 'running' when a new process holds the lock was killed
# (power loss, Task Scheduler's time limit) before it could record its end.
ORPHANED = "process ended without recording a result (killed, crashed or power loss)"


@dataclass(frozen=True)
class TransformStatus:
    last_success_started_at: datetime | None
    last_attempt_at: datetime | None
    last_status: str | None
    last_error: str | None
    consecutive_failures: int  # failed transforms since the last successful one
    last_failure_at: datetime | None


def _to_db(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        raise ValueError("refusing to store a naive datetime; pass an aware UTC timestamp")
    return ts.astimezone(UTC).replace(tzinfo=None)


def _from_db(ts: datetime | None) -> datetime | None:
    return None if ts is None else ts.replace(tzinfo=UTC)


@dataclass(frozen=True)
class RunStats:
    requests: int = 0
    not_modified: int = 0
    rows_fetched: int = 0
    files_written: int = 0


class StateStore:
    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        """Open the state DB. DuckDB allows one writing process at a time; opening fails with
        `duckdb.IOException` while another process (a running ingest) holds the file."""
        if read_only:
            self._con = duckdb.connect(str(path), read_only=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(path))
        self._migrate()

    def _migrate(self) -> None:
        self._con.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
        current = self.schema_version()
        if current > SCHEMA_VERSION:
            raise RuntimeError(
                f"state DB has schema version {current}, newer than this code ({SCHEMA_VERSION})"
            )
        for version, _description, sql in MIGRATIONS:
            if version <= current:
                continue
            self._con.execute("BEGIN TRANSACTION")
            try:
                self._con.execute(sql)
                self._con.execute("DELETE FROM schema_version")
                self._con.execute("INSERT INTO schema_version VALUES (?)", [version])
                self._con.execute("COMMIT")
            except BaseException:
                self._con.execute("ROLLBACK")
                raise

    def schema_version(self) -> int:
        """0 for a database from before versioning (or a read-only one never migrated)."""
        try:
            row = self._con.execute("SELECT max(version) FROM schema_version").fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0]) if row and row[0] is not None else 0

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- stream state -------------------------------------------------------------------------

    def get_stream_state(self, source: str, stream: str) -> StreamState:
        row = self._con.execute(
            "SELECT watermark_kind, watermark_value, last_success_at, last_deep_refetch_at,"
            " http_validators, consecutive_failures, last_failure_at"
            " FROM stream_state WHERE source = ? AND stream = ?",
            [source, stream],
        ).fetchone()
        if row is None:
            return StreamState(source, stream)
        kind, value, last_success, last_deep, validators, failures, last_failure = row
        return StreamState(
            source=source,
            stream=stream,
            watermark=Watermark.deserialize(kind, value) if kind else None,
            last_success_at=_from_db(last_success),
            last_deep_refetch_at=_from_db(last_deep),
            http_validators=json.loads(validators) if validators else {},
            consecutive_failures=failures or 0,
            last_failure_at=_from_db(last_failure),
        )

    # -- runs -----------------------------------------------------------------------------------

    def start_run(self, source: str, stream: str, mode: str, started_at: datetime) -> str:
        run_id = uuid.uuid4().hex
        self._con.execute(
            "INSERT INTO runs (run_id, source, stream, mode, started_at, status)"
            " VALUES (?, ?, ?, ?, ?, 'running')",
            [run_id, source, stream, mode, _to_db(started_at)],
        )
        return run_id

    def finish_run_success(
        self,
        run_id: str,
        state: StreamState,
        stats: RunStats,
        finished_at: datetime,
        watermark_before: Watermark | None,
    ) -> None:
        """Commit the new stream state and mark the run successful, atomically."""
        wm = state.watermark
        self._con.execute("BEGIN TRANSACTION")
        try:
            self._con.execute(
                "INSERT OR REPLACE INTO stream_state (source, stream, watermark_kind,"
                " watermark_value, last_success_at, last_deep_refetch_at, http_validators,"
                " consecutive_failures, last_failure_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    state.source,
                    state.stream,
                    wm.kind.value if wm else None,
                    wm.serialize() if wm else None,
                    _to_db(state.last_success_at),
                    _to_db(state.last_deep_refetch_at),
                    json.dumps(state.http_validators),
                    state.consecutive_failures,
                    _to_db(state.last_failure_at),
                ],
            )
            self._finish(run_id, "success", stats, finished_at, watermark_before, wm, None)
            self._con.execute("COMMIT")
        except BaseException:  # incl. KeyboardInterrupt: never leave the transaction open
            self._con.execute("ROLLBACK")
            raise

    def finish_run_failed(
        self,
        run_id: str,
        stats: RunStats,
        finished_at: datetime,
        watermark_before: Watermark | None,
        error: str,
    ) -> None:
        """Mark the run failed. A failed *incremental* run also counts towards the stream's retry
        backoff; a failed manual backfill doesn't, so it never delays the scheduled runs."""
        self._con.execute("BEGIN TRANSACTION")
        try:
            self._finish(run_id, "failed", stats, finished_at, watermark_before, None, error)
            self._count_failure(run_id, finished_at)
            self._con.execute("COMMIT")
        except BaseException:
            self._con.execute("ROLLBACK")
            raise

    def _count_failure(self, run_id: str, failed_at: datetime) -> None:
        """Count a failed *incremental* run towards its stream's retry backoff."""
        self._con.execute(
            "INSERT INTO stream_state (source, stream, consecutive_failures, last_failure_at)"
            " SELECT source, stream, 1, ? FROM runs WHERE run_id = ? AND mode = 'incremental'"
            " ON CONFLICT (source, stream) DO UPDATE SET"
            " consecutive_failures = coalesce(stream_state.consecutive_failures, 0) + 1,"
            " last_failure_at = excluded.last_failure_at",
            [_to_db(failed_at), run_id],
        )

    def _finish(
        self,
        run_id: str,
        status: str,
        stats: RunStats,
        finished_at: datetime,
        before: Watermark | None,
        after: Watermark | None,
        error: str | None,
    ) -> None:
        self._con.execute(
            "UPDATE runs SET finished_at = ?, status = ?, requests = ?, not_modified = ?,"
            " rows_fetched = ?, files_written = ?, watermark_before = ?, watermark_after = ?,"
            " error = ? WHERE run_id = ?",
            [
                _to_db(finished_at),
                status,
                stats.requests,
                stats.not_modified,
                stats.rows_fetched,
                stats.files_written,
                before.serialize() if before else None,
                after.serialize() if after else None,
                error,
                run_id,
            ],
        )

    def get_run(self, run_id: str) -> dict[str, object]:
        cur = self._con.execute("SELECT * FROM runs WHERE run_id = ?", [run_id])
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            raise KeyError(run_id)
        return dict(zip(cols, row, strict=True))

    # -- raw manifest ---------------------------------------------------------------------------

    def is_unchanged(
        self, source: str, stream: str, request_key: str, content_hash: str, current_run: str
    ) -> bool:
        """True if writing this batch again would add no information.

        That holds only when the latest file for this request key has the same content AND no
        other run wrote to the stream since. Otherwise an overlapping window could have stored a
        different value for the same rows in between (A -> B -> A must keep the last A). Files of
        the current run are ignored: requests within one run cover disjoint keys by contract.
        """
        latest = self._con.execute(
            "SELECT content_hash, run_id, ingested_at, path FROM raw_files"
            " WHERE source = ? AND stream = ? AND request_key = ?"
            " ORDER BY ingested_at DESC, path DESC LIMIT 1",
            [source, stream, request_key],
        ).fetchone()
        if latest is None or latest[0] != content_hash:
            return False
        _, latest_run, latest_at, _ = latest
        newer = self._con.execute(
            "SELECT 1 FROM raw_files WHERE source = ? AND stream = ? AND ingested_at >= ?"
            " AND run_id NOT IN (?, ?) LIMIT 1",
            [source, stream, latest_at, latest_run, current_run],
        ).fetchone()
        return newer is None

    def add_raw_file(self, entry: RawFileEntry) -> None:
        self._con.execute(
            "INSERT INTO raw_files VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                entry.path,
                entry.source,
                entry.stream,
                entry.run_id,
                entry.request_key,
                _to_db(entry.ingested_at),
                entry.content_hash,
                entry.row_count,
            ],
        )

    def abort_orphaned(self, now: datetime) -> int:
        """Mark runs and transforms left 'running' by a killed process as failed.

        Only call this while holding the ingest lock: then nothing else can be running. An
        orphaned incremental run counts towards the stream's retry backoff like any failure.
        """
        orphans = [
            r[0]
            for r in self._con.execute(
                "SELECT run_id FROM runs WHERE status = 'running'"
            ).fetchall()
        ]
        (transforms,) = self._con.execute(
            "SELECT count(*) FROM transforms WHERE status = 'running'"
        ).fetchone()  # type: ignore[misc]
        self._con.execute("BEGIN TRANSACTION")
        try:
            for run_id in orphans:
                self._count_failure(run_id, now)
            for table in ("runs", "transforms"):
                self._con.execute(
                    f"UPDATE {table} SET status = 'failed', finished_at = ?, error = ?"
                    " WHERE status = 'running'",
                    [_to_db(now), ORPHANED],
                )
            self._con.execute("COMMIT")
        except BaseException:
            self._con.execute("ROLLBACK")
            raise
        return len(orphans) + int(transforms)

    # -- transforms -------------------------------------------------------------------------------

    def start_transform(self, started_at: datetime) -> str:
        transform_id = uuid.uuid4().hex
        self._con.execute(
            "INSERT INTO transforms (transform_id, started_at, status) VALUES (?, ?, 'running')",
            [transform_id, _to_db(started_at)],
        )
        return transform_id

    def finish_transform(
        self,
        transform_id: str,
        finished_at: datetime,
        *,
        models_built: int,
        error: str | None = None,
    ) -> None:
        self._con.execute(
            "UPDATE transforms SET finished_at = ?, status = ?, models_built = ?, error = ?"
            " WHERE transform_id = ?",
            [
                _to_db(finished_at),
                "failed" if error else "success",
                models_built,
                error,
                transform_id,
            ],
        )

    def transform_status(self) -> TransformStatus:
        (success_at,) = self._con.execute(
            "SELECT max(started_at) FROM transforms WHERE status = 'success'"
        ).fetchone()  # type: ignore[misc]
        last = self._con.execute(
            "SELECT started_at, status, error FROM transforms"
            " WHERE status <> 'running' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        failures, last_failure = self._con.execute(
            "SELECT count(*), max(finished_at) FROM transforms"
            " WHERE status = 'failed' AND (?::TIMESTAMP IS NULL OR started_at > ?::TIMESTAMP)",
            [success_at, success_at],
        ).fetchone()  # type: ignore[misc]
        return TransformStatus(
            last_success_started_at=_from_db(success_at),
            last_attempt_at=_from_db(last[0]) if last else None,
            last_status=last[1] if last else None,
            last_error=last[2] if last else None,
            consecutive_failures=int(failures),
            last_failure_at=_from_db(last_failure),
        )

    def latest_raw_ingest(self) -> datetime | None:
        (latest,) = self._con.execute("SELECT max(ingested_at) FROM raw_files").fetchone()  # type: ignore[misc]
        return _from_db(latest)

    # -- quality ----------------------------------------------------------------------------------

    def add_quality_results(self, results: list[QualityResult]) -> None:
        if not results:
            return
        self._con.executemany(
            "INSERT INTO quality_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                [_to_db(r.checked_at), r.check, r.subject, r.value, r.unit, r.ok, r.details]
                for r in results
            ],
        )

    def latest_quality_results(self) -> list[QualityResult]:
        """The results of the most recent transform."""
        rows = self._con.execute(
            "SELECT checked_at, check_name, subject, value, unit, ok, details FROM quality_results"
            " WHERE checked_at = (SELECT max(checked_at) FROM quality_results)"
            " ORDER BY check_name, subject"
        ).fetchall()
        return [QualityResult(_from_db(t), *rest) for t, *rest in rows]  # type: ignore[arg-type]

    def raw_files(self, source: str, stream: str) -> list[RawFileEntry]:
        rows = self._con.execute(
            "SELECT path, source, stream, run_id, request_key, ingested_at, content_hash, row_count"
            " FROM raw_files WHERE source = ? AND stream = ? ORDER BY ingested_at, path",
            [source, stream],
        ).fetchall()
        return [
            RawFileEntry(p, s, st, r, k, _from_db(t), h, n)  # type: ignore[arg-type]
            for p, s, st, r, k, t, h, n in rows
        ]
