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


SCHEMA = """
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
"""


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
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(path))
        self._con.execute(SCHEMA)

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
            " http_validators FROM stream_state WHERE source = ? AND stream = ?",
            [source, stream],
        ).fetchone()
        if row is None:
            return StreamState(source, stream)
        kind, value, last_success, last_deep, validators = row
        return StreamState(
            source=source,
            stream=stream,
            watermark=Watermark.deserialize(kind, value) if kind else None,
            last_success_at=_from_db(last_success),
            last_deep_refetch_at=_from_db(last_deep),
            http_validators=json.loads(validators) if validators else {},
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
                "INSERT OR REPLACE INTO stream_state VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    state.source,
                    state.stream,
                    wm.kind.value if wm else None,
                    wm.serialize() if wm else None,
                    _to_db(state.last_success_at),
                    _to_db(state.last_deep_refetch_at),
                    json.dumps(state.http_validators),
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
        self._finish(run_id, "failed", stats, finished_at, watermark_before, None, error)

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
