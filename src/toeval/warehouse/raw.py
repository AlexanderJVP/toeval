"""Immutable raw layer: one Parquet file per fetched batch, never modified afterwards.

Layout: <raw>/<source>/<stream>/ingest_date=YYYY-MM-DD/<HHMMSS>_<run>_<hash>.parquet
Each file carries `_ingested_at` and `_request_key` columns so staging can keep the latest version
of every natural key. A batch is skipped only when it provably adds nothing (see
`StateStore.is_unchanged`), which makes re-runs cheap without ever losing the latest observation.
"""

import hashlib
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from toeval.state import RawFileEntry, StateStore

log = logging.getLogger(__name__)

INGESTED_AT = "_ingested_at"
REQUEST_KEY = "_request_key"


def content_hash(table: pa.Table) -> str:
    """Stable hash of a table's schema and values, independent of how it is chunked."""
    sink = pa.BufferOutputStream()
    combined = table.combine_chunks()
    with pa.ipc.new_stream(sink, combined.schema) as writer:
        writer.write_table(combined)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


class RawWriter:
    def __init__(self, raw_root: Path, store: StateStore) -> None:
        self._root = raw_root
        self._store = store

    def write(
        self,
        source: str,
        stream: str,
        table: pa.Table,
        *,
        run_id: str,
        request_key: str,
        ingested_at: datetime,
    ) -> RawFileEntry | None:
        """Persist a batch. Returns None when it would add no information."""
        digest = content_hash(table)
        if self._store.is_unchanged(source, stream, request_key, digest, run_id):
            log.info(
                "%s/%s %s: unchanged since last fetch, not written", source, stream, request_key
            )
            return None

        path = self._path(source, stream, ingested_at, run_id, digest)
        _write_atomic(path, _with_metadata(table, ingested_at, request_key))
        entry = RawFileEntry(
            path=path.relative_to(self._root).as_posix(),
            source=source,
            stream=stream,
            run_id=run_id,
            request_key=request_key,
            ingested_at=ingested_at,
            content_hash=digest,
            row_count=table.num_rows,
        )
        self._store.add_raw_file(entry)
        return entry

    def quarantine(
        self, source: str, stream: str, table: pa.Table, *, run_id: str, ingested_at: datetime
    ) -> Path:
        """Keep a batch that failed hard quality checks for inspection, outside the raw layer."""
        path = self._path(source, stream, ingested_at, run_id, content_hash(table), "_quarantine")
        _write_atomic(path, table)
        return path

    def _path(
        self,
        source: str,
        stream: str,
        ingested_at: datetime,
        run_id: str,
        digest: str,
        prefix: str = "",
    ) -> Path:
        ts = ingested_at.astimezone(UTC)
        base = self._root / prefix if prefix else self._root
        return (
            base
            / source
            / stream
            / f"ingest_date={ts:%Y-%m-%d}"
            / f"{ts:%H%M%S}_{run_id[:8]}_{digest[:12]}.parquet"
        )


def _with_metadata(table: pa.Table, ingested_at: datetime, request_key: str) -> pa.Table:
    n = table.num_rows
    ts = pa.array([ingested_at.astimezone(UTC)] * n, type=pa.timestamp("us", tz="UTC"))
    return table.append_column(INGESTED_AT, ts).append_column(
        REQUEST_KEY, pa.array([request_key] * n, type=pa.string())
    )


def _write_atomic(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)
