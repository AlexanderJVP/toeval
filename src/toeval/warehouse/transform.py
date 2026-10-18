"""Rebuild staging and marts from raw, then run the quality checks (docs/decisions/0001, 0006).

Everything downstream of raw is a pure function of raw, so a transform is always a full rebuild:
each model's SQL (in `sql/`) runs in an in-memory DuckDB over Parquet views and is written to
one Parquet file, replaced atomically. SQL files only reference view names
(`raw_<source>_<stream>`, `stg_*`, `mart_*`), never paths, so they port to another engine.

Quality checks are SQL too (`sql/quality/<check>.sql`, returning subject/value/details). They warn
instead of failing: the data is what the source published, and hard checks already ran at
ingestion. Results are logged and stored in the state DB (`quality_results`).
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path

import duckdb

from toeval.config import Settings
from toeval.state import QualityResult, StateStore

log = logging.getLogger(__name__)

SQL = files("toeval.warehouse") / "sql"


@dataclass(frozen=True)
class Model:
    name: str  # view name for downstream SQL and the output file name
    layer: str  # "staging" | "marts"
    sql: str  # path under sql/
    inputs: tuple[str, ...]  # views it reads


@dataclass(frozen=True)
class RawInput:
    source: str
    stream: str

    @property
    def view(self) -> str:
        return f"raw_{self.source}_{self.stream}"


RAW_INPUTS = (RawInput("elia", "load"), RawInput("elia", "wind"), RawInput("elia", "solar"))

MODELS = (
    Model("stg_elia_load", "staging", "staging/elia_load.sql", ("raw_elia_load",)),
    Model("stg_elia_wind", "staging", "staging/elia_wind.sql", ("raw_elia_wind",)),
    Model("stg_elia_solar", "staging", "staging/elia_solar.sql", ("raw_elia_solar",)),
    Model(
        "mart_energy_15min",
        "marts",
        "marts/energy_15min.sql",
        ("stg_elia_load", "stg_elia_solar", "stg_elia_wind"),
    ),
    Model("mart_energy_hourly", "marts", "marts/energy_hourly.sql", ("mart_energy_15min",)),
)


@dataclass(frozen=True)
class Check:
    name: str  # sql/quality/<name>.sql
    max_ok: float  # value <= max_ok is ok, otherwise a warning
    unit: str
    inputs: tuple[str, ...]


def checks(revision_window: timedelta) -> tuple[Check, ...]:
    stg = ("stg_elia_load", "stg_elia_wind", "stg_elia_solar")
    raw = tuple(r.view for r in RAW_INPUTS)
    return (
        Check("duplicate_keys", 0, "rows", stg),
        Check("grid_gaps", 0, "quarter-hours", stg),
        Check("measured_holes", 0, "quarter-hours", ("mart_energy_15min",)),
        Check("wind_parts", 0, "quarter-hours", ("mart_energy_15min",)),
        # Wind and solar end at the last completed Brussels day, fetched daily at 12:00.
        Check("freshness", 72, "hours", ("mart_energy_15min",)),
        # Warn once revisions reach 80% of the window: the window may then be too short.
        Check("revision_lag", 0.8 * revision_window / timedelta(days=1), "days", raw),
    )


@dataclass(frozen=True)
class TransformResult:
    built: tuple[str, ...]
    skipped: tuple[str, ...]  # models whose inputs don't exist yet (stream never ingested)
    quality: tuple[QualityResult, ...]

    @property
    def warnings(self) -> tuple[QualityResult, ...]:
        return tuple(q for q in self.quality if not q.ok)


def outputs_missing(settings: Settings) -> bool:
    """True if a model that *can* be built (its raw inputs exist) has no output yet. Models of
    streams that were never ingested don't count, or run-due would transform on every tick."""
    available = {r.view for r in RAW_INPUTS if _has_raw(settings, r)}
    for model in MODELS:
        if set(model.inputs) <= available:
            if not _output_path(settings, model).exists():
                return True
            available.add(model.name)
    return False


def _has_raw(settings: Settings, raw: RawInput) -> bool:
    return any((settings.raw_path / raw.source / raw.stream).rglob("*.parquet"))


def transform(
    settings: Settings,
    store: StateStore,
    *,
    now: datetime,
    revision_window: timedelta = timedelta(days=14),
) -> TransformResult:
    started = time.monotonic()
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        available = _register_raw(con, settings)
        built, skipped = [], []
        for model in MODELS:
            if not set(model.inputs) <= available:
                log.warning("transform: skipping %s (no %s yet)", model.name, model.inputs)
                skipped.append(model.name)
                continue
            rows = _build(con, settings, model)
            available.add(model.name)
            built.append(model.name)
            log.info("transform: %s: %d rows", model.name, rows)

        con.execute("SET VARIABLE now = ?", [now.isoformat()])
        results = []
        for check in checks(revision_window):
            if set(check.inputs) <= available:
                results.extend(_run_check(con, check, now))
    finally:
        con.close()

    store.add_quality_results(results)
    for q in results:
        level = logging.INFO if q.ok else logging.WARNING
        log.log(level, "quality %s %s: %s", q.check, q.subject, q.summary)
    log.info("transform: done in %.1fs", time.monotonic() - started)
    return TransformResult(tuple(built), tuple(skipped), tuple(results))


def _register_raw(con: duckdb.DuckDBPyConnection, settings: Settings) -> set[str]:
    available = set()
    for raw in RAW_INPUTS:
        if not _has_raw(settings, raw):
            continue
        directory = settings.raw_path / raw.source / raw.stream
        # Only raw/: quarantined batches live elsewhere and are never read here.
        glob = (directory / "**" / "*.parquet").as_posix()
        con.execute(
            f"CREATE VIEW {raw.view} AS SELECT * FROM read_parquet("
            f"'{_quote(glob)}', filename = true, union_by_name = true, hive_partitioning = false)"
        )
        available.add(raw.view)
    return available


def _build(con: duckdb.DuckDBPyConnection, settings: Settings, model: Model) -> int:
    path = _output_path(settings, model)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    query = (SQL / model.sql).read_text(encoding="utf-8")
    con.execute(f"COPY ({query}) TO '{_quote(tmp.as_posix())}' (FORMAT parquet, COMPRESSION zstd)")
    os.replace(tmp, path)
    con.execute(
        f"CREATE OR REPLACE VIEW {model.name} AS "
        f"SELECT * FROM read_parquet('{_quote(path.as_posix())}')"
    )
    (rows,) = con.execute(f"SELECT count(*) FROM {model.name}").fetchone()  # type: ignore[misc]
    return int(rows)


def _run_check(con: duckdb.DuckDBPyConnection, check: Check, now: datetime) -> list[QualityResult]:
    query = (SQL / "quality" / f"{check.name}.sql").read_text(encoding="utf-8")
    rows = con.execute(f"SELECT subject, value, details FROM ({query})").fetchall()
    return [
        QualityResult(
            checked_at=now,
            check=check.name,
            subject=subject,
            value=None if value is None else float(value),
            unit=check.unit,
            ok=value is not None and value <= check.max_ok,
            details=details or "",
        )
        for subject, value, details in rows
    ]


def _output_path(settings: Settings, model: Model) -> Path:
    root = settings.staging_path if model.layer == "staging" else settings.marts_path
    return root / f"{model.name}.parquet"


def _quote(s: str) -> str:
    return s.replace("'", "''")


__all__ = ["MODELS", "TransformResult", "checks", "outputs_missing", "transform"]
