"""Rebuild staging and marts from raw, then run the quality checks (docs/decisions/0001, 0006).

Everything downstream of raw is a pure function of raw, so a transform is always a full rebuild:
each model's SQL (in `sql/`) runs in an in-memory DuckDB over Parquet views and is written to
one Parquet file, replaced atomically. SQL files only reference view names
(`raw_<source>_<stream>`, `stg_*`, `mart_*`, and `ref_*` reference data registered from
`calendar.py`), never paths, so they port to another engine.

Quality checks are SQL too (`sql/quality/<source>/<check>.sql`, returning subject/value/details,
where subject is "<source>/<stream>"). They warn instead of failing: the data is what the source
published, and hard checks already ran at ingestion. Thresholds are fixed per check or come from
each stream's spec (`freshness_limit`, `revision_lag_limit`). Results are logged and stored in the
state DB (`quality_results`).
"""

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path

import duckdb

from toeval.config import Settings
from toeval.connectors.spec import StreamSpec
from toeval.events import NULL_SINK, EventSink
from toeval.state import QualityResult, StateStore
from toeval.warehouse import calendar

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


RAW_INPUTS = (
    RawInput("elia", "load"),
    RawInput("elia", "wind"),
    RawInput("elia", "solar"),
    RawInput("openmeteo", "hourly"),
    RawInput("irceline", "hourly"),
)

MODELS = (
    Model("stg_elia_load", "staging", "staging/elia_load.sql", ("raw_elia_load",)),
    Model("stg_elia_wind", "staging", "staging/elia_wind.sql", ("raw_elia_wind",)),
    Model("stg_elia_solar", "staging", "staging/elia_solar.sql", ("raw_elia_solar",)),
    Model(
        "stg_openmeteo_hourly",
        "staging",
        "staging/openmeteo_hourly.sql",
        ("raw_openmeteo_hourly",),
    ),
    Model(
        "stg_irceline_hourly",
        "staging",
        "staging/irceline_hourly.sql",
        ("raw_irceline_hourly",),
    ),
    Model(
        "mart_energy_15min",
        "marts",
        "marts/energy_15min.sql",
        ("stg_elia_load", "stg_elia_solar", "stg_elia_wind"),
    ),
    Model("mart_energy_hourly", "marts", "marts/energy_hourly.sql", ("mart_energy_15min",)),
    Model("mart_weather_hourly", "marts", "marts/weather_hourly.sql", ("stg_openmeteo_hourly",)),
    Model("mart_air_hourly", "marts", "marts/air_hourly.sql", ("stg_irceline_hourly",)),
    Model(
        "mart_energy_weather_hourly",
        "marts",
        "marts/energy_weather_hourly.sql",
        ("mart_energy_hourly", "mart_weather_hourly", "mart_air_hourly"),
    ),
    Model("mart_calendar_day", "marts", "marts/calendar_day.sql", calendar.REFERENCE_VIEWS),
    Model(
        "mart_daily",
        "marts",
        "marts/daily.sql",
        ("mart_energy_weather_hourly", "mart_calendar_day"),
    ),
    Model("mart_weekly", "marts", "marts/weekly.sql", ("mart_daily",)),
)

# Check name -> subject ("<source>/<stream>") -> the highest value that is still ok.
Limits = Mapping[str, Mapping[str, float]]
PER_STREAM = None  # Check.max_ok of checks whose threshold comes from each stream's spec


@dataclass(frozen=True)
class Check:
    source: str  # sql/quality/<source>/<name>.sql
    name: str
    unit: str
    inputs: tuple[str, ...]
    max_ok: float | None = 0  # value <= max_ok is ok, otherwise a warning

    def limit(self, subject: str, limits: Limits) -> float | None:
        """None: no limit is known for this subject (its SQL and stream names disagree)."""
        if self.max_ok is not None:
            return self.max_ok
        return limits.get(self.name, {}).get(subject)


_ELIA_STG = ("stg_elia_load", "stg_elia_wind", "stg_elia_solar")
CHECKS = (
    Check("elia", "duplicate_keys", "rows", _ELIA_STG),
    Check("elia", "grid_gaps", "quarter-hours", _ELIA_STG),
    Check("elia", "measured_holes", "quarter-hours", ("mart_energy_15min",)),
    Check("elia", "wind_parts", "quarter-hours", ("mart_energy_15min",)),
    Check("elia", "freshness", "hours", ("mart_energy_15min",), PER_STREAM),
    Check(
        "elia",
        "revision_lag",
        "days",
        ("raw_elia_load", "raw_elia_wind", "raw_elia_solar"),
        PER_STREAM,
    ),
    Check("openmeteo", "duplicate_keys", "rows", ("stg_openmeteo_hourly",)),
    Check("openmeteo", "grid_gaps", "hours", ("stg_openmeteo_hourly",)),
    Check("openmeteo", "freshness", "hours", ("stg_openmeteo_hourly",), PER_STREAM),
    Check("openmeteo", "revision_lag", "days", ("raw_openmeteo_hourly",), PER_STREAM),
    Check("irceline", "duplicate_keys", "rows", ("stg_irceline_hourly",)),
    Check("irceline", "freshness", "hours", ("stg_irceline_hourly",), PER_STREAM),
    Check("irceline", "stale_series", "series", ("stg_irceline_hourly",)),
    Check("irceline", "coverage", "series", ("stg_irceline_hourly",)),
    Check("irceline", "value_range", "values", ("stg_irceline_hourly",)),
    Check(
        "irceline",
        "no_data_codes",
        "values",
        ("raw_irceline_hourly", "stg_irceline_hourly", "mart_air_hourly"),
    ),
    Check("irceline", "revision_lag", "days", ("raw_irceline_hourly",), PER_STREAM),
    Check("calendar", "school_holidays", "days", ("ref_school_holidays_flanders",)),
)


def quality_limits(streams: Mapping[str, StreamSpec]) -> dict[str, dict[str, float]]:
    """The per-stream thresholds from each stream's (effective) spec, keyed like the checks'
    subjects: "<source>/<stream>"."""
    limits: dict[str, dict[str, float]] = {"freshness": {}, "revision_lag": {}}
    for subject, spec in streams.items():
        limits["freshness"][subject] = spec.freshness_limit / timedelta(hours=1)
        if (lag := spec.revision_lag_max()) is not None:
            limits["revision_lag"][subject] = lag / timedelta(days=1)
    return limits


def default_limits() -> dict[str, dict[str, float]]:
    """The limits of the connectors' default specs (without sources.toml overrides)."""
    from toeval.connectors.registry import CONNECTORS

    return quality_limits(
        {f"{name}/{s.name}": s for name, cls in CONNECTORS.items() for s in cls.spec.streams}
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
    if available:  # the calendar can always be built, but alone it isn't worth a transform
        available |= set(calendar.REFERENCE_VIEWS)
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
    limits: Limits | None = None,
    events: EventSink = NULL_SINK,
) -> TransformResult:
    """Rebuild every model, run the checks, and record the attempt in `transforms`.

    All outputs are written to temporary files first and only then swapped in, so a failure
    (e.g. another program holding a mart file open on Windows) never leaves fresh staging next to
    stale marts. A failure is recorded and re-raised; `Pipeline.needs_transform` retries it.

    `limits` are the per-stream quality thresholds (`quality_limits`); default: the connectors'.
    """
    limits = default_limits() if limits is None else limits
    started = time.monotonic()

    def finished() -> datetime:  # `now` is the start; the caller's clock may be a fake one
        return now + timedelta(seconds=time.monotonic() - started)

    transform_id = store.start_transform(now)
    events.emit("transform_started", models=len(MODELS))
    built: list[str] = []
    try:
        result = _transform(settings, store, now, limits, built, events)
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        store.finish_transform(transform_id, finished(), models_built=0, error=error)
        events.emit(
            "transform_finished",
            status="failed",
            duration_s=round(time.monotonic() - started, 1),
            error=error,
        )
        raise
    store.finish_transform(transform_id, finished(), models_built=len(built))
    duration = time.monotonic() - started
    events.emit(
        "transform_finished",
        status="success",
        duration_s=round(duration, 1),
        built=len(result.built),
        checks=len(result.quality),
        warnings=len(result.warnings),
    )
    log.info("transform: done in %.1fs", duration)
    return result


def _transform(
    settings: Settings,
    store: StateStore,
    now: datetime,
    limits: Limits,
    built: list[str],
    events: EventSink,
) -> TransformResult:
    staged: list[tuple[Path, Path]] = []  # (tmp, final), swapped in after everything succeeded
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        available = _register_raw(con, settings) | calendar.register(con)
        skipped = []
        for index, model in enumerate(MODELS, 1):
            progress = {"model": model.name, "layer": model.layer, "index": index}
            if not set(model.inputs) <= available:
                log.warning("transform: skipping %s (no %s yet)", model.name, model.inputs)
                skipped.append(model.name)
                events.emit("model_skipped", **progress)
                continue
            tmp, rows = _build(con, settings, model)
            staged.append((tmp, _output_path(settings, model)))
            available.add(model.name)
            built.append(model.name)
            log.info("transform: %s: %d rows", model.name, rows)
            events.emit("model_built", **progress, inputs=list(model.inputs), rows=rows)

        con.execute("SET VARIABLE now = ?", [now.isoformat()])
        results = []
        for check in CHECKS:
            if set(check.inputs) <= available:
                results.extend(_run_check(con, check, now, limits))
    except BaseException:
        con.close()
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)
        raise
    con.close()  # release the temporary files before moving them

    try:
        _swap(staged)
    finally:
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)
    store.add_quality_results(results)
    for q in results:
        level = logging.INFO if q.ok else logging.WARNING
        log.log(level, "quality %s %s: %s", q.check, q.subject, q.summary)
    return TransformResult(tuple(built), tuple(skipped), tuple(results))


def _swap(staged: list[tuple[Path, Path]]) -> None:
    """Move every new output in, or none of them.

    Each current output is renamed to `.bak` first; on Windows that fails while another program
    holds the file open, before anything changed. Only then are the new files moved in, and any
    error restores the backups.
    """
    backups: list[tuple[Path, Path]] = []  # (bak, final)
    moved: list[Path] = []
    try:
        for _, final in staged:
            if final.exists():
                bak = final.with_name(final.name + ".bak")
                os.replace(final, bak)
                backups.append((bak, final))
        for tmp, final in staged:
            os.replace(tmp, final)
            moved.append(final)
    except BaseException:
        for final in moved:
            final.unlink(missing_ok=True)
        for bak, final in backups:
            os.replace(bak, final)
        raise
    for bak, _ in backups:
        bak.unlink(missing_ok=True)


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


def _build(con: duckdb.DuckDBPyConnection, settings: Settings, model: Model) -> tuple[Path, int]:
    """Write the model to a temporary file; downstream models read it from there."""
    path = _output_path(settings, model)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    query = (SQL / model.sql).read_text(encoding="utf-8")
    con.execute(f"COPY ({query}) TO '{_quote(tmp.as_posix())}' (FORMAT parquet, COMPRESSION zstd)")
    con.execute(
        f"CREATE OR REPLACE VIEW {model.name} AS "
        f"SELECT * FROM read_parquet('{_quote(tmp.as_posix())}')"
    )
    (rows,) = con.execute(f"SELECT count(*) FROM {model.name}").fetchone()  # type: ignore[misc]
    return tmp, int(rows)


def _run_check(
    con: duckdb.DuckDBPyConnection, check: Check, now: datetime, limits: Limits
) -> list[QualityResult]:
    query = (SQL / "quality" / check.source / f"{check.name}.sql").read_text(encoding="utf-8")
    rows = con.execute(f"SELECT subject, value, details FROM ({query})").fetchall()
    results = []
    for subject, value, details in rows:
        limit = check.limit(subject, limits)
        if limit is None:
            # A bug, not a data problem, but checks never block the warehouse (ADR 0006): warn.
            log.error("quality %s: no limit for subject %r (not a <source>/<stream>?)",
                      check.name, subject)  # fmt: skip
            details = f"no limit for this subject; {details or ''}"
        results.append(
            QualityResult(
                checked_at=now,
                check=check.name,
                subject=subject,
                value=None if value is None else float(value),
                unit=check.unit,
                ok=limit is not None and value is not None and value <= limit,
                details=details or "",
            )
        )
    return results


def _output_path(settings: Settings, model: Model) -> Path:
    root = settings.staging_path if model.layer == "staging" else settings.marts_path
    return root / f"{model.name}.parquet"


def _quote(s: str) -> str:
    return s.replace("'", "''")


__all__ = [
    "CHECKS",
    "MODELS",
    "TransformResult",
    "default_limits",
    "outputs_missing",
    "quality_limits",
    "transform",
]
