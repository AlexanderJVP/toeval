"""Staging, marts and quality checks over synthetic raw files with Elia's real schemas."""

import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from toeval.config import Settings
from toeval.connectors.elia import WIND_PARTS
from toeval.state import StateStore
from toeval.warehouse.raw import RawWriter, content_hash
from toeval.warehouse.transform import TransformResult, outputs_missing, transform

FIXTURES = Path(__file__).parent / "fixtures" / "elia"
SCHEMAS = {s: pq.read_schema(FIXTURES / f"{d}.parquet") for s, d in
           (("load", "ods001"), ("wind", "ods031"), ("solar", "ods032"))}  # fmt: skip
Q = timedelta(minutes=15)
DAY0 = datetime(2026, 9, 30, 22, tzinfo=UTC)  # 2026-10-01 00:00 Brussels
SOLAR_REGIONS = {"Flanders": 0.7, "Wallonia": 0.25, "Brussels": 0.05}
SOLAR_PROVINCES = ("Antwerp", "Limburg")  # a subset is enough for the hierarchy tests

Value = Callable[[datetime], float | None]


def quarters(start: datetime, end: datetime) -> list[datetime]:
    out, t = [], start
    while t < end:
        out.append(t)
        t += Q
    return out


def table(stream: str, rows: list[dict]) -> pa.Table:
    schema = SCHEMAS[stream]
    full = [
        {name: row.get(name) for name in schema.names} | {"resolutioncode": "PT15M"} for row in rows
    ]
    return pa.Table.from_pylist(full, schema=schema)


def load_rows(start: datetime, end: datetime, value: Value) -> pa.Table:
    return table(
        "load",
        [
            {"datetime": t, "totalload": value(t), "dayaheadforecast": 1000.0}
            for t in quarters(start, end)
        ],
    )


def wind_rows(start: datetime, end: datetime, value: Value, *, skip: set | None = None) -> pa.Table:
    rows = []
    for t in quarters(start, end):
        for part in sorted(WIND_PARTS):
            if skip and (t, part) in skip:
                continue
            offshore, region, grid = part
            rows.append({"datetime": t, "offshoreonshore": offshore, "region": region,
                         "gridconnectiontype": grid, "measured": value(t)})  # fmt: skip
    return table("wind", rows)


def solar_rows(start: datetime, end: datetime, value: Value) -> pa.Table:
    rows = []
    for t in quarters(start, end):
        total = value(t)
        rows.append({"datetime": t, "region": "Belgium", "measured": total})
        for region, share in SOLAR_REGIONS.items():
            rows.append(
                {
                    "datetime": t,
                    "region": region,
                    "measured": None if total is None else total * share,
                }
            )
        for province in SOLAR_PROVINCES:
            rows.append(
                {
                    "datetime": t,
                    "region": province,
                    "measured": None if total is None else total * 0.1,
                }
            )
    return table("solar", rows)


class Raw:
    """Writes batches the way connector runs do: one run per call, via RawWriter."""

    def __init__(self, settings: Settings, store: StateStore) -> None:
        self.settings = settings
        self.store = store
        self.writer = RawWriter(settings.raw_path, settings.quarantine_path, store)

    def write(self, stream: str, batch: pa.Table, *, at: datetime, key: str | None = None) -> None:
        self.writer.write(
            "elia",
            stream,
            batch,
            run_id=uuid.uuid4().hex,
            request_key=key or f"{at}",
            ingested_at=at,
        )

    def all_streams(self, start: datetime, end: datetime, value: Value, *, at: datetime) -> None:
        self.write("load", load_rows(start, end, value), at=at)
        self.write("wind", wind_rows(start, end, value), at=at)
        self.write("solar", solar_rows(start, end, value), at=at)


@pytest.fixture
def raw(settings: Settings, store: StateStore) -> Raw:
    return Raw(settings, store)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    yield c
    c.close()


def run(settings: Settings, store: StateStore, now: datetime, **kwargs: object) -> TransformResult:
    return transform(settings, store, now=now, **kwargs)  # type: ignore[arg-type]


def query(con: duckdb.DuckDBPyConnection, settings: Settings, model: str, sql: str) -> list[tuple]:
    layer = settings.staging_path if model.startswith("stg_") else settings.marts_path
    path = (layer / f"{model}.parquet").as_posix()
    return con.execute(sql.replace("{model}", f"read_parquet('{path}')")).fetchall()


def quality(result: TransformResult, check: str, subject: str):  # type: ignore[no-untyped-def]
    (q,) = [q for q in result.quality if q.check == check and q.subject == subject]
    return q


def constant(v: float) -> Value:
    return lambda _t: v


# -- staging -------------------------------------------------------------------------------------


def test_latest_ingested_version_wins_even_after_a_b_a(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.write("load", load_rows(DAY0, day1, constant(100.0)), at=day1, key="first")
    raw.write(
        "load", load_rows(DAY0, day1, constant(105.0)), at=day1 + timedelta(days=1), key="overlap"
    )
    raw.write(
        "load", load_rows(DAY0, day1, constant(100.0)), at=day1 + timedelta(days=2), key="first"
    )

    run(settings, store, day1 + timedelta(days=2))

    assert query(
        con, settings, "stg_elia_load", "SELECT DISTINCT load_mw, count(*) FROM {model} GROUP BY 1"
    ) == [(100.0, 96)]


def test_quarantined_batches_are_never_read(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.write("load", load_rows(DAY0, day1, constant(100.0)), at=day1)
    raw.writer.quarantine(
        "elia", "load", load_rows(DAY0, day1, constant(-1.0)),
        run_id="q", request_key="bad", ingested_at=day1 + timedelta(hours=1),
    )  # fmt: skip

    run(settings, store, day1)

    assert query(con, settings, "stg_elia_load", "SELECT min(load_mw) FROM {model}") == [(100.0,)]


def test_rebuild_is_idempotent(settings: Settings, store: StateStore, raw: Raw) -> None:
    raw.all_streams(DAY0, DAY0 + timedelta(days=2), constant(50.0), at=DAY0 + timedelta(days=2))
    now = DAY0 + timedelta(days=2)

    run(settings, store, now)
    first = {
        p.name: content_hash(pq.read_table(p)) for p in settings.data_path.rglob("*_elia_*.parquet")
    }
    first |= {p.name: content_hash(pq.read_table(p)) for p in settings.marts_path.glob("*.parquet")}
    run(settings, store, now)
    second = {
        p.name: content_hash(pq.read_table(p)) for p in settings.data_path.rglob("*_elia_*.parquet")
    }
    second |= {
        p.name: content_hash(pq.read_table(p)) for p in settings.marts_path.glob("*.parquet")
    }

    assert first == second
    assert len(second) == 5
    assert not list(settings.data_path.rglob("*.tmp"))


def test_streams_not_ingested_yet_are_skipped(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    raw.write("load", load_rows(DAY0, DAY0 + timedelta(days=1), constant(1.0)), at=DAY0)

    result = run(settings, store, DAY0)

    assert result.built == ("stg_elia_load",)
    assert "mart_energy_15min" in result.skipped
    assert result.quality == ()


# -- marts ---------------------------------------------------------------------------------------


def test_national_solar_is_the_belgium_row_never_the_sum_of_all_rows(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    raw.all_streams(DAY0, DAY0 + timedelta(days=1), constant(400.0), at=DAY0 + timedelta(days=1))

    run(settings, store, DAY0 + timedelta(days=1))

    assert query(
        con,
        settings,
        "mart_energy_15min",
        "SELECT DISTINCT solar_mw, wind_mw, load_mw FROM {model}",
    ) == [
        (400.0, 2000.0, 400.0)  # wind: 5 parts x 400
    ]


def test_wind_is_null_unless_all_five_parts_are_present(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    gap = DAY0 + timedelta(hours=6)
    raw.write(
        "wind",
        wind_rows(
            DAY0,
            DAY0 + timedelta(days=1),
            constant(10.0),
            skip={(gap, ("Offshore", "Federal", "Elia"))},
        ),
        at=DAY0 + timedelta(days=1),
    )
    raw.write(
        "load",
        load_rows(DAY0, DAY0 + timedelta(days=1), constant(1.0)),
        at=DAY0 + timedelta(days=1),
    )
    raw.write(
        "solar",
        solar_rows(DAY0, DAY0 + timedelta(days=1), constant(1.0)),
        at=DAY0 + timedelta(days=1),
    )

    result = run(settings, store, DAY0 + timedelta(days=1))

    rows = query(
        con,
        settings,
        "mart_energy_15min",
        "SELECT wind_mw, wind_parts_present, count(*) FROM {model} GROUP BY ALL ORDER BY 3",
    )
    assert rows == [(None, 4, 1), (50.0, 5, 95)]
    assert not quality(result, "wind_parts", "elia/wind").ok


def test_missing_quarter_hours_appear_as_null_rows_and_are_flagged(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    now = DAY0 + timedelta(days=1)
    hole = (DAY0 + timedelta(hours=10), DAY0 + timedelta(hours=11))
    raw.all_streams(DAY0, hole[0], constant(5.0), at=now)
    raw.write("load", load_rows(hole[1], now, constant(5.0)), at=now, key="after")

    result = run(settings, store, now)

    rows = query(con, settings, "mart_energy_15min",
                 "SELECT count(*), count(load_mw) FROM {model}")  # fmt: skip
    assert rows == [(96, 92)]  # 4 null rows on the grid, not 4 missing rows
    assert quality(result, "grid_gaps", "elia/load").value == 4
    assert quality(result, "measured_holes", "elia/load").value == 4


def test_old_holes_are_reported_but_only_recent_ones_warn(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    now = DAY0 + timedelta(days=60)
    old_hole = DAY0 + timedelta(days=1)
    raw.all_streams(DAY0, now, lambda t: None if t == old_hole else 7.0, at=now)

    q = quality(run(settings, store, now), "measured_holes", "elia/load")

    assert q.ok
    assert "all history: 1" in q.details


@pytest.mark.parametrize(
    ("local_day", "quarter_hours", "hours"),
    [
        ("2026-10-25", 100, 25),  # clocks go back: 25-hour day
        ("2026-03-29", 92, 23),  # clocks go forward: 23-hour day
        ("2026-06-01", 96, 24),
    ],
)
def test_brussels_days_around_dst(
    settings: Settings,
    store: StateStore,
    raw: Raw,
    con: duckdb.DuckDBPyConnection,
    local_day: str,
    quarter_hours: int,
    hours: int,
) -> None:
    day = datetime.fromisoformat(local_day).replace(tzinfo=UTC)
    raw.all_streams(
        day - timedelta(days=1), day + timedelta(days=2), constant(1.0), at=day + timedelta(days=2)
    )

    run(settings, store, day + timedelta(days=2))

    sql15 = f"SELECT count(*) FROM {{model}} WHERE interval_start_local::DATE = DATE '{local_day}'"
    sqlh = f"SELECT count(*) FROM {{model}} WHERE hour_start_local::DATE = DATE '{local_day}'"
    assert query(con, settings, "mart_energy_15min", sql15) == [(quarter_hours,)]
    assert query(con, settings, "mart_energy_hourly", sqlh) == [(hours,)]


def test_hourly_values_need_all_four_quarters(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    now = DAY0 + timedelta(hours=3)
    missing = DAY0 + timedelta(hours=1, minutes=30)
    raw.all_streams(DAY0, now, lambda t: None if t == missing else float(t.minute), at=now)

    run(settings, store, now)

    rows = query(con, settings, "mart_energy_hourly",
                 "SELECT load_mw, load_quarters FROM {model} ORDER BY hour_start_utc")  # fmt: skip
    assert rows == [(22.5, 4), (None, 3), (22.5, 4)]  # mean of 0, 15, 30, 45


# -- quality -------------------------------------------------------------------------------------


def test_revision_lag_measures_how_old_values_were_when_they_changed(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.all_streams(DAY0, day1, constant(100.0), at=day1)
    late = day1 + timedelta(days=4)  # Elia corrects the whole day four days later
    raw.write("load", load_rows(DAY0, day1, constant(101.0)), at=late, key="later")

    result = run(settings, store, late, revision_window=timedelta(days=4))

    q = quality(result, "revision_lag", "elia/load")
    assert q.value == pytest.approx(5.0)  # the first quarter-hour was 5 days old
    assert "96 revised in the last 30 days" in q.details
    assert not q.ok  # 5 days > 80% of a 4-day window
    assert quality(result, "revision_lag", "elia/wind").value == 0


def test_first_measurement_of_a_forecast_row_is_not_a_revision(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.write("load", load_rows(DAY0, day1, lambda _t: None), at=DAY0, key="k")  # forecast only
    raw.write("load", load_rows(DAY0, day1, constant(100.0)), at=day1, key="k")

    raw.write("wind", wind_rows(DAY0, day1, constant(1.0)), at=day1)
    raw.write("solar", solar_rows(DAY0, day1, constant(1.0)), at=day1)

    assert quality(run(settings, store, day1), "revision_lag", "elia/load").value == 0


def test_freshness_warns_when_data_stops_arriving(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.all_streams(DAY0, day1, constant(1.0), at=day1)

    fresh = quality(run(settings, store, day1 + timedelta(hours=12)), "freshness", "elia/solar")
    stale = quality(run(settings, store, day1 + timedelta(days=4)), "freshness", "elia/solar")

    assert fresh.ok
    assert not stale.ok
    assert stale.value == pytest.approx(4 * 24 + 0.25)


def test_results_are_stored_per_transform(settings: Settings, store: StateStore, raw: Raw) -> None:
    raw.all_streams(DAY0, DAY0 + timedelta(days=1), constant(1.0), at=DAY0 + timedelta(days=1))

    run(settings, store, DAY0 + timedelta(days=1))
    result = run(settings, store, DAY0 + timedelta(days=2))

    latest = store.latest_quality_results()
    assert len(latest) == len(result.quality) == 16
    assert {q.checked_at for q in latest} == {DAY0 + timedelta(days=2)}


def test_outputs_missing_only_counts_buildable_models(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    assert not outputs_missing(settings)  # nothing ingested: nothing to build
    raw.write("load", load_rows(DAY0, DAY0 + timedelta(days=1), constant(1.0)), at=DAY0)
    assert outputs_missing(settings)

    run(settings, store, DAY0)

    assert not outputs_missing(settings)  # marts can't be built without wind and solar


def test_an_old_revision_stops_warning_after_30_days(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.all_streams(DAY0, day1, constant(100.0), at=day1)
    raw.write("load", load_rows(DAY0, day1, constant(101.0)), at=day1 + timedelta(days=20), key="x")

    later = day1 + timedelta(days=60)
    q = quality(run(settings, store, later, revision_window=timedelta(days=14)), "revision_lag",
                "elia/load")  # fmt: skip

    assert q.ok
    assert q.value == 0
    assert "all history: 96, max 21.0 days" in q.details


def test_failed_swap_keeps_all_old_outputs_and_records_the_failure(
    settings: Settings,
    store: StateStore,
    raw: Raw,
    con: duckdb.DuckDBPyConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day1 = DAY0 + timedelta(days=1)
    raw.all_streams(DAY0, day1, constant(1.0), at=day1)
    run(settings, store, day1)
    raw.write("load", load_rows(DAY0, day1, constant(2.0)), at=day1 + timedelta(hours=1), key="new")

    def locked(src: object, dst: object) -> None:
        raise PermissionError(f"[WinError 32] file in use: {dst}")

    monkeypatch.setattr("toeval.warehouse.transform.os.replace", locked)
    with pytest.raises(PermissionError):
        run(settings, store, day1 + timedelta(hours=1))

    status = store.transform_status()
    assert (status.last_status, status.consecutive_failures) == ("failed", 1)
    assert "WinError 32" in (status.last_error or "")
    assert status.last_success_started_at == day1
    assert query(con, settings, "stg_elia_load", "SELECT DISTINCT load_mw FROM {model}") == [(1.0,)]
    assert not list(settings.data_path.rglob("*.tmp"))
