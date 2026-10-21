"""Air quality staging, mart_air_hourly's one-hour shift and the IRCEL-CELINE quality checks
(docs/decisions/0009), over synthetic raw files with the connector's exact schema."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from test_transform import load_rows, solar_rows, wind_rows
from test_weather import Raw, hours, rows, weather_rows
from toeval.config import Settings
from toeval.connectors.irceline import SCHEMA, STATIONS
from toeval.state import QualityResult, StateStore
from toeval.warehouse.transform import TransformResult, transform

H = timedelta(hours=1)
DAY = timedelta(days=1)
T0 = datetime(2026, 9, 30, 22, tzinfo=UTC)  # 2026-10-01 00:00 Brussels
Value = Callable[[datetime], float | None]


@pytest.fixture
def raw(settings: Settings, store: StateStore) -> Raw:
    return Raw(settings, store)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    yield c
    c.close()


def air_rows(
    start: datetime,
    end: datetime,
    value: Value | float = 20.0,
    *,
    station: str = "42R801",
    pollutant: str = "no2",
) -> pa.Table:
    """Hourly values with end-of-hour timestamps in (start, end]: like the API, the first value
    of a day starting at `start` is stamped start + 1 h. A None value is a missing hour."""
    f = value if callable(value) else (lambda _t: value)
    out = []
    for t in hours(start + H, end + H):
        if (v := f(t)) is not None:
            out.append({"station": station, "station_label": f"{station} - Somewhere",
                        "latitude": 51.2, "longitude": 4.4, "pollutant": pollutant,
                        "series_id": f"{station}-{pollutant}", "time": t, "value": v})  # fmt: skip
    return pa.Table.from_pylist(out, schema=SCHEMA)


def air(raw: Raw, batch: pa.Table, *, at: datetime) -> None:
    raw.write("irceline", "hourly", batch, at=at)


def quality(result: TransformResult, check: str) -> QualityResult:
    (q,) = [q for q in result.quality if q.check == check and q.subject == "irceline/hourly"]
    return q


# -- staging and the mart ------------------------------------------------------------------------


def test_a_value_moves_to_the_hour_it_covers(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    # IRCEL's value stamped 01:00 UTC is the mean over 00:00-01:00.
    air(raw, air_rows(T0, T0 + 2 * H, lambda t: float(t.hour)), at=T0 + 3 * H)

    transform(settings, store, now=T0 + 3 * H)

    assert rows(con, settings, "stg_irceline_hourly",
                "SELECT hour_end_utc::VARCHAR, value_ugm3 FROM {model} ORDER BY 1") == [
        ("2026-09-30 23:00:00+00", 23.0), ("2026-10-01 00:00:00+00", 0.0),
    ]  # fmt: skip
    assert rows(con, settings, "mart_air_hourly",
                "SELECT hour_start_utc::VARCHAR, hour_start_local::VARCHAR, no2_ugm3 "
                "FROM {model} ORDER BY 1") == [
        ("2026-09-30 22:00:00+00", "2026-10-01 00:00:00", 23.0),
        ("2026-09-30 23:00:00+00", "2026-10-01 01:00:00", 0.0),
    ]  # fmt: skip


def test_pollutants_become_columns_and_missing_hours_null_rows(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    end = T0 + 6 * H
    air(raw, pa.concat_tables([
        air_rows(T0, end, lambda t: None if t == T0 + 3 * H else 30.0),  # 02:00-03:00 missing
        air_rows(T0, end, 12.0, pollutant="pm10"),
        air_rows(T0, end, 50.0, station="41R012", pollutant="o3"),
    ]), at=end)  # fmt: skip

    result = transform(settings, store, now=end)

    assert rows(con, settings, "mart_air_hourly",
                "SELECT station, count(*), count(no2_ugm3), count(pm10_ugm3), count(pm25_ugm3), "
                "count(o3_ugm3) FROM {model} GROUP BY 1 ORDER BY 1") == [
        ("41R012", 6, 0, 0, 0, 6),
        ("42R801", 6, 5, 6, 0, 0),  # the missing NO2 hour is a NULL, not a gap in the grid
    ]  # fmt: skip
    assert quality(result, "duplicate_keys").value == 0


def test_a_newer_ingest_replaces_a_value_and_revision_lag_measures_its_age(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    end = T0 + 3 * DAY
    air(raw, air_rows(T0, end, 10.0), at=end)
    revised = T0 + 6 * H
    air(raw, air_rows(T0, end, lambda t: 99.0 if t == revised else 10.0), at=end + 7 * DAY)

    result = transform(settings, store, now=end + 7 * DAY)

    assert rows(
        con, settings, "stg_irceline_hourly", "SELECT count(*), sum(value_ugm3) FROM {model}"
    ) == [(72, 71 * 10.0 + 99.0)]
    assert quality(result, "revision_lag").value == pytest.approx((end + 7 * DAY - revised) / DAY)
    assert quality(result, "revision_lag").ok  # 10 days, far below the 320-day limit


# -- quality -------------------------------------------------------------------------------------


def test_freshness_follows_the_freshest_series(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    newest = T0 + 10 * DAY
    air(raw, pa.concat_tables([
        air_rows(T0, newest),
        air_rows(T0, T0 + 2 * DAY, pollutant="o3"),  # went quiet 8 days earlier
    ]), at=newest)  # fmt: skip

    fresh = transform(settings, store, now=newest + 30 * H)
    late = transform(settings, store, now=newest + 40 * H)

    assert quality(fresh, "freshness").value == pytest.approx(30)
    assert quality(fresh, "freshness").ok
    assert not quality(late, "freshness").ok  # beyond the 36-hour limit
    assert quality(fresh, "stale_series").value == 1  # the quiet one is reported separately


def test_stale_series_counts_recent_silences_beyond_two_days_only(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    newest = T0 + 60 * DAY
    air(raw, pa.concat_tables([
        air_rows(T0, newest),
        air_rows(T0, newest - 1 * DAY, pollutant="pm10"),  # a day of maintenance: normal
        air_rows(T0, newest - 3 * DAY, pollutant="pm25"),  # quiet: warns
        air_rows(T0, newest - 40 * DAY, pollutant="o3"),  # retired long ago: details only
    ]), at=newest)  # fmt: skip

    q = quality(transform(settings, store, now=newest + H), "stale_series")

    assert q.value == 1
    assert not q.ok
    assert "quiet: 42R801 pm25 since" in q.details
    assert q.details.endswith("quiet for over 30 days: 42R801 o3")


def test_coverage_warns_below_three_quarters_of_the_last_30_days(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    newest = T0 + 40 * DAY

    def every_third_hour_missing(t: datetime) -> float | None:  # 67%
        return None if t.hour % 3 == 0 else 20.0

    def every_fifth_hour_missing(t: datetime) -> float | None:  # 80%
        return None if t.hour % 5 == 0 and t.hour < 20 else 20.0

    air(raw, pa.concat_tables([
        air_rows(T0, newest),
        air_rows(T0, newest, every_third_hour_missing, pollutant="pm10"),
        air_rows(T0, newest, every_fifth_hour_missing, pollutant="pm25"),
    ]), at=newest)  # fmt: skip

    q = quality(transform(settings, store, now=newest + H), "coverage")

    assert q.value == 1
    assert q.details.startswith("below 75%: 42R801 pm10 67")


def test_implausible_values_warn_but_stay_in_the_mart(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    newest = T0 + 40 * DAY
    odd = {T0 + 5 * H: 5000.0, newest - DAY: -9.0, newest - 2 * DAY: -0.3, newest - 3 * H: 900.0}
    air(raw, air_rows(T0, newest, lambda t: odd.get(t, 20.0), pollutant="pm25"), at=newest)

    q = quality(transform(settings, store, now=newest + H), "value_range")

    assert q.value == 1  # -9 is recent; -0.3 is noise and 900 a real peak; 5000 is old
    assert q.details.endswith("all history: 2")
    assert rows(
        con, settings, "mart_air_hourly", "SELECT min(pm25_ugm3), max(pm25_ugm3) FROM {model}"
    ) == [(-9.0, 5000.0)]


def test_no_data_codes_become_null_hours_over_the_whole_history(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    # IRCEL published PM2.5 = -9399.06 during 2021: far older than value_range's 30 days.
    newest = T0 + 60 * DAY
    code = {T0 + 5 * H: -9399.06, T0 + 6 * H: -9999.0}
    air(raw, air_rows(T0, newest, lambda t: code.get(t, 20.0), pollutant="pm25"), at=newest)
    # A later version turns a real value into a code: the latest version wins, then is dropped.
    air(raw, air_rows(T0 + 6 * H, T0 + 7 * H, -9399.06, pollutant="pm25"), at=newest + H)

    result = transform(settings, store, now=newest + 2 * H)

    assert rows(
        con, settings, "stg_irceline_hourly", "SELECT count(*), min(value_ugm3) FROM {model}"
    ) == [(60 * 24 - 3, 20.0)]
    assert rows(con, settings, "mart_air_hourly",
                "SELECT count(*), count(pm25_ugm3), min(pm25_ugm3) FROM {model}") == [
        (60 * 24, 60 * 24 - 3, 20.0),  # NULL rows on the grid, not gaps
    ]  # fmt: skip
    q = quality(result, "no_data_codes")
    assert q.value == 0
    assert q.ok
    assert q.details == "staging: 0, mart: 0; dropped from raw: 3"
    assert quality(result, "value_range").details.endswith("all history: 0")


# -- energy + weather + air ----------------------------------------------------------------------


def test_combined_table_takes_air_from_the_one_reference_station(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    # energy_weather_hourly.sql hard-codes 42R801 Borgerhout: it must stay a curated station.
    assert "42R801" in {s.code for s in STATIONS}
    end = T0 + 4 * H
    for name, make in {"load": load_rows, "wind": wind_rows, "solar": solar_rows}.items():
        raw.write("elia", name, make(T0, end, lambda _t: 100.0), at=end)
    raw.weather(weather_rows(T0, end, lambda _t: {}), at=end)
    air(raw, pa.concat_tables([
        air_rows(T0, end, lambda t: None if t == T0 + 2 * H else float(t.hour)),  # 23-00 missing
        air_rows(T0, T0 + 2 * H, 30.0, pollutant="o3"),  # O3 goes quiet after 2 hours
        # Another Antwerp station, much higher: it must never leak in or shift the level.
        air_rows(T0, end, 500.0, station="42R804"),
        air_rows(T0, end, 500.0, station="42R804", pollutant="o3"),
    ]), at=end)  # fmt: skip

    transform(settings, store, now=end)

    assert rows(con, settings, "mart_energy_weather_hourly",
                "SELECT hour_start_utc::VARCHAR, load_mw, air_antwerp_no2_ugm3, "
                "air_antwerp_pm10_ugm3, air_antwerp_pm25_ugm3, air_antwerp_o3_ugm3 "
                "FROM {model} ORDER BY 1") == [
        # IRCEL's value stamped 23:00 UTC covers 22:00-23:00, the first energy hour.
        ("2026-09-30 22:00:00+00", 100.0, 23.0, None, None, 30.0),
        ("2026-09-30 23:00:00+00", 100.0, None, None, None, 30.0),  # NO2 missing: NULL
        ("2026-10-01 00:00:00+00", 100.0, 1.0, None, None, None),  # O3 quiet: NULL, not 500
        ("2026-10-01 01:00:00+00", 100.0, 2.0, None, None, None),
    ]  # fmt: skip
