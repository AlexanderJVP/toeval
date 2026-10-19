"""Weather staging, mart_weather_hourly's time alignment (docs/decisions/0008) and the
per-stream quality limits, over synthetic raw files with the connector's exact schema."""

import math
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from test_transform import DAY0, load_rows, solar_rows, wind_rows
from toeval.config import Settings
from toeval.connectors.openmeteo import LOCATIONS, SCHEMA, VARIABLES
from toeval.pipeline import Pipeline
from toeval.state import StateStore
from toeval.warehouse.raw import RawWriter
from toeval.warehouse.transform import TransformResult, transform

H = timedelta(hours=1)
Weather = Callable[[datetime], dict[str, float | None]]


def hours(start: datetime, end: datetime) -> list[datetime]:
    return [start + i * H for i in range(int((end - start) / H))]


def weather_rows(
    start: datetime, end: datetime, values: Weather, *, location: str = "offshore"
) -> pa.Table:
    rows = []
    for t in hours(start, end):
        row = {name: 1.0 for name in VARIABLES} | values(t)
        rows.append(
            {"location": location, "grid_latitude": 51.5, "grid_longitude": 3.0, "time": t} | row
        )
    return pa.Table.from_pylist(rows, schema=SCHEMA)


class Raw:
    def __init__(self, settings: Settings, store: StateStore) -> None:
        self.writer = RawWriter(settings.raw_path, settings.quarantine_path, store)

    def write(self, source: str, stream: str, batch: pa.Table, *, at: datetime) -> None:
        self.writer.write(source, stream, batch, run_id=uuid.uuid4().hex,
                          request_key=f"{at}-{uuid.uuid4().hex}", ingested_at=at)  # fmt: skip

    def weather(self, batch: pa.Table, *, at: datetime) -> None:
        self.write("openmeteo", "hourly", batch, at=at)


@pytest.fixture
def raw(settings: Settings, store: StateStore) -> Raw:
    return Raw(settings, store)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    yield c
    c.close()


def rows(con: duckdb.DuckDBPyConnection, settings: Settings, model: str, sql: str) -> list[tuple]:
    layer = settings.staging_path if model.startswith("stg_") else settings.marts_path
    path = (layer / f"{model}.parquet").as_posix()
    return con.execute(sql.replace("{model}", f"read_parquet('{path}')")).fetchall()


def quality(result: TransformResult, check: str, subject: str = "openmeteo/hourly"):  # type: ignore[no-untyped-def]
    (q,) = [q for q in result.quality if q.check == check and q.subject == subject]
    return q


def by_hour(values: dict[datetime, dict[str, float | None]]) -> Weather:
    return lambda t: values.get(t, {})


# -- alignment -----------------------------------------------------------------------------------


def test_preceding_hour_values_move_to_the_hour_they_cover(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    # Rain and sun reported at 01:00 fell between 00:00 and 01:00.
    t1 = DAY0 + H
    raw.weather(weather_rows(DAY0, DAY0 + 3 * H, by_hour({
        t1: {"precipitation": 2.5, "shortwave_radiation": 300.0},
    })), at=DAY0 + 3 * H)  # fmt: skip

    transform(settings, store, now=DAY0 + 3 * H)

    assert rows(con, settings, "mart_weather_hourly",
                "SELECT hour_start_utc = TIMESTAMPTZ '2026-09-30 22:00:00+00', precipitation_mm, "
                "shortwave_radiation_wm2 FROM {model} ORDER BY hour_start_utc") == [
        (True, 2.5, 300.0),
        (False, 1.0, 1.0),
    ]  # fmt: skip


def test_snapshots_are_averaged_over_the_hour_and_need_both_ends(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    end = DAY0 + 4 * H
    temperature = {t: {"temperature_2m": float(i)} for i, t in enumerate(hours(DAY0, end))}
    temperature[DAY0 + 2 * H] = {"temperature_2m": None}  # ERA5 left one value empty
    raw.weather(weather_rows(DAY0, end, by_hour(temperature)), at=end)

    transform(settings, store, now=end)

    # Hours 0-1, 1-2, 2-3: snapshots (0, 1), (1, None), (None, 3). Three hours from four snapshots.
    assert rows(con, settings, "mart_weather_hourly",
                "SELECT temperature_2m_c FROM {model} ORDER BY hour_start_utc") == [
        (0.5,), (None,), (None,)
    ]  # fmt: skip


def test_missing_hours_are_null_rows_on_the_grid(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    end = DAY0 + 6 * H
    raw.weather(weather_rows(DAY0, DAY0 + 2 * H, by_hour({})), at=end)
    raw.weather(weather_rows(DAY0 + 3 * H, end, by_hour({})), at=end)

    result = transform(settings, store, now=end)

    counts = "SELECT count(*), count(temperature_2m_c), count(precipitation_mm) FROM {model}"
    # 02:00 is missing: the hours either side lose their means, the hour before it its total.
    assert rows(con, settings, "mart_weather_hourly", counts) == [(5, 3, 4)]
    assert quality(result, "grid_gaps").value == 1


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ((10.0, 350.0), (10.0, 10.0), 0.0),  # not 180: degrees don't average
        ((10.0, 80.0), (10.0, 100.0), 90.0),
        ((1.0, 0.0), (9.0, 90.0), math.degrees(math.atan2(9, 1))),  # weighted by speed
        ((0.0, 123.0), (0.0, 321.0), None),  # calm: no direction
    ],
)
def test_wind_direction_is_the_direction_of_the_mean_wind_vector(
    settings: Settings,
    store: StateStore,
    raw: Raw,
    con: duckdb.DuckDBPyConnection,
    first: tuple[float, float],
    second: tuple[float, float],
    expected: float | None,
) -> None:
    snapshots = {
        DAY0: {"wind_speed_100m": first[0], "wind_direction_100m": first[1]},
        DAY0 + H: {"wind_speed_100m": second[0], "wind_direction_100m": second[1]},
    }
    raw.weather(weather_rows(DAY0, DAY0 + 2 * H, by_hour(snapshots)), at=DAY0 + 2 * H)

    transform(settings, store, now=DAY0 + 2 * H)

    sql = "SELECT wind_direction_100m_deg, wind_speed_100m_ms FROM {model}"
    ((direction, speed),) = rows(con, settings, "mart_weather_hourly", sql)
    assert speed == (first[0] + second[0]) / 2
    if expected is None:
        assert direction is None
    else:
        assert (direction - expected + 180) % 360 - 180 == pytest.approx(0, abs=1e-9)


def test_brussels_hours_repeat_when_the_clocks_go_back(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    start = datetime(2026, 10, 24, 22, tzinfo=UTC)  # 2026-10-25 00:00 Brussels
    raw.weather(weather_rows(start, start + 26 * H, by_hour({})), at=start + 26 * H)

    transform(settings, store, now=start + 26 * H)

    assert rows(con, settings, "mart_weather_hourly",
                "SELECT count(*), count(DISTINCT hour_start_local) FROM {model} "
                "WHERE hour_start_local::DATE = DATE '2026-10-25'") == [(25, 24)]  # fmt: skip


def test_offshore_wind_speed_lines_up_with_offshore_wind_output(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    """The check ADR 0008 names: with the alignment right, Elia's hourly wind output correlates
    better with the same hour's offshore wind than with the hour before or after."""

    def wind(t: datetime) -> float:  # m/s; the fast terms make a one-hour shift visible
        x = (t - DAY0) / H
        return (10 + 4 * math.sin(2 * math.pi * x / 13) + 3 * math.sin(2 * math.pi * x / 5.3)
                + 2 * math.sin(2 * math.pi * x / 2.7 + 1))  # fmt: skip

    def quarter_mean(t: datetime) -> float:  # Elia: average power over the quarter-hour
        return 100 * sum(wind(t + timedelta(minutes=m + 0.5)) for m in range(15)) / 15

    now = DAY0 + timedelta(days=4)
    raw.write("elia", "wind", wind_rows(DAY0, now, quarter_mean), at=now)
    raw.write("elia", "load", load_rows(DAY0, now, quarter_mean), at=now)
    raw.write("elia", "solar", solar_rows(DAY0, now, quarter_mean), at=now)
    raw.weather(weather_rows(DAY0, now, lambda t: {"wind_speed_100m": wind(t)}), at=now)

    transform(settings, store, now=now)

    energy = (settings.marts_path / "mart_energy_hourly.parquet").as_posix()
    weather = (settings.marts_path / "mart_weather_hourly.parquet").as_posix()
    corr = {
        lag: con.execute(
            f"SELECT corr(e.wind_mw, w.wind_speed_100m_ms) FROM read_parquet('{energy}') e "
            f"JOIN read_parquet('{weather}') w "
            f"ON w.hour_start_utc = e.hour_start_utc + INTERVAL ({lag}) HOUR"
        ).fetchone()[0]  # type: ignore[index]
        for lag in (-1, 0, 1)
    }
    assert corr[0] > 0.98
    assert corr[0] > corr[-1] + 0.05
    assert corr[0] > corr[1] + 0.05


# -- staging and quality -------------------------------------------------------------------------


def test_latest_version_of_each_hour_wins(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    end = DAY0 + 3 * H
    raw.weather(weather_rows(DAY0, end, lambda _t: {"temperature_2m": 10.0}), at=end)
    raw.weather(weather_rows(DAY0, end, lambda _t: {"temperature_2m": 11.0}), at=end + H)

    result = transform(settings, store, now=end + H)

    assert rows(con, settings, "stg_openmeteo_hourly",
                "SELECT DISTINCT temperature_2m_c, count(*) FROM {model} GROUP BY 1") == [
        (11.0, 3)
    ]  # fmt: skip
    assert quality(result, "duplicate_keys").value == 0


def test_freshness_allows_for_eras_delay_but_not_more(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    end = DAY0 + timedelta(days=1)
    raw.weather(weather_rows(DAY0, end, by_hour({})), at=end)
    raw.weather(weather_rows(DAY0, end, by_hour({}), location="libramont"), at=end - H)

    seven_days = quality(transform(settings, store, now=end + timedelta(days=7)), "freshness")
    nine_days = quality(transform(settings, store, now=end + timedelta(days=9)), "freshness")

    assert seven_days.ok  # Elia's 72 hours would warn here
    assert not nine_days.ok
    assert nine_days.value == pytest.approx(9 * 24 + 1)


def test_revision_lag_warns_only_beyond_the_deep_recheck(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    end = DAY0 + timedelta(days=1)
    raw.weather(weather_rows(DAY0, end, lambda _t: {"temperature_2m": 10.0}), at=end)
    # Final ERA5 replaces preliminary values ~70 days later, seen by the 120-day re-check.
    seventy = end + timedelta(days=70)
    raw.weather(weather_rows(DAY0, end, lambda _t: {"temperature_2m": 10.2}), at=seventy)

    q = quality(transform(settings, store, now=seventy), "revision_lag")
    assert q.ok
    assert q.value == pytest.approx(71)

    later = end + timedelta(days=100)
    raw.weather(weather_rows(DAY0, end, lambda _t: {"temperature_2m": 10.3}), at=later)
    assert not quality(transform(settings, store, now=later), "revision_lag").ok


def test_sources_toml_limits_reach_the_checks(
    settings: Settings, store: StateStore, raw: Raw
) -> None:
    end = DAY0 + timedelta(days=1)
    raw.weather(weather_rows(DAY0, end, by_hour({})), at=end)
    settings.sources_path.write_text(
        '[openmeteo.streams.hourly]\nfreshness_limit = "1d"\n', "utf-8"
    )

    result = Pipeline(settings, store, clock=lambda: end + timedelta(days=2)).transform()

    assert not quality(result, "freshness").ok


# -- energy + weather ----------------------------------------------------------------------------


def test_combined_table_has_every_energy_hour_with_land_means_and_offshore_wind(
    settings: Settings, store: StateStore, raw: Raw, con: duckdb.DuckDBPyConnection
) -> None:
    land = [loc.name for loc in LOCATIONS if loc.name != "offshore"]
    assert len(land) == 6  # energy_weather_hourly.sql requires all 6 for a land mean
    now = DAY0 + timedelta(days=1)
    for name in ("load", "wind", "solar"):
        make = {"load": load_rows, "wind": wind_rows, "solar": solar_rows}[name]
        raw.write("elia", name, make(DAY0, now, lambda _t: 100.0), at=now)
    weather_end = DAY0 + 4 * H  # ERA5 is behind: weather for the first 3 hours only
    for i, name in enumerate(land):
        raw.weather(weather_rows(DAY0, weather_end, lambda _t, i=i: {
            "temperature_2m": float(10 + i), "wind_speed_100m": 5.0, "wind_direction_100m": 270.0,
        }, location=name), at=now)  # fmt: skip
    raw.weather(weather_rows(DAY0, weather_end, lambda _t: {
        "wind_speed_100m": 12.0, "wind_direction_100m": 225.0,
    }), at=now)  # fmt: skip
    # One land point lacks one snapshot: the land mean of the two hours around it is NULL.
    raw.weather(weather_rows(DAY0 + 2 * H, DAY0 + 3 * H, lambda _t: {
        "temperature_2m": None, "wind_speed_100m": 5.0, "wind_direction_100m": 270.0,
    }, location="libramont"), at=now + H)  # fmt: skip

    transform(settings, store, now=now)

    out = rows(con, settings, "mart_energy_weather_hourly",
               "SELECT load_mw, temperature_2m_c, wind_speed_100m_ms, "
               "offshore_wind_speed_100m_ms, round(offshore_wind_direction_100m_deg, 6), "
               "weather_land_points FROM {model} ORDER BY hour_start_utc")  # fmt: skip
    assert len(out) == 24  # every energy hour
    assert out[0] == (100.0, 12.5, 5.0, 12.0, 225.0, 6)  # mean of 10..15
    # Libramont's 02:00 temperature is missing: no land temperature for the hours either side,
    # but the other variables keep their land means.
    assert (out[1][1], out[1][2], out[1][5]) == (None, 5.0, 5)
    assert (out[2][1], out[2][2], out[2][5]) == (None, 5.0, 5)
    assert out[3] == (100.0, None, None, None, None, 0)  # energy only: no weather yet
