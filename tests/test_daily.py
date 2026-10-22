"""mart_daily and mart_weekly (docs/decisions/0010): the 90% day rule on the day's real hours,
precipitation sums that need every hour, and weeks that need all 7 days. One synthetic fortnight,
Monday 2026-10-19 to Sunday 2026-11-01 (Brussels), with the 25-hour day of 2026-10-25."""

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pytest

from test_air import air, air_rows
from test_transform import load_rows, solar_rows, wind_rows
from test_weather import Raw, rows, weather_rows
from toeval.config import Settings
from toeval.connectors.openmeteo import LOCATIONS
from toeval.state import StateStore
from toeval.warehouse.transform import transform

H = timedelta(hours=1)
BRUSSELS = ZoneInfo("Europe/Brussels")
START = datetime(2026, 10, 18, 22, tzinfo=UTC)  # Monday 2026-10-19 00:00 Brussels (CEST)
END = datetime(2026, 11, 1, 23, tzinfo=UTC)  # Monday 2026-11-02 00:00 Brussels (CET)

# NO2 hours missing, by Brussels (day, hour) of the hour's start.
NO2_MISSING = {
    (date(2026, 10, 20), 0), (date(2026, 10, 20), 1),  # 22 of 24: kept
    (date(2026, 10, 21), 0), (date(2026, 10, 21), 1), (date(2026, 10, 21), 2),  # 21 of 24: NULL
    # 22 of 25 on the day the clocks go back: NULL, although 22 of 24 would be kept.
    (date(2026, 10, 25), 5), (date(2026, 10, 25), 6), (date(2026, 10, 25), 7),
}  # fmt: skip
SATURDAY = date(2026, 10, 31)  # no air quality at all
THURSDAY_RAIN_GAP = datetime(2026, 10, 22, 10, tzinfo=UTC)  # 12:00 Brussels: one land point


def local(t: datetime) -> datetime:
    return t.astimezone(BRUSSELS)


def no2(t: datetime) -> float | None:  # t is IRCEL's end-of-hour stamp
    start = local(t - H)
    if (start.date(), start.hour) in NO2_MISSING or start.date() == SATURDAY:
        return None
    return float(start.hour)  # so a day's mean depends on which hours are missing


def pm10(t: datetime) -> float | None:
    day = local(t - H).date()
    return None if day == SATURDAY else 34.0 if day == date(2026, 10, 19) else 20.0


@pytest.fixture
def raw(settings: Settings, store: StateStore) -> Raw:
    return Raw(settings, store)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    yield c
    c.close()


@pytest.fixture
def built(settings: Settings, store: StateStore, raw: Raw) -> None:
    for name, make in {"load": load_rows, "wind": wind_rows, "solar": solar_rows}.items():
        raw.write("elia", name, make(START, END, lambda _t: 100.0), at=END)
    for loc in LOCATIONS:
        # Precipitation of hour h is ERA5's value at h + 1 (ADR 0008).
        def rain(t: datetime, name: str = loc.name) -> dict[str, float | None]:
            gap = name == "antwerp" and t == THURSDAY_RAIN_GAP + H
            return {"precipitation": None} if gap else {}

        raw.weather(weather_rows(START, END + H, rain, location=loc.name), at=END)
    air(raw, air_rows(START, END, no2), at=END)
    air(raw, air_rows(START, END, pm10, pollutant="pm10"), at=END)

    result = transform(settings, store, now=END)
    assert {"mart_daily", "mart_weekly"} <= set(result.built)


def day(con: duckdb.DuckDBPyConnection, settings: Settings, d: str, cols: str) -> tuple:
    (row,) = rows(con, settings, "mart_daily", f"SELECT {cols} FROM {{model}} WHERE day = '{d}'")
    return row


# -- days ----------------------------------------------------------------------------------------


@pytest.mark.usefixtures("built")
def test_days_span_the_data_in_brussels_time(
    settings: Settings, con: duckdb.DuckDBPyConnection
) -> None:
    assert rows(con, settings, "mart_daily",
                "SELECT count(*), min(day)::VARCHAR, max(day)::VARCHAR, sum(day_hours) "
                "FROM {model}") == [(14, "2026-10-19", "2026-11-01", 14 * 24 + 1)]  # fmt: skip
    # The complete day: every hour of the first Brussels day counts (it starts at 22:00 UTC).
    assert day(con, settings, "2026-10-19",
               "day_hours, load_mw, load_hours, air_antwerp_no2_ugm3, air_antwerp_no2_hours, "
               "air_antwerp_pm10_ugm3, precipitation_mm, weather_hours") == (
        24, 100.0, 24, 11.5, 24, 34.0, 24.0, 24,
    )  # fmt: skip


@pytest.mark.usefixtures("built")
def test_a_day_needs_90_percent_of_its_real_hours(
    settings: Settings, con: duckdb.DuckDBPyConnection
) -> None:
    cols = "air_antwerp_no2_ugm3, air_antwerp_no2_hours"
    assert day(con, settings, "2026-10-20", cols) == (12.5, 22)  # mean of the hours present
    assert day(con, settings, "2026-10-21", cols) == (None, 21)  # below 90%: NULL
    assert day(con, settings, "2026-10-31", cols) == (None, 0)
    # 25 hours: 22 present is below 90% (22.5), and every series counts 25 hours.
    assert day(con, settings, "2026-10-25",
               f"day_hours, {cols}, load_mw, load_hours, weather_hours") == (
        25, None, 22, 100.0, 25, 25,
    )  # fmt: skip


@pytest.mark.usefixtures("built")
def test_a_daily_sum_needs_every_hour(settings: Settings, con: duckdb.DuckDBPyConnection) -> None:
    assert day(con, settings, "2026-10-21", "precipitation_mm") == (24.0,)
    assert day(con, settings, "2026-10-22", "precipitation_mm, temperature_2m_c") == (None, 1.0)
    assert day(con, settings, "2026-10-25", "precipitation_mm") == (25.0,)  # 25 hours of 1 mm


# -- weeks ---------------------------------------------------------------------------------------


@pytest.mark.usefixtures("built")
def test_a_week_needs_all_seven_days(settings: Settings, con: duckdb.DuckDBPyConnection) -> None:
    out = rows(con, settings, "mart_weekly",
               "SELECT week_start::VARCHAR, iso_week, days, week_hours, public_holidays, "
               "load_mw, load_days, air_antwerp_pm10_ugm3, air_antwerp_pm10_days, "
               "air_antwerp_no2_ugm3, air_antwerp_no2_days, precipitation_mm "
               "FROM {model} ORDER BY 1")  # fmt: skip
    assert out == [
        # PM10: (34 + 6 x 20) / 7. NO2 lacks Wednesday and Sunday; rain lacks Thursday.
        ("2026-10-19", 43, 7, 169, 0, 100.0, 7, 22.0, 7, None, 5, None),
        # A whole Saturday without air quality: no weekly air values. Sunday is All Saints.
        ("2026-10-26", 44, 7, 168, 1, 100.0, 7, None, 6, None, 6, 7 * 24.0),
    ]
