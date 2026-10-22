"""The calendar's reference data and mart_calendar_day (docs/decisions/0010).

The school-holiday table and the Easter algorithm are independent sources, so each checks the
other: the official Ascension days off, the carnival week and the Easter break must fall where
the computed Easter puts them."""

from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise

import duckdb
import pytest

from test_weather import rows
from toeval.config import Settings
from toeval.state import StateStore
from toeval.warehouse.calendar import (
    CALENDAR_START,
    easter,
    public_holidays,
    school_holidays,
)
from toeval.warehouse.transform import transform

NOW = datetime(2026, 10, 22, 10, tzinfo=UTC)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    yield c
    c.close()


# -- reference data ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("year", "sunday"),
    [(2008, date(2008, 3, 23)), (2015, date(2015, 4, 5)), (2016, date(2016, 3, 27)),
     (2019, date(2019, 4, 21)), (2024, date(2024, 3, 31)), (2025, date(2025, 4, 20)),
     (2038, date(2038, 4, 25))],
)  # fmt: skip
def test_easter_sunday(year: int, sunday: date) -> None:
    assert easter(year) == sunday


def test_public_holidays_of_2026_match_the_published_list() -> None:
    assert public_holidays(2026) == [
        (date(2026, 1, 1), "Nieuwjaar"),
        (date(2026, 4, 6), "Paasmaandag"),
        (date(2026, 5, 1), "Dag van de Arbeid"),
        (date(2026, 5, 14), "Onze-Lieve-Heer-Hemelvaart"),
        (date(2026, 5, 25), "Pinkstermaandag"),
        (date(2026, 7, 21), "Nationale feestdag"),
        (date(2026, 8, 15), "Onze-Lieve-Vrouw-Hemelvaart"),
        (date(2026, 11, 1), "Allerheiligen"),
        (date(2026, 11, 11), "Wapenstilstand"),
        (date(2026, 12, 25), "Kerstmis"),
    ]


def periods() -> list[tuple[str, str, date, date]]:
    return [
        (r["school_year"], r["holiday"], date.fromisoformat(r["first_day"]),
         date.fromisoformat(r["last_day"]))
        for r in school_holidays()
    ]  # fmt: skip


def test_school_holiday_table_has_every_holiday_of_every_school_year_once() -> None:
    table = periods()
    years = sorted({y for y, *_ in table})
    assert years[0] == "2014-2015"  # covers CALENDAR_START
    assert date(2014, 12, 22) <= CALENDAR_START
    assert years == [f"{y}-{y + 1}" for y in range(2014, 2014 + len(years))]  # no gaps
    holidays = {"herfstvakantie", "kerstvakantie", "krokusvakantie", "paasvakantie",
                "hemelvaart", "zomervakantie"}  # fmt: skip
    assert Counter((y, h) for y, h, *_ in table) == {(y, h): 1 for y in years for h in holidays}


def test_school_holidays_never_overlap() -> None:
    ordered = sorted(periods(), key=lambda p: p[2])
    for (_, a, _, a_end), (_, b, b_start, _) in pairwise(ordered):
        assert a_end < b_start, (a, b)


def test_school_holidays_agree_with_the_computed_easter() -> None:
    for year, holiday, first, last in periods():
        span = (last - first).days + 1
        e = easter(last.year)
        if holiday == "hemelvaart":  # Ascension Thursday and the Friday after
            assert (first, span) == (e + timedelta(days=39), 2), year
        elif holiday == "krokusvakantie":  # carnival: the week with Ash Wednesday
            assert first <= e - timedelta(days=46) <= last, year
            assert (first.isoweekday(), span) == (1, 7), year
        elif holiday == "paasvakantie":  # two weeks with Easter Monday, or ending on it
            assert first <= e + timedelta(days=1) <= last, year
            assert (first.isoweekday(), span) in {(1, 14), (1, 15)}, year
        elif holiday == "herfstvakantie":  # the week of 1 November (2020: two, for COVID)
            assert date(last.year, 10, 26) <= first <= date(last.year, 11, 2), year
            assert (first.isoweekday(), span) == (1, 14 if year == "2020-2021" else 7), year
        elif holiday == "kerstvakantie":
            assert first <= date(last.year, 1, 1) <= last, year
            assert (first.isoweekday(), span) == (1, 14), year
        else:
            assert (first, last) == (date(last.year, 7, 1), date(last.year, 8, 31)), year


# -- mart_calendar_day ---------------------------------------------------------------------------


def test_calendar_days_follow_brussels_time_and_carry_the_holidays(
    settings: Settings, store: StateStore, con: duckdb.DuckDBPyConnection
) -> None:
    result = transform(settings, store, now=NOW)  # no raw at all: the calendar still builds

    assert "mart_calendar_day" in result.built
    ((n, first, last),) = rows(
        con, settings, "mart_calendar_day",
        "SELECT count(*), min(day)::VARCHAR, max(day)::VARCHAR FROM {model}",
    )  # fmt: skip
    assert (first, last) == ("2015-01-01", "2030-08-31")
    assert n == (date(2030, 8, 31) - date(2015, 1, 1)).days + 1

    def day(d: str) -> tuple:
        (row,) = rows(con, settings, "mart_calendar_day",
                      "SELECT day_start_utc::VARCHAR, day_hours, iso_year, iso_week, "
                      "week_start::VARCHAR, iso_weekday, is_weekend, public_holiday, "
                      f"school_holiday_flanders FROM {{model}} WHERE day = DATE '{d}'")  # fmt: skip
        return row

    assert day("2015-01-01") == ("2014-12-31 23:00:00+00", 24, 2015, 1, "2014-12-29", 4, False,
                                 "Nieuwjaar", "kerstvakantie")  # fmt: skip
    assert day("2026-03-29")[:2] == ("2026-03-28 23:00:00+00", 23)  # clocks go forward
    assert day("2026-10-25")[:2] == ("2026-10-24 22:00:00+00", 25)  # clocks go back
    # 2027-01-01 is a Friday in ISO week 53 of 2026.
    assert day("2027-01-01")[2:8] == (2026, 53, "2026-12-28", 5, False, "Nieuwjaar")
    assert day("2020-11-12")[7:] == (None, "herfstvakantie")  # the COVID extension
    assert day("2026-10-24")[5:] == (6, True, None, None)  # an ordinary Saturday
    assert rows(con, settings, "mart_calendar_day",
                "SELECT count(*) FROM {model} WHERE public_holiday IS NOT NULL "
                "AND year(day) = 2026") == [(10,)]  # fmt: skip


def test_school_holiday_check_warns_half_a_year_before_the_table_ends(
    settings: Settings, store: StateStore
) -> None:
    def check(now: datetime):  # type: ignore[no-untyped-def]
        result = transform(settings, store, now=now)
        (q,) = [q for q in result.quality if q.subject == "calendar/school_holidays"]
        return q

    assert check(NOW).ok
    assert check(NOW).details.startswith("table ends 2030-08-31")
    late = check(datetime(2030, 4, 1, tzinfo=UTC))  # 152 days before the end
    assert not late.ok
    assert late.value == 180 - 152
