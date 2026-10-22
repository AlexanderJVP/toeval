"""Reference data for mart_calendar_day (docs/decisions/0010): Belgian public holidays, computed,
and Flemish school holidays, from a hand-checked table.

Both are registered as views in the transform's DuckDB (`ref_public_holidays`,
`ref_school_holidays_flanders`), like the raw inputs; nothing goes into raw. The SQL in
`sql/marts/calendar_day.sql` turns them into one row per Brussels day.

`reference/school_holidays_flanders.csv` lists the official Flemish school holidays, school
years 2014-2015 to 2029-2030, each checked against the official calendar. `source`:

- A: onderwijs.vlaanderen.be/nl/schoolvakanties-vorige-schooljaren, web.archive.org 2016-04-20
- B: onderwijs.vlaanderen.be/nl/schoolvakanties-volgende-schooljaren, web.archive.org 2016-04-17
- C: onderwijs.vlaanderen.be/nl/schoolvakanties, web.archive.org 2021-01-23 (the autumn break of
  2020 was exceptionally extended to two weeks because of COVID)
- D: onderwijs.vlaanderen.be/nl/schoolvakanties-volgende-schooljaren, web.archive.org 2021-01-20
- E: vlaanderen.be/onderwijs-en-vorming/.../schoolvakanties, live on 2026-10-22

Where sources overlap (2023-2027 in D and E, 2015-2016 in A and B) they agree. Add a school year
when the government publishes it; the `calendar/school_holidays` check warns when the table ends
less than 180 days ahead.
"""

import csv
from datetime import date, timedelta
from importlib.resources import files

import duckdb
import pyarrow as pa

SCHOOL_HOLIDAYS_CSV = files("toeval.warehouse") / "reference" / "school_holidays_flanders.csv"

# The daily marts start with Elia's first Brussels day (ADR 0010).
CALENDAR_START = date(2015, 1, 1)

REFERENCE_VIEWS = ("ref_public_holidays", "ref_school_holidays_flanders")


def easter(year: int) -> date:
    """Easter Sunday in the Gregorian calendar (the anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month, day = divmod(h + m - 7 * n + 114, 31)
    return date(year, month, day + 1)


def public_holidays(year: int) -> list[tuple[date, str]]:
    """The 10 Belgian legal public holidays (Dutch names). Easter and Whit Sunday are Sundays
    anyway and are left out, as in the law."""
    e = easter(year)
    return sorted([
        (date(year, 1, 1), "Nieuwjaar"),
        (e + timedelta(days=1), "Paasmaandag"),
        (date(year, 5, 1), "Dag van de Arbeid"),
        (e + timedelta(days=39), "Onze-Lieve-Heer-Hemelvaart"),
        (e + timedelta(days=50), "Pinkstermaandag"),
        (date(year, 7, 21), "Nationale feestdag"),
        (date(year, 8, 15), "Onze-Lieve-Vrouw-Hemelvaart"),
        (date(year, 11, 1), "Allerheiligen"),
        (date(year, 11, 11), "Wapenstilstand"),
        (date(year, 12, 25), "Kerstmis"),
    ])  # fmt: skip


def school_holidays() -> list[dict[str, str]]:
    with SCHOOL_HOLIDAYS_CSV.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def register(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Register the reference views and return their names. Public holidays cover every year of
    the school-holiday table, so the calendar never outruns either of them."""
    rows = school_holidays()
    school = pa.table({
        "school_year": [r["school_year"] for r in rows],
        "holiday": [r["holiday"] for r in rows],
        "first_day": [date.fromisoformat(r["first_day"]) for r in rows],
        "last_day": [date.fromisoformat(r["last_day"]) for r in rows],
    })  # fmt: skip
    last_year = max(date.fromisoformat(r["last_day"]) for r in rows).year
    names: dict[date, list[str]] = {}
    for year in range(CALENDAR_START.year, last_year + 1):
        for day, name in public_holidays(year):
            names.setdefault(day, []).append(name)
    public = pa.table({
        "day": list(names),
        # Two holidays can share a date (Ascension on 1 May when Easter is on 23 March).
        "name": [" / ".join(n) for n in names.values()],
    })  # fmt: skip
    con.register("ref_school_holidays_flanders", school)
    con.register("ref_public_holidays", public)
    return set(REFERENCE_VIEWS)
