# 0010. A shared calendar and aligned daily and weekly marts

- Status: accepted (owner, 2026-10-22), done 2026-10-22
- Date: 2026-10-22

## Context

Phase 3's correlation tracker runs weekly and compares series across sources, so every source must aggregate to the same days and weeks with the same rules. The hourly marts are aligned on UTC hours. Days and weeks belong to Brussels time: a local day has 23, 24 or 25 hours.

Calendar effects (weekends, public holidays, school holidays) drive traffic, NO2 and electricity load. Without them as columns, the tracker would rediscover "Sundays" as a correlation.

## Decisions (owner, 2026-10-22)

### Shape

- `mart_calendar_day`: one row per Brussels day, the calendar alone.
- `mart_daily` and `mart_weekly`: the calendar joined to the values of `mart_energy_weather_hourly`, one wide row per Brussels day and per ISO week. Phase 3 reads one table per grain.

Per-station and per-location detail stays in the hourly marts. Months are left until Statbel needs them.

### Calendar (`mart_calendar_day`)

From **2015-01-01** to the end of the school-holiday table. The daily and weekly marts read `mart_energy_weather_hourly`, whose rows are Elia's hours (from 2015-01-01 Brussels; `energy_weather_hourly.sql` selects from `mart_energy_hourly`), so they start in 2015 too: air quality from 2011–2014 and weather from 2014-12-31 don't reach them. A calendar from 2015 matches that, and limits the school-holiday table to school years from 2014–15. Columns:

- `day` (DATE, Brussels), `day_start_utc` and `day_end_utc`, `day_hours` (23 / 24 / 25);
- `iso_year`, `iso_week`, `week_start` (the Monday), `iso_weekday` (1 = Monday), `is_weekend`;
- `public_holiday` (name or NULL): the 10 Belgian legal holidays: New Year, Easter Monday, Labour Day, Ascension, Whit Monday, National Day (21 July), Assumption (15 August), All Saints (1 November), Armistice (11 November), Christmas. They are computed in Python (the Gregorian Easter algorithm, no new dependency) and tested against published dates;
- `school_holiday_flanders` (name or NULL): Flemish school holidays (autumn, Christmas, spring/carnival, Easter, summer) from a small hand-checked reference table in the repo (`school_holidays_flanders.csv`), sourced from the official calendars on onderwijs.vlaanderen.be, school years 2014–15 to the latest published. Every year is checked against an official source; a year that can't be verified is listed here as unverified, never guessed. A table and not the decree's rules, because there are exceptions: in 2020 the autumn break was extended to two weeks because of COVID. Antwerp is Flemish; since 2022–23 the French community's calendar differs, and it is left out.

The transform registers both as views from Python, like the raw inputs; nothing goes into raw. A quality check, `calendar/school_holidays`, warns when the school-holiday table ends less than 180 days ahead, so the yearly update isn't forgotten.

### Daily values (`mart_daily`)

Every numeric column of `mart_energy_weather_hourly` is aggregated over the Brussels day's real hours (23 / 24 / 25):

- **Means** (MW, °C, %, hPa, m/s, W/m², µg/m³): the mean of the hours present, **NULL unless at least 90% of the day's real hours are present** (≥ 22 of 24, ≥ 21 of 23, ≥ 23 of 25). Missing hours are never filled in.
- **Sums** (precipitation, mm): NULL unless every hour is present, because a sum over missing hours is too low.
- **Not aggregated:** offshore wind direction (degrees can't be averaged; the hourly mart has no u/v components) and `weather_land_points`.
- **Count columns** `*_hours` for the measured series (load, solar, wind, land temperature, the four air columns), like `*_quarters` in `mart_energy_hourly`.

#### Calibration: the 90% rule (real data, 2015-01-01 to 2026-09-30)

Days kept:

| Rule | Load | Temperature | NO2 (42R801) | PM10 | O3 |
|---|---|---|---|---|---|
| every hour | 99.9% | 100% | 43.5% | 91.9% | 45.6% |
| ≥ 90% of hours | 100% | 100% | 89.0% | 96.5% | 90.2% |

Requiring every hour is unrealistic for air quality: 1,510 days miss exactly 2 NO2 hours, steadily every year, consistent with routine instrument checks.

Averaging the hours present assumes the missing hours look like the rest of the day. That only holds if gaps don't fall at particular times, so the bias was measured on complete days by removing hours and comparing with the full mean:

| | NO2 | O3 | PM10 |
|---|---|---|---|
| gap share per local hour on ≥ 90% days (even = 4.2%) | 3.9–4.5% | 3.7–5.3% | 0.0–14.5%, a 09:00–14:00 block |
| expected bias, 2 hours missing as the real gaps fall | +0.005 µg/m³ (0.0% of the day-to-day std) | −0.010 (0.0%) | −0.096 (−0.9%) |
| worst case, always the 2 most-missed hours | −0.12 (0.9%) | −0.42 (1.9%) | −0.14 (1.3%) |

The day-to-day standard deviations are 14.0, 22.3 and 10.6 µg/m³. PM10's gaps cluster in working hours, but even the worst case is under 2% of the normal variation, so the rule is kept. Method: 1,855 complete NO2 days (1,944 O3, 3,922 PM10); the expected bias weights the effect of dropping each local hour by how often that hour is really missing.

### Weekly values (`mart_weekly`)

ISO weeks, Monday 00:00 to Monday 00:00 Brussels. Built **from the daily values**, not from 90% of 168 hours: 17 missing hours could be a whole Saturday, which would break the weekday/weekend pattern that is the main signal in traffic-related NO2.

- A weekly mean is the mean of the 7 daily values, **NULL unless all 7 days have one**. Precipitation: the sum of 7 daily sums, under the same rule.
- Count columns `*_days`.
- Weeks at the edges of the data (incomplete) are NULL.

Calibration, 612 weeks:

| Rule | NO2 | O3 | PM10 |
|---|---|---|---|
| all 7 days | 63.9% | 71.4% | 88.2% |
| at most 1 missing weekday, weekends complete | 81.0% | 83.7% | 92.5% |
| error of one missing weekday: systematic / typical per week, as % of the week-to-week std | −3.0% / 10.3% | +1.0% / 7.3% | −1.6% / 11.0% |

A missing weekday adds an error of about 10% of the week-to-week variation. That is too much, so all 7 days are required. About 390 of 612 NO2 weeks remain, enough for a weekly tracker. NULL weeks probably cluster during instrument outages: the tracker must treat them as gaps, not draw across them.

### Step 1, built (2026-10-22)

- `toeval/warehouse/calendar.py` registers `ref_public_holidays` (computed) and `ref_school_holidays_flanders` (from `reference/school_holidays_flanders.csv`); `sql/marts/calendar_day.sql` builds `mart_calendar_day`: 5,722 days, 2015-01-01 to 2030-08-31, 157 public holidays, 16 days of 23 hours and 15 of 25.
- **Every school year from 2014–15 to 2029–30 was verified; none is unverified.** Sources: the current official page (vlaanderen.be, 2023–24 to 2029–30) and Internet Archive snapshots of the official onderwijs.vlaanderen.be pages (2016: 2014–15 to 2019–20; 2021: 2020–21 to 2026–27). Where they overlap they agree. The module docstring lists the snapshots.
- The table holds the official school-free periods: autumn, Christmas, carnival (krokus), Easter, summer, and the **Ascension Thursday and Friday** ("hemelvaart"), which the official calendar lists as two free days. The other free days (1 May, 11 November, Easter and Whit Monday) are public holidays anyway.
- 2020–21's autumn break is the official exceptional two weeks (2–15 November 2020). COVID school closures that were not official holidays (such as those from March 2020) are not in the table (see Consequences).
- `tests/test_calendar.py` cross-checks the two independent sources: every Ascension break starts on the computed Easter + 39 days, every carnival week contains Ash Wednesday, every Easter break contains Easter Monday; lengths, weekdays and non-overlap are checked too. Mutation-checked (Brussels → UTC days, Ascension one day off).
- The calendar needs no raw data, so it always builds; `outputs_missing` only counts it once some raw data exists, so an empty warehouse doesn't trigger a transform.

### Step 2, built (2026-10-22)

- `sql/marts/daily.sql` and `sql/marts/weekly.sql`, with the rules above. Days are taken from `hour_start_local`, so every hour belongs to the Brussels day it starts in. Daily rows run from the first to the last day of the hourly table (its last days hold only Elia's load forecast, so their measured values are NULL).
- `tests/test_daily.py`: one synthetic fortnight with the 25-hour day, covering 22 of 24 hours (kept), 21 of 24 and 22 of 25 (NULL), a rain sum with one missing hour, and a week without its Saturday. Mutation-checked: an 80% threshold, a fixed 24-hour day, a 90% rule for sums, UTC days and a 6-day week each fail a test.
- On the real data: 4,319 days (2015-01-01 to 2026-10-28) and 618 weeks. Days with a value: load 4,311, temperature and precipitation 4,306, NO2 3,836, PM10 4,163, O3 3,887. Weeks with a value: load 615, temperature 614, NO2 393, PM10 542, O3 439 (the calibration expected about 390 NO2 weeks). Cross-check: the daily load means times the day's hours give 80.2 TWh for 2025, the same as the hourly mart.

## Consequences

- One input: `mart_energy_weather_hourly`. A new source joins the hourly table first and then appears in the daily and weekly marts with the same rules.
- The school-holiday table needs a yearly update (the check warns).
- **Known risk for Phase 3: COVID.** The lockdowns (2020–2021) aren't calendar holidays and are left out for now. Without them, the tracker will find 2020 itself as a strong correlation: NO2, load and traffic dropped together. Raise it with the owner when Phase 3 starts.

## Steps

1. `mart_calendar_day`: public holidays computed and tested, the Flemish school-holiday table verified against the official calendars, DST days, the horizon check.
2. `mart_daily` and `mart_weekly` with the rules above, tested on synthetic raw (23- and 25-hour days, the 90% boundary, a missing Saturday), with mutation checks.
3. Docs (README, CLAUDE.md, backlog) and the console's marts label.
