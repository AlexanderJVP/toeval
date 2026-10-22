-- One row per Brussels day (docs/decisions/0010), from 2015-01-01 (Elia's first Brussels day, where
-- the daily marts start) to the last day of the school-holiday table. The day's UTC bounds come
-- from Brussels midnight, so day_hours is 23 on the spring DST day and 25 in October: the hourly
-- marts' hours belong to the day with day_start_utc <= hour_start_utc < day_end_utc.
-- ISO weeks start on Monday; iso_year can differ from the calendar year around New Year.
-- Inputs: ref_public_holidays, ref_school_holidays_flanders (registered from
-- toeval/warehouse/calendar.py).
WITH days AS (
    SELECT unnest(generate_series(
        DATE '2015-01-01', (SELECT max(last_day) FROM ref_school_holidays_flanders), INTERVAL 1 DAY
    ))::DATE AS day
),
bounds AS (
    SELECT
        day,
        timezone('Europe/Brussels', day::TIMESTAMP) AS day_start_utc,
        timezone('Europe/Brussels', (day + 1)::TIMESTAMP) AS day_end_utc
    FROM days
)
SELECT
    b.day,
    b.day_start_utc,
    b.day_end_utc,
    date_diff('hour', b.day_start_utc, b.day_end_utc) AS day_hours,
    isoyear(b.day) AS iso_year,
    weekofyear(b.day) AS iso_week,
    date_trunc('week', b.day)::DATE AS week_start,
    isodow(b.day) AS iso_weekday,
    isodow(b.day) >= 6 AS is_weekend,
    p.name AS public_holiday,
    s.holiday AS school_holiday_flanders
FROM bounds AS b
LEFT JOIN ref_public_holidays AS p ON p.day = b.day
LEFT JOIN ref_school_holidays_flanders AS s ON b.day BETWEEN s.first_day AND s.last_day
ORDER BY b.day
