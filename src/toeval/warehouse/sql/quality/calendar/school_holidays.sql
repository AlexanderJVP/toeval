-- Is the Flemish school-holiday table (toeval/warehouse/reference/school_holidays_flanders.csv)
-- still ahead of time? value = days of the next 180 it doesn't cover yet: 0 while it reaches at
-- least 180 days past getvariable('now'). A new school year is published well ahead (ADR 0010);
-- add it to the CSV when this warns.
WITH last AS (
    SELECT max(last_day) AS last_day FROM ref_school_holidays_flanders
),
ahead AS (
    SELECT last_day, date_diff('day', getvariable('now')::TIMESTAMPTZ::DATE, last_day) AS days
    FROM last
)
SELECT
    'calendar/school_holidays' AS subject,
    greatest(0, 180 - days)::DOUBLE AS value,
    format('table ends {} ({} days ahead)', last_day, days) AS details
FROM ahead
