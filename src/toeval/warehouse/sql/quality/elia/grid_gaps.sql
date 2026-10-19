-- Quarter-hours missing entirely from staging between a stream's first and last interval.
-- value = number of missing quarter-hours.
WITH intervals AS (
    SELECT 'elia/load' AS subject, interval_start_utc AS t FROM stg_elia_load
    UNION ALL SELECT DISTINCT 'elia/wind', interval_start_utc FROM stg_elia_wind
    UNION ALL SELECT DISTINCT 'elia/solar', interval_start_utc FROM stg_elia_solar
),
bounds AS (
    SELECT subject, min(t) AS first_t, max(t) AS last_t, count(DISTINCT t) AS present
    FROM intervals GROUP BY subject
)
SELECT
    subject,
    ((epoch(last_t) - epoch(first_t)) / 900 + 1 - present)::DOUBLE AS value,
    format('{} quarter-hours from {} to {}', present, first_t, last_t) AS details
FROM bounds
