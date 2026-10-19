-- Hours missing from staging between each location's first and last hour (all-null hours are
-- never stored, so this counts hours ERA5 left empty inside the covered range).
-- value = missing hours summed over the locations.
WITH bounds AS (
    SELECT location, min(time_utc) AS first_t, max(time_utc) AS last_t, count(*) AS present
    FROM stg_openmeteo_hourly
    GROUP BY location
)
SELECT
    'openmeteo/hourly' AS subject,
    sum((epoch(last_t) - epoch(first_t)) / 3600 + 1 - present)::DOUBLE AS value,
    format('{} locations, {} to {}', count(*), min(first_t), max(last_t)) AS details
FROM bounds
