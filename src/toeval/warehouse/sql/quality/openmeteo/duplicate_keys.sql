-- Staging must hold exactly one row per natural key. value = duplicate rows.
SELECT
    'openmeteo/hourly' AS subject,
    (count(*) - count(DISTINCT (location, time_utc)))::DOUBLE AS value,
    format('{} rows, {} locations', count(*), count(DISTINCT location)) AS details
FROM stg_openmeteo_hourly
