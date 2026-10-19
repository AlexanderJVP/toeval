-- How old an hour was when ERA5 changed any of its values, from successive raw versions of the
-- same (location, time); see elia/revision_lag.sql for the method. This is the evidence for the
-- provisional ERA5T re-check (docs/decisions/0008): Copernicus is said to replace preliminary
-- values after 2-3 months. value = age in days of the oldest hour revised in the last 30 days.
WITH changes AS (
    SELECT
        time,
        _ingested_at,
        v,
        lag(v) OVER (PARTITION BY location, time ORDER BY _ingested_at) AS previous
    FROM (
        SELECT
            location, time, _ingested_at,
            [temperature_2m, relative_humidity_2m, precipitation, cloud_cover, pressure_msl,
             wind_speed_10m, wind_speed_100m, wind_direction_100m, shortwave_radiation] AS v
        FROM raw_openmeteo_hourly
    )
),
revisions AS (
    SELECT
        (epoch(_ingested_at) - epoch(time)) / 86400 AS age_days,
        _ingested_at > getvariable('now')::TIMESTAMPTZ - INTERVAL 30 DAY AS recent
    FROM changes
    WHERE previous IS NOT NULL AND v IS DISTINCT FROM previous
)
SELECT
    'openmeteo/hourly' AS subject,
    coalesce(max(age_days) FILTER (WHERE recent), 0)::DOUBLE AS value,
    format('{} revised in the last 30 days (median age {} days); all history: {}, max {} days',
           count(age_days) FILTER (WHERE recent),
           coalesce(round(median(age_days) FILTER (WHERE recent), 1), 0),
           count(age_days), coalesce(round(max(age_days), 1), 0)) AS details
FROM revisions
