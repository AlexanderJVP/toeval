-- Hours between getvariable('now') and the latest hour with a value, for the stalest location.
-- ERA5 runs ~6 days behind, so the threshold (StreamSpec.freshness_limit) allows for that.
WITH latest AS (
    SELECT location, max(time_utc) AS t FROM stg_openmeteo_hourly GROUP BY location
)
SELECT
    'openmeteo/hourly' AS subject,
    ((epoch(getvariable('now')::TIMESTAMPTZ) - epoch(min(t))) / 3600)::DOUBLE AS value,
    format('last value {} (stalest location: {})', min(t), arg_min(location, t)) AS details
FROM latest
