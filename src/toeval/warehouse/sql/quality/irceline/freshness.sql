-- Hours between getvariable('now') and the latest value of the *freshest* series: is the source
-- still delivering? A single station going quiet is stale_series' job, so one broken instrument
-- doesn't look like a dead source. Values are published within ~3 h and fetched daily, so the
-- threshold (StreamSpec.freshness_limit, 36 h) allows for one missed run.
SELECT
    'irceline/hourly' AS subject,
    ((epoch(getvariable('now')::TIMESTAMPTZ) - epoch(max(hour_end_utc))) / 3600)::DOUBLE AS value,
    format('last value {} ({} {})', max(hour_end_utc),
           arg_max(station, hour_end_utc), arg_max(pollutant, hour_end_utc)) AS details
FROM stg_irceline_hourly
