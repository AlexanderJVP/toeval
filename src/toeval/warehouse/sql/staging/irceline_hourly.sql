-- IRCEL-CELINE: hourly air quality per station and pollutant (docs/decisions/0009), latest
-- ingested version of each (station, pollutant, time). hour_end_utc is the API's timestamp,
-- unchanged: it is the *end* of the hour the mean covers. mart_air_hourly shifts it to
-- hour_start_utc. Values are as published (provisional, small negatives included).
-- Input: raw_irceline_hourly.
SELECT
    station,
    station_label,
    latitude,
    longitude,
    pollutant,
    series_id,
    time AS hour_end_utc,
    value AS value_ugm3,
    _ingested_at AS ingested_at
FROM raw_irceline_hourly
QUALIFY row_number() OVER (
    PARTITION BY station, pollutant, time ORDER BY _ingested_at DESC, filename DESC
) = 1
ORDER BY station, pollutant, hour_end_utc
