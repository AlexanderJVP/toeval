-- Staging must hold exactly one row per natural key. value = duplicate rows.
SELECT
    'irceline/hourly' AS subject,
    (count(*) - count(DISTINCT (station, pollutant, hour_end_utc)))::DOUBLE AS value,
    format('{} rows, {} series', count(*), count(DISTINCT (station, pollutant))) AS details
FROM stg_irceline_hourly
