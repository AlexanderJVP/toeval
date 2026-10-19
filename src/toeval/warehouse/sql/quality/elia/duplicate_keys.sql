-- Staging must hold exactly one row per natural key. value = duplicate rows.
SELECT
    'elia/load' AS subject,
    (count(*) - count(DISTINCT interval_start_utc))::DOUBLE AS value,
    format('{} rows', count(*)) AS details
FROM stg_elia_load
UNION ALL
SELECT 'elia/wind', (count(*) - count(DISTINCT (
    interval_start_utc, offshore_onshore, region, grid_connection_type)))::DOUBLE,
    format('{} rows', count(*))
FROM stg_elia_wind
UNION ALL
SELECT 'elia/solar', (count(*) - count(DISTINCT (interval_start_utc, region)))::DOUBLE,
    format('{} rows', count(*))
FROM stg_elia_solar
