-- "No data" codes (values below -1000 µg/m³) left in staging or mart_air_hourly, over the whole
-- history: staging drops them (ADR 0009), so anything above 0 means that rule broke. Unlike
-- value_range this isn't limited to the last 30 days, because IRCEL's -9399.06 sat in 2021.
-- The details count the raw values the rule drops (every version, as published).
WITH stg AS (
    SELECT count(*) AS n FROM stg_irceline_hourly WHERE value_ugm3 < -1000
),
mart AS (
    SELECT count(*) AS n FROM mart_air_hourly
    WHERE no2_ugm3 < -1000 OR pm10_ugm3 < -1000 OR pm25_ugm3 < -1000 OR o3_ugm3 < -1000
),
raw AS (
    SELECT count(*) AS n FROM raw_irceline_hourly WHERE value < -1000
)
SELECT
    'irceline/hourly' AS subject,
    (stg.n + mart.n)::DOUBLE AS value,
    format('staging: {}, mart: {}; dropped from raw: {}', stg.n, mart.n, raw.n) AS details
FROM stg, mart, raw
