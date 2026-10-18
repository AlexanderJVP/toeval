-- National load, solar and wind per quarter-hour (MW, average power over the interval).
-- One row per quarter-hour on a gap-free grid, so missing data shows up as NULL, not as a
-- missing row. Keyed by interval_start_utc; interval_start_local is Brussels wall-clock time
-- and repeats during the October DST change, so never use it as a key.
-- Inputs: stg_elia_load, stg_elia_solar, stg_elia_wind. See docs/decisions/0003:
--   solar = the 'Belgium' row only (the other rows are regions/provinces of it);
--   wind  = the sum of the 5 disjoint parts, NULL unless all 5 are present.
WITH load AS (
    SELECT interval_start_utc, load_mw, load_forecast_day_ahead_mw
    FROM stg_elia_load
),
solar AS (
    SELECT interval_start_utc, solar_mw, solar_forecast_day_ahead_mw
    FROM stg_elia_solar
    WHERE region = 'Belgium'
),
wind AS (
    SELECT
        interval_start_utc,
        count(wind_mw) AS wind_parts_present,
        sum(wind_mw) AS wind_sum_mw,
        count(wind_forecast_day_ahead_mw) AS wind_forecast_parts,
        sum(wind_forecast_day_ahead_mw) AS wind_forecast_sum_mw
    FROM stg_elia_wind
    GROUP BY interval_start_utc
),
bounds AS (
    SELECT min(t) AS first_interval, max(t) AS last_interval
    FROM (
        SELECT interval_start_utc AS t FROM load
        UNION ALL SELECT interval_start_utc FROM solar
        UNION ALL SELECT interval_start_utc FROM wind
    )
),
spine AS (
    SELECT unnest(generate_series(first_interval, last_interval, INTERVAL 15 MINUTE))
        AS interval_start_utc
    FROM bounds
)
SELECT
    spine.interval_start_utc,
    timezone('Europe/Brussels', spine.interval_start_utc) AS interval_start_local,
    load.load_mw,
    load.load_forecast_day_ahead_mw,
    solar.solar_mw,
    solar.solar_forecast_day_ahead_mw,
    CASE WHEN wind.wind_parts_present = 5 THEN wind.wind_sum_mw END AS wind_mw,
    CASE WHEN wind.wind_forecast_parts = 5 THEN wind.wind_forecast_sum_mw END
        AS wind_forecast_day_ahead_mw,
    coalesce(wind.wind_parts_present, 0) AS wind_parts_present
FROM spine
LEFT JOIN load USING (interval_start_utc)
LEFT JOIN solar USING (interval_start_utc)
LEFT JOIN wind USING (interval_start_utc)
ORDER BY spine.interval_start_utc
