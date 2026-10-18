-- mart_energy_15min rolled up to UTC hours. A value is the mean of the hour's four quarter-hours
-- (MW; numerically equal to MWh for the hour) and is NULL unless all four are present, so a
-- partial hour never looks like a dip. The *_quarters columns say how many were present.
-- A Brussels local day has 23, 24 or 25 of these hours.
-- Input: mart_energy_15min.
SELECT
    time_bucket(INTERVAL 1 HOUR, interval_start_utc) AS hour_start_utc,
    timezone('Europe/Brussels', time_bucket(INTERVAL 1 HOUR, interval_start_utc))
        AS hour_start_local,
    CASE WHEN count(load_mw) = 4 THEN avg(load_mw) END AS load_mw,
    CASE WHEN count(load_forecast_day_ahead_mw) = 4 THEN avg(load_forecast_day_ahead_mw) END
        AS load_forecast_day_ahead_mw,
    CASE WHEN count(solar_mw) = 4 THEN avg(solar_mw) END AS solar_mw,
    CASE WHEN count(solar_forecast_day_ahead_mw) = 4 THEN avg(solar_forecast_day_ahead_mw) END
        AS solar_forecast_day_ahead_mw,
    CASE WHEN count(wind_mw) = 4 THEN avg(wind_mw) END AS wind_mw,
    CASE WHEN count(wind_forecast_day_ahead_mw) = 4 THEN avg(wind_forecast_day_ahead_mw) END
        AS wind_forecast_day_ahead_mw,
    count(load_mw) AS load_quarters,
    count(solar_mw) AS solar_quarters,
    count(wind_mw) AS wind_quarters
FROM mart_energy_15min
GROUP BY ALL
ORDER BY hour_start_utc
