-- National energy and weather per UTC hour [hour_start_utc, +1 h): one wide row per hour of
-- mart_energy_hourly, for analyses that need both (docs/decisions/0008).
--   * land weather = the mean over the 6 land points, NULL unless all 6 have a value, so a
--     missing point never shifts the average;
--   * offshore wind (speed and direction at 100 m) is kept separate: it drives offshore output;
--   * air quality from ONE fixed reference station, 42R801 Borgerhout (Antwerp; all four
--     pollutants since 2011), not a mean of the stations present: stations come and go
--     (42R804 starts in 2017), which would shift the level for reasons unrelated to the air
--     (docs/decisions/0009, "Join"). An hour without its value stays NULL.
-- Per-location values stay in mart_weather_hourly, per-station ones in mart_air_hourly. ERA5 runs ~6 days behind, so the latest
-- days (and Elia's load forecast hours) have energy but no weather yet.
-- Inputs: mart_energy_hourly, mart_weather_hourly, mart_air_hourly.
WITH land AS (
    SELECT
        hour_start_utc,
        count(temperature_2m_c) AS land_points,
        CASE WHEN count(temperature_2m_c) = 6 THEN avg(temperature_2m_c) END AS temperature_2m_c,
        CASE WHEN count(relative_humidity_2m_pct) = 6 THEN avg(relative_humidity_2m_pct) END
            AS relative_humidity_2m_pct,
        CASE WHEN count(precipitation_mm) = 6 THEN avg(precipitation_mm) END AS precipitation_mm,
        CASE WHEN count(cloud_cover_pct) = 6 THEN avg(cloud_cover_pct) END AS cloud_cover_pct,
        CASE WHEN count(pressure_msl_hpa) = 6 THEN avg(pressure_msl_hpa) END AS pressure_msl_hpa,
        CASE WHEN count(wind_speed_10m_ms) = 6 THEN avg(wind_speed_10m_ms) END
            AS wind_speed_10m_ms,
        CASE WHEN count(wind_speed_100m_ms) = 6 THEN avg(wind_speed_100m_ms) END
            AS wind_speed_100m_ms,
        CASE WHEN count(shortwave_radiation_wm2) = 6 THEN avg(shortwave_radiation_wm2) END
            AS shortwave_radiation_wm2
    FROM mart_weather_hourly
    WHERE location <> 'offshore'
    GROUP BY hour_start_utc
),
offshore AS (
    SELECT
        hour_start_utc,
        wind_speed_100m_ms AS offshore_wind_speed_100m_ms,
        wind_direction_100m_deg AS offshore_wind_direction_100m_deg
    FROM mart_weather_hourly
    WHERE location = 'offshore'
),
air AS (
    SELECT
        hour_start_utc,
        no2_ugm3 AS air_antwerp_no2_ugm3,
        pm10_ugm3 AS air_antwerp_pm10_ugm3,
        pm25_ugm3 AS air_antwerp_pm25_ugm3,
        o3_ugm3 AS air_antwerp_o3_ugm3
    FROM mart_air_hourly
    WHERE station = '42R801'
)
SELECT
    e.hour_start_utc,
    e.hour_start_local,
    e.load_mw,
    e.load_forecast_day_ahead_mw,
    e.solar_mw,
    e.solar_forecast_day_ahead_mw,
    e.wind_mw,
    e.wind_forecast_day_ahead_mw,
    land.temperature_2m_c,
    land.relative_humidity_2m_pct,
    land.precipitation_mm,
    land.cloud_cover_pct,
    land.pressure_msl_hpa,
    land.wind_speed_10m_ms,
    land.wind_speed_100m_ms,
    land.shortwave_radiation_wm2,
    offshore.offshore_wind_speed_100m_ms,
    offshore.offshore_wind_direction_100m_deg,
    coalesce(land.land_points, 0) AS weather_land_points,
    air.air_antwerp_no2_ugm3,
    air.air_antwerp_pm10_ugm3,
    air.air_antwerp_pm25_ugm3,
    air.air_antwerp_o3_ugm3
FROM mart_energy_hourly AS e
LEFT JOIN land USING (hour_start_utc)
LEFT JOIN offshore USING (hour_start_utc)
LEFT JOIN air USING (hour_start_utc)
ORDER BY e.hour_start_utc
