-- One row per Brussels day (docs/decisions/0010): the calendar plus mart_energy_weather_hourly
-- aggregated over the day's real hours (day_hours: 23, 24 or 25), from the first to the last day
-- with any hourly row. Missing hours are never filled in:
--   * a mean is NULL unless at least 90% of the day's hours have a value (>= 22 of 24, 21 of 23,
--     23 of 25). Calibrated on the real data: the hours that go missing bias the mean by under 2%
--     of the day-to-day variation, even in the worst case (ADR 0010, "Calibration");
--   * precipitation is a sum, so it is NULL unless every hour has a value;
--   * offshore wind direction (degrees can't be averaged) and weather_land_points are left out.
-- *_hours count the hours with a value for the main series. Means of MW are numerically the
-- average power; times day_hours they give MWh.
-- Inputs: mart_energy_weather_hourly, mart_calendar_day.
WITH h AS (
    SELECT *, hour_start_local::DATE AS day FROM mart_energy_weather_hourly
),
agg AS (
    SELECT
        h.day,
        any_value(c.day_hours) AS day_hours,
        CASE WHEN count(load_mw) >= 0.9 * any_value(c.day_hours) THEN avg(load_mw) END
            AS load_mw,
        CASE WHEN count(load_forecast_day_ahead_mw) >= 0.9 * any_value(c.day_hours)
            THEN avg(load_forecast_day_ahead_mw) END AS load_forecast_day_ahead_mw,
        CASE WHEN count(solar_mw) >= 0.9 * any_value(c.day_hours) THEN avg(solar_mw) END
            AS solar_mw,
        CASE WHEN count(solar_forecast_day_ahead_mw) >= 0.9 * any_value(c.day_hours)
            THEN avg(solar_forecast_day_ahead_mw) END AS solar_forecast_day_ahead_mw,
        CASE WHEN count(wind_mw) >= 0.9 * any_value(c.day_hours) THEN avg(wind_mw) END
            AS wind_mw,
        CASE WHEN count(wind_forecast_day_ahead_mw) >= 0.9 * any_value(c.day_hours)
            THEN avg(wind_forecast_day_ahead_mw) END AS wind_forecast_day_ahead_mw,
        CASE WHEN count(temperature_2m_c) >= 0.9 * any_value(c.day_hours)
            THEN avg(temperature_2m_c) END AS temperature_2m_c,
        CASE WHEN count(relative_humidity_2m_pct) >= 0.9 * any_value(c.day_hours)
            THEN avg(relative_humidity_2m_pct) END AS relative_humidity_2m_pct,
        CASE WHEN count(precipitation_mm) = any_value(c.day_hours)
            THEN sum(precipitation_mm) END AS precipitation_mm,
        CASE WHEN count(cloud_cover_pct) >= 0.9 * any_value(c.day_hours)
            THEN avg(cloud_cover_pct) END AS cloud_cover_pct,
        CASE WHEN count(pressure_msl_hpa) >= 0.9 * any_value(c.day_hours)
            THEN avg(pressure_msl_hpa) END AS pressure_msl_hpa,
        CASE WHEN count(wind_speed_10m_ms) >= 0.9 * any_value(c.day_hours)
            THEN avg(wind_speed_10m_ms) END AS wind_speed_10m_ms,
        CASE WHEN count(wind_speed_100m_ms) >= 0.9 * any_value(c.day_hours)
            THEN avg(wind_speed_100m_ms) END AS wind_speed_100m_ms,
        CASE WHEN count(shortwave_radiation_wm2) >= 0.9 * any_value(c.day_hours)
            THEN avg(shortwave_radiation_wm2) END AS shortwave_radiation_wm2,
        CASE WHEN count(offshore_wind_speed_100m_ms) >= 0.9 * any_value(c.day_hours)
            THEN avg(offshore_wind_speed_100m_ms) END AS offshore_wind_speed_100m_ms,
        CASE WHEN count(air_antwerp_no2_ugm3) >= 0.9 * any_value(c.day_hours)
            THEN avg(air_antwerp_no2_ugm3) END AS air_antwerp_no2_ugm3,
        CASE WHEN count(air_antwerp_pm10_ugm3) >= 0.9 * any_value(c.day_hours)
            THEN avg(air_antwerp_pm10_ugm3) END AS air_antwerp_pm10_ugm3,
        CASE WHEN count(air_antwerp_pm25_ugm3) >= 0.9 * any_value(c.day_hours)
            THEN avg(air_antwerp_pm25_ugm3) END AS air_antwerp_pm25_ugm3,
        CASE WHEN count(air_antwerp_o3_ugm3) >= 0.9 * any_value(c.day_hours)
            THEN avg(air_antwerp_o3_ugm3) END AS air_antwerp_o3_ugm3,
        count(load_mw) AS load_hours,
        count(solar_mw) AS solar_hours,
        count(wind_mw) AS wind_hours,
        count(temperature_2m_c) AS weather_hours,
        count(air_antwerp_no2_ugm3) AS air_antwerp_no2_hours,
        count(air_antwerp_pm10_ugm3) AS air_antwerp_pm10_hours,
        count(air_antwerp_pm25_ugm3) AS air_antwerp_pm25_hours,
        count(air_antwerp_o3_ugm3) AS air_antwerp_o3_hours
    FROM h
    JOIN mart_calendar_day AS c USING (day)
    GROUP BY h.day
)
SELECT
    c.day,
    c.day_start_utc,
    c.day_hours,
    c.iso_year,
    c.iso_week,
    c.week_start,
    c.iso_weekday,
    c.is_weekend,
    c.public_holiday,
    c.school_holiday_flanders,
    agg.* EXCLUDE (day, day_hours)
FROM mart_calendar_day AS c
LEFT JOIN agg USING (day)
WHERE c.day BETWEEN (SELECT min(day) FROM h) AND (SELECT max(day) FROM h)
ORDER BY c.day
