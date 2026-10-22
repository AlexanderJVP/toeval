-- One row per ISO week (Monday 00:00 to Monday 00:00 Brussels, docs/decisions/0010), built from
-- mart_daily's daily values, not from hours: 90% of a week's hours could miss a whole Saturday
-- and break the weekday/weekend pattern. A weekly value is the mean of the 7 daily values (the
-- sum, for precipitation) and is NULL unless all 7 days have one; a missing weekday would add an
-- error of about 10% of the week-to-week variation (ADR 0010). Weeks at the edges of the data
-- have fewer than 7 days and so no values. *_days count the days with a value. Days past the end
-- of the calendar have no week_start (and no values) and are left out; mart_daily keeps them.
-- Input: mart_daily.
SELECT
    week_start,
    any_value(iso_year) AS iso_year,
    any_value(iso_week) AS iso_week,
    count(*) AS days,
    sum(day_hours)::INTEGER AS week_hours,
    count(public_holiday) AS public_holidays,
    count(school_holiday_flanders) AS school_holiday_days_flanders,
    CASE WHEN count(load_mw) = 7 THEN avg(load_mw) END AS load_mw,
    CASE WHEN count(load_forecast_day_ahead_mw) = 7 THEN avg(load_forecast_day_ahead_mw) END
        AS load_forecast_day_ahead_mw,
    CASE WHEN count(solar_mw) = 7 THEN avg(solar_mw) END AS solar_mw,
    CASE WHEN count(solar_forecast_day_ahead_mw) = 7 THEN avg(solar_forecast_day_ahead_mw) END
        AS solar_forecast_day_ahead_mw,
    CASE WHEN count(wind_mw) = 7 THEN avg(wind_mw) END AS wind_mw,
    CASE WHEN count(wind_forecast_day_ahead_mw) = 7 THEN avg(wind_forecast_day_ahead_mw) END
        AS wind_forecast_day_ahead_mw,
    CASE WHEN count(temperature_2m_c) = 7 THEN avg(temperature_2m_c) END AS temperature_2m_c,
    CASE WHEN count(relative_humidity_2m_pct) = 7 THEN avg(relative_humidity_2m_pct) END
        AS relative_humidity_2m_pct,
    CASE WHEN count(precipitation_mm) = 7 THEN sum(precipitation_mm) END AS precipitation_mm,
    CASE WHEN count(cloud_cover_pct) = 7 THEN avg(cloud_cover_pct) END AS cloud_cover_pct,
    CASE WHEN count(pressure_msl_hpa) = 7 THEN avg(pressure_msl_hpa) END AS pressure_msl_hpa,
    CASE WHEN count(wind_speed_10m_ms) = 7 THEN avg(wind_speed_10m_ms) END AS wind_speed_10m_ms,
    CASE WHEN count(wind_speed_100m_ms) = 7 THEN avg(wind_speed_100m_ms) END
        AS wind_speed_100m_ms,
    CASE WHEN count(shortwave_radiation_wm2) = 7 THEN avg(shortwave_radiation_wm2) END
        AS shortwave_radiation_wm2,
    CASE WHEN count(offshore_wind_speed_100m_ms) = 7 THEN avg(offshore_wind_speed_100m_ms) END
        AS offshore_wind_speed_100m_ms,
    CASE WHEN count(air_antwerp_no2_ugm3) = 7 THEN avg(air_antwerp_no2_ugm3) END
        AS air_antwerp_no2_ugm3,
    CASE WHEN count(air_antwerp_pm10_ugm3) = 7 THEN avg(air_antwerp_pm10_ugm3) END
        AS air_antwerp_pm10_ugm3,
    CASE WHEN count(air_antwerp_pm25_ugm3) = 7 THEN avg(air_antwerp_pm25_ugm3) END
        AS air_antwerp_pm25_ugm3,
    CASE WHEN count(air_antwerp_o3_ugm3) = 7 THEN avg(air_antwerp_o3_ugm3) END
        AS air_antwerp_o3_ugm3,
    count(load_mw) AS load_days,
    count(solar_mw) AS solar_days,
    count(wind_mw) AS wind_days,
    count(temperature_2m_c) AS weather_days,
    count(air_antwerp_no2_ugm3) AS air_antwerp_no2_days,
    count(air_antwerp_pm10_ugm3) AS air_antwerp_pm10_days,
    count(air_antwerp_pm25_ugm3) AS air_antwerp_pm25_days,
    count(air_antwerp_o3_ugm3) AS air_antwerp_o3_days
FROM mart_daily
WHERE week_start IS NOT NULL
GROUP BY week_start
ORDER BY week_start
