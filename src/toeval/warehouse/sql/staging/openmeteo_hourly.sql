-- Open-Meteo ERA5: hourly weather at 7 points (docs/decisions/0008), latest ingested version of
-- each (location, time). time_utc is the API's timestamp, unchanged: precipitation and
-- shortwave radiation cover the hour *before* it, the other values are snapshots at it. The
-- alignment to hour intervals happens in mart_weather_hourly.
-- Input: raw_openmeteo_hourly.
SELECT
    location,
    grid_latitude,
    grid_longitude,
    time AS time_utc,
    temperature_2m AS temperature_2m_c,
    relative_humidity_2m AS relative_humidity_2m_pct,
    precipitation AS precipitation_mm,
    cloud_cover AS cloud_cover_pct,
    pressure_msl AS pressure_msl_hpa,
    wind_speed_10m AS wind_speed_10m_ms,
    wind_speed_100m AS wind_speed_100m_ms,
    wind_direction_100m AS wind_direction_100m_deg,
    shortwave_radiation AS shortwave_radiation_wm2,
    _ingested_at AS ingested_at
FROM raw_openmeteo_hourly
QUALIFY row_number() OVER (
    PARTITION BY location, time ORDER BY _ingested_at DESC, filename DESC
) = 1
ORDER BY location, time_utc
