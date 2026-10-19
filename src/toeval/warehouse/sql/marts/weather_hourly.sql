-- ERA5 weather per location and UTC hour [hour_start_utc, +1 h), aligned with
-- mart_energy_hourly (docs/decisions/0008, "Time alignment"):
--   * precipitation and shortwave radiation at time h+1 cover [h, h+1): taken from h+1;
--   * snapshots (temperature, humidity, cloud cover, pressure, wind speed) at h and h+1 are
--     averaged, the trapezoid estimate of the hour's mean. NULL unless both exist;
--   * wind direction is the direction of the mean wind vector of those two snapshots, because
--     degrees can't be averaged (350 and 10 would give 180). NULL in calm air (no direction).
-- One row per location and hour on a gap-free grid, from its first to its last snapshot, so
-- missing data shows up as NULL. hour_start_local is Brussels time and repeats in October.
-- Input: stg_openmeteo_hourly.
WITH s AS (
    SELECT
        *,
        -- Meteorological direction is where the wind comes *from*: u, v point where it blows.
        -wind_speed_100m_ms * sin(radians(wind_direction_100m_deg)) AS u_100m,
        -wind_speed_100m_ms * cos(radians(wind_direction_100m_deg)) AS v_100m
    FROM stg_openmeteo_hourly
),
spine AS (
    SELECT
        location,
        unnest(generate_series(min(time_utc), max(time_utc) - INTERVAL 1 HOUR, INTERVAL 1 HOUR))
            AS hour_start_utc
    FROM s
    GROUP BY location
),
hours AS (
    SELECT
        spine.location,
        spine.hour_start_utc,
        (a.temperature_2m_c + b.temperature_2m_c) / 2 AS temperature_2m_c,
        (a.relative_humidity_2m_pct + b.relative_humidity_2m_pct) / 2 AS relative_humidity_2m_pct,
        b.precipitation_mm,
        (a.cloud_cover_pct + b.cloud_cover_pct) / 2 AS cloud_cover_pct,
        (a.pressure_msl_hpa + b.pressure_msl_hpa) / 2 AS pressure_msl_hpa,
        (a.wind_speed_10m_ms + b.wind_speed_10m_ms) / 2 AS wind_speed_10m_ms,
        (a.wind_speed_100m_ms + b.wind_speed_100m_ms) / 2 AS wind_speed_100m_ms,
        (a.u_100m + b.u_100m) / 2 AS u_100m,
        (a.v_100m + b.v_100m) / 2 AS v_100m,
        b.shortwave_radiation_wm2
    FROM spine
    LEFT JOIN s AS a
        ON a.location = spine.location AND a.time_utc = spine.hour_start_utc
    LEFT JOIN s AS b
        ON b.location = spine.location AND b.time_utc = spine.hour_start_utc + INTERVAL 1 HOUR
)
SELECT
    location,
    hour_start_utc,
    timezone('Europe/Brussels', hour_start_utc) AS hour_start_local,
    temperature_2m_c,
    relative_humidity_2m_pct,
    precipitation_mm,
    cloud_cover_pct,
    pressure_msl_hpa,
    wind_speed_10m_ms,
    wind_speed_100m_ms,
    CASE
        WHEN u_100m * u_100m + v_100m * v_100m > 1e-6
        THEN (degrees(atan2(-u_100m, -v_100m)) + 360) % 360
    END AS wind_direction_100m_deg,
    shortwave_radiation_wm2
FROM hours
ORDER BY location, hour_start_utc
