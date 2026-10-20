-- Air quality per station and UTC hour [hour_start_utc, +1 h), aligned with mart_energy_hourly
-- and mart_weather_hourly (docs/decisions/0009, "Time alignment"): IRCEL's value at time t is the
-- mean over [t - 1 h, t), so hour_start_utc = hour_end_utc - 1 h.
-- One row per station and hour on a gap-free grid, from its first to its last hour with any
-- pollutant, so missing data shows up as NULL; a pollutant a station doesn't measure is NULL
-- throughout. The station label and coordinates are the latest published ones.
-- hour_start_local is Brussels time and repeats in October.
-- Input: stg_irceline_hourly.
WITH s AS (
    SELECT *, hour_end_utc - INTERVAL 1 HOUR AS hour_start_utc FROM stg_irceline_hourly
),
stations AS (
    SELECT
        station,
        arg_max(station_label, hour_end_utc) AS station_label,
        arg_max(latitude, hour_end_utc) AS latitude,
        arg_max(longitude, hour_end_utc) AS longitude,
        min(hour_start_utc) AS first_hour,
        max(hour_start_utc) AS last_hour
    FROM s
    GROUP BY station
),
spine AS (
    SELECT
        station,
        unnest(generate_series(first_hour, last_hour, INTERVAL 1 HOUR)) AS hour_start_utc
    FROM stations
),
pivoted AS (
    SELECT
        station,
        hour_start_utc,
        max(value_ugm3) FILTER (WHERE pollutant = 'no2') AS no2_ugm3,
        max(value_ugm3) FILTER (WHERE pollutant = 'pm10') AS pm10_ugm3,
        max(value_ugm3) FILTER (WHERE pollutant = 'pm25') AS pm25_ugm3,
        max(value_ugm3) FILTER (WHERE pollutant = 'o3') AS o3_ugm3
    FROM s
    GROUP BY station, hour_start_utc
)
SELECT
    spine.station,
    stations.station_label,
    stations.latitude,
    stations.longitude,
    spine.hour_start_utc,
    timezone('Europe/Brussels', spine.hour_start_utc) AS hour_start_local,
    pivoted.no2_ugm3,
    pivoted.pm10_ugm3,
    pivoted.pm25_ugm3,
    pivoted.o3_ugm3
FROM spine
JOIN stations USING (station)
LEFT JOIN pivoted USING (station, hour_start_utc)
ORDER BY spine.station, spine.hour_start_utc
