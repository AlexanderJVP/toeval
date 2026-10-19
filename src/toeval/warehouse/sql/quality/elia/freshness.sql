-- Hours between getvariable('now') and the last measured quarter-hour of each national series.
-- Elia publishes wind and solar a day behind, so the threshold (StreamSpec.freshness_limit, 72 h) allows for that.
WITH latest AS (
    SELECT 'elia/load' AS subject, max(interval_start_utc) FILTER (WHERE load_mw IS NOT NULL) AS t
    FROM mart_energy_15min
    UNION ALL
    SELECT 'elia/solar', max(interval_start_utc) FILTER (WHERE solar_mw IS NOT NULL)
    FROM mart_energy_15min
    UNION ALL
    SELECT 'elia/wind', max(interval_start_utc) FILTER (WHERE wind_mw IS NOT NULL)
    FROM mart_energy_15min
)
SELECT
    subject,
    ((epoch(getvariable('now')::TIMESTAMPTZ) - epoch(t)) / 3600)::DOUBLE AS value,
    format('last measured {}', t) AS details
FROM latest
