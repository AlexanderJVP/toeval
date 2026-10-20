-- Physically implausible values: below -5 or above 1000 µg/m³. Small negatives are measurement
-- noise near zero (PM2.5 down to -0.3 in the live data) and peaks of several hundred are real
-- (PM10 473 in the Ghent port area, 2026), so this only catches broken sensors or sentinel
-- values. A soft check: the values stay in staging and the marts as published (no imputation),
-- because one spike must not quarantine a whole window (ADR 0009).
-- value = such values in the 30 days before the newest value; older ones only in the details.
WITH bounds AS (
    SELECT max(hour_end_utc) AS newest FROM stg_irceline_hourly
),
bad AS (
    SELECT station, pollutant, hour_end_utc, value_ugm3,
           hour_end_utc > newest - INTERVAL 30 DAY AS recent
    FROM stg_irceline_hourly, bounds
    WHERE value_ugm3 < -5 OR value_ugm3 > 1000
)
SELECT
    'irceline/hourly' AS subject,
    count(*) FILTER (WHERE recent)::DOUBLE AS value,
    format('recent: {}; all history: {}',
           coalesce(string_agg(station || ' ' || pollutant || ' ' || value_ugm3 || ' at '
                               || hour_end_utc, ', ' ORDER BY hour_end_utc)
                    FILTER (WHERE recent), 'none'),
           count(*)) AS details
FROM bad
