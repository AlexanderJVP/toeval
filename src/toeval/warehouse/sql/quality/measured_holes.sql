-- Quarter-hours without a measured value inside the measured range of each national series
-- (first..last measured interval), from mart_energy_15min. For wind this includes intervals
-- where not all 5 parts were present.
-- value = such quarter-hours in the 30 days before the last measured one: those are new and
-- actionable. Older holes are known source issues (docs/decisions/0006) and only reported in
-- the details, so they don't raise the same warning forever.
WITH series AS (
    SELECT 'elia/load' AS subject, interval_start_utc AS t, load_mw AS v FROM mart_energy_15min
    UNION ALL SELECT 'elia/solar', interval_start_utc, solar_mw FROM mart_energy_15min
    UNION ALL SELECT 'elia/wind', interval_start_utc, wind_mw FROM mart_energy_15min
),
measured_range AS (
    SELECT subject, min(t) FILTER (WHERE v IS NOT NULL) AS first_t,
           max(t) FILTER (WHERE v IS NOT NULL) AS last_t
    FROM series GROUP BY subject
),
holes AS (
    SELECT s.subject, s.t, s.t > r.last_t - INTERVAL 30 DAY AS recent
    FROM series s JOIN measured_range r USING (subject)
    WHERE s.t BETWEEN r.first_t AND r.last_t AND s.v IS NULL
)
SELECT
    r.subject,
    count(h.t) FILTER (WHERE h.recent)::DOUBLE AS value,
    format('first recent: {}; all history: {} (last {})',
           coalesce(min(h.t) FILTER (WHERE h.recent)::VARCHAR, 'none'), count(h.t),
           coalesce(max(h.t)::VARCHAR, 'none')) AS details
FROM measured_range r LEFT JOIN holes h USING (subject)
GROUP BY r.subject
