-- How old an hour was when IRCEL changed its value, from successive raw versions of the same
-- (station, pollutant, time); see elia/revision_lag.sql for the method. This is the evidence
-- for the provisional 400-day re-check (docs/decisions/0009): whether validated values replace
-- the provisional ones in this API is undocumented. value = age in days of the oldest hour
-- revised in the last 30 days.
WITH changes AS (
    SELECT
        time,
        _ingested_at,
        value,
        lag(value) OVER (PARTITION BY station, pollutant, time ORDER BY _ingested_at) AS previous
    FROM raw_irceline_hourly
),
revisions AS (
    SELECT
        (epoch(_ingested_at) - epoch(time)) / 86400 AS age_days,
        _ingested_at > getvariable('now')::TIMESTAMPTZ - INTERVAL 30 DAY AS recent
    FROM changes
    WHERE previous IS NOT NULL AND value IS DISTINCT FROM previous
)
SELECT
    'irceline/hourly' AS subject,
    coalesce(max(age_days) FILTER (WHERE recent), 0)::DOUBLE AS value,
    format('{} revised in the last 30 days (median age {} days); all history: {}, max {} days',
           count(age_days) FILTER (WHERE recent),
           coalesce(round(median(age_days) FILTER (WHERE recent), 1), 0),
           count(age_days), coalesce(round(max(age_days), 1), 0)) AS details
FROM revisions
