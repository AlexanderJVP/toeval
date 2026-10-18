-- How old a measured value was when Elia changed it, from successive raw versions of the same
-- natural key (docs/decisions/0002 §3). A revision = a measured value that differs from the
-- previous raw version, which already had one (first measurements are not revisions).
-- value = age in days of the oldest interval revised in the last 30 days (by ingestion time), at
-- the time of the revision. It warns when revisions come close to the edge of the revision
-- window (threshold in transform.py). Only recent revisions count, so one old outlier doesn't
-- warn forever; the all-time maximum is in the details. Revisions older than the window are
-- only seen by the monthly deep re-fetch.
WITH versions AS (
    SELECT 'elia/load' AS subject, '' AS part, datetime AS t, totalload AS v, _ingested_at
    FROM raw_elia_load
    UNION ALL
    SELECT 'elia/wind', concat_ws('/', offshoreonshore, region, gridconnectiontype), datetime,
           measured, _ingested_at
    FROM raw_elia_wind
    UNION ALL
    SELECT 'elia/solar', region, datetime, measured, _ingested_at
    FROM raw_elia_solar
),
changes AS (
    SELECT subject, t, _ingested_at, v,
           lag(v) OVER (PARTITION BY subject, part, t ORDER BY _ingested_at) AS previous
    FROM versions
),
revisions AS (
    SELECT
        subject,
        (epoch(_ingested_at) - epoch(t)) / 86400 AS age_days,
        _ingested_at > getvariable('now')::TIMESTAMPTZ - INTERVAL 30 DAY AS recent
    FROM changes
    WHERE previous IS NOT NULL AND v IS DISTINCT FROM previous
),
subjects AS (
    SELECT * FROM (VALUES ('elia/load'), ('elia/wind'), ('elia/solar')) s(subject)
)
SELECT
    subject,
    coalesce(max(age_days) FILTER (WHERE recent), 0)::DOUBLE AS value,
    format('{} revised in the last 30 days (median age {} days); all history: {}, max {} days',
           count(age_days) FILTER (WHERE recent),
           coalesce(round(median(age_days) FILTER (WHERE recent), 1), 0),
           count(age_days), coalesce(round(max(age_days), 1), 0)) AS details
FROM subjects LEFT JOIN revisions USING (subject)
GROUP BY subject
