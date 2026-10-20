-- Series with values for fewer than 75% of the hours in the 30 days before the newest value of
-- any series (missing hours are nulls the API publishes, dropped at ingestion). Live data on
-- 2026-10-20: ~97% per series, the worst 74%. Only series with a value in that window count;
-- one that stopped earlier is stale_series' business. value = such series.
WITH bounds AS (
    SELECT max(hour_end_utc) AS newest FROM stg_irceline_hourly
),
recent AS (
    SELECT station || ' ' || pollutant AS series, count(*) / 720 AS share
    FROM stg_irceline_hourly, bounds
    WHERE hour_end_utc > newest - INTERVAL 30 DAY
    GROUP BY ALL
)
SELECT
    'irceline/hourly' AS subject,
    count(*) FILTER (WHERE share < 0.75)::DOUBLE AS value,
    format('below 75%: {}; {} series, median {}%',
           coalesce(string_agg(series || ' ' || round(100 * share) || '%', ', ' ORDER BY series)
                    FILTER (WHERE share < 0.75), 'none'),
           count(*), coalesce(round(100 * median(share)), 0)) AS details
FROM recent
