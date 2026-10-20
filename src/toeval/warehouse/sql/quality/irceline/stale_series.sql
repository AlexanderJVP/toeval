-- Series (station + pollutant) that stopped reporting while the source kept delivering: their
-- last value is more than 48 h older than the newest value of any series. Measured against the
-- newest value, not now, so a dead source raises freshness only, not 54 of these.
-- value = series that went quiet within the 30 days before the newest value: those are new and
-- actionable. A series quiet for longer (a retired instrument) is only listed in the details, so
-- it doesn't warn forever. A short outage (a day of maintenance) is normal and not counted.
WITH latest AS (
    SELECT station, pollutant, max(hour_end_utc) AS t FROM stg_irceline_hourly GROUP BY ALL
),
quiet AS (
    SELECT
        station || ' ' || pollutant AS series,
        t,
        t >= max(t) OVER () - INTERVAL 30 DAY AS recent,
        t < max(t) OVER () - INTERVAL 48 HOUR AS stale
    FROM latest
)
SELECT
    'irceline/hourly' AS subject,
    count(*) FILTER (WHERE stale AND recent)::DOUBLE AS value,
    format('quiet: {}; quiet for over 30 days: {}',
           coalesce(string_agg(series || ' since ' || t, ', ' ORDER BY series)
                    FILTER (WHERE stale AND recent), 'none'),
           coalesce(string_agg(series, ', ' ORDER BY series) FILTER (WHERE stale AND NOT recent),
                    'none')) AS details
FROM quiet
