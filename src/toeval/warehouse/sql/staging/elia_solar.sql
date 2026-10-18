-- Elia ods032: solar generation per quarter-hour for Belgium, its 3 regions and 10 provinces.
-- The levels overlap (Belgium = sum of the regions), so never sum all rows; see
-- docs/decisions/0003. Latest ingested version of each (interval, region).
-- Input: raw_elia_solar.
SELECT
    datetime AS interval_start_utc,
    resolutioncode AS resolution,
    region,
    CASE
        WHEN region = 'Belgium' THEN 'country'
        WHEN region IN ('Flanders', 'Wallonia', 'Brussels') THEN 'region'
        ELSE 'province'
    END AS level,
    measured AS solar_mw,
    mostrecentforecast AS solar_forecast_most_recent_mw,
    mostrecentconfidence10 AS solar_forecast_most_recent_p10_mw,
    mostrecentconfidence90 AS solar_forecast_most_recent_p90_mw,
    dayahead11hforecast AS solar_forecast_day_ahead_11h_mw,
    dayaheadforecast AS solar_forecast_day_ahead_mw,
    dayaheadconfidence10 AS solar_forecast_day_ahead_p10_mw,
    dayaheadconfidence90 AS solar_forecast_day_ahead_p90_mw,
    weekaheadforecast AS solar_forecast_week_ahead_mw,
    monitoredcapacity AS monitored_capacity_mw,
    loadfactor AS load_factor_pct,
    _ingested_at AS ingested_at
FROM raw_elia_solar
QUALIFY row_number() OVER (
    PARTITION BY datetime, region ORDER BY _ingested_at DESC, filename DESC
) = 1
ORDER BY interval_start_utc, region
