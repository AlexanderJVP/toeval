-- Elia ods031: wind generation per quarter-hour for 5 disjoint parts (no national total row;
-- see docs/decisions/0003), latest ingested version of each (interval, part).
-- Input: raw_elia_wind.
SELECT
    datetime AS interval_start_utc,
    resolutioncode AS resolution,
    offshoreonshore AS offshore_onshore,
    region,
    gridconnectiontype AS grid_connection_type,
    measured AS wind_mw,
    mostrecentforecast AS wind_forecast_most_recent_mw,
    mostrecentconfidence10 AS wind_forecast_most_recent_p10_mw,
    mostrecentconfidence90 AS wind_forecast_most_recent_p90_mw,
    dayahead11hforecast AS wind_forecast_day_ahead_11h_mw,
    dayaheadforecast AS wind_forecast_day_ahead_mw,
    dayaheadconfidence10 AS wind_forecast_day_ahead_p10_mw,
    dayaheadconfidence90 AS wind_forecast_day_ahead_p90_mw,
    weekaheadforecast AS wind_forecast_week_ahead_mw,
    monitoredcapacity AS monitored_capacity_mw,
    loadfactor AS load_factor_pct,
    decrementalbidid AS decremental_bid_id,
    _ingested_at AS ingested_at
FROM raw_elia_wind
QUALIFY row_number() OVER (
    PARTITION BY datetime, offshoreonshore, region, gridconnectiontype
    ORDER BY _ingested_at DESC, filename DESC
) = 1
ORDER BY interval_start_utc, offshore_onshore, region, grid_connection_type
