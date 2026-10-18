-- Elia ods001: Belgian grid load per quarter-hour, latest ingested version of each interval.
-- Input: raw_elia_load (every raw file of the stream, plus `filename`).
SELECT
    datetime AS interval_start_utc,
    resolutioncode AS resolution,
    totalload AS load_mw,
    mostrecentforecast AS load_forecast_most_recent_mw,
    mostrecentconfidence10 AS load_forecast_most_recent_p10_mw,
    mostrecentconfidence90 AS load_forecast_most_recent_p90_mw,
    dayaheadforecast AS load_forecast_day_ahead_mw,
    dayaheadconfidence10 AS load_forecast_day_ahead_p10_mw,
    dayaheadconfidence90 AS load_forecast_day_ahead_p90_mw,
    weekaheadforecast AS load_forecast_week_ahead_mw,
    _ingested_at AS ingested_at
FROM raw_elia_load
QUALIFY row_number() OVER (
    PARTITION BY datetime ORDER BY _ingested_at DESC, filename DESC
) = 1
ORDER BY interval_start_utc
