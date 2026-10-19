# 0008. Open-Meteo ERA5 weather for 7 points around Belgium

- Status: accepted (step 1 of 3 built: connector and raw data; the stream starts paused)
- Date: 2026-10-19

## Context

Phase 2 adds sources that line up with Elia in time. Weather comes first: it has decades of history, it is what drives wind and solar output, and both later applications (the correlation tracker and the JEPA model) need long, consistent series. The owner chose "a handful of points around Belgium", ERA5 only, and more data over less.

## Facts (verified 2026-10-19)

From Open-Meteo's documentation and pricing pages, and from live requests:

- `GET https://archive-api.open-meteo.com/v1/archive`, no key. Free for **non-commercial use** only; licence **CC-BY 4.0** (credit Open-Meteo and Copernicus ERA5).
- Limits: **600 calls/minute, 5,000/hour, 10,000/day**. A request counts as more than one call above 10 variables or above 2 weeks of data, fractionally (e.g. 4 weeks = 2 calls). The pricing page doesn't say how several locations in one request count; we assume one per location, which is the conservative reading.
- ERA5 (`models=era5`): 0.25° grid (~25 km), hourly since 1940, updated daily with **~5 days' delay**. It covers the sea; ERA5-Land doesn't, and the offshore point needs it. The faster ECMWF IFS model only starts in 2017, and Open-Meteo itself advises one model for long series.
- Hours ERA5 doesn't cover yet come back as **nulls, not an error** (a request ending 2026-10-19 had values up to 2026-10-13 23:00).
- `start_date` / `end_date` are inclusive dates. A date **after today is refused** with HTTP 400 (`Parameter 'end_date' is out of allowed range from 1940-01-01 to <today>`).
- Several locations per request (comma-separated coordinates); the response is a list in request order. Each point snaps to its nearest grid cell, returned as `latitude` / `longitude`.
- Size and speed: 11.8 years × 1 location × 8 variables = 5.5 MB JSON in 3 s; 1 year × 5 locations = 2.3 MB in 0.7 s.

## Decision

### Source and stream

Source `openmeteo`, one stream `hourly` (`src/toeval/connectors/openmeteo.py`). Natural key `(location, time)`.

| Location | Requested | ERA5 cell | Why |
|---|---|---|---|
| antwerp | 51.22, 4.40 | 51.25, 4.50 | city and port |
| brussels | 50.85, 4.35 | 50.75, 4.25 | centre |
| ghent | 51.05, 3.72 | 51.00, 3.75 | Flanders inland |
| liege | 50.63, 5.57 | 50.75, 5.50 | Wallonia, Meuse valley |
| ostend | 51.23, 2.92 | 51.25, 3.00 | coast, onshore wind |
| libramont | 49.92, 5.38 | 50.00, 5.50 | Ardennes, 488 m: cooler, different sun |
| offshore | 51.60, 2.90 | 51.50, 3.00 (sea, elevation 0) | Belgian offshore wind zone |

Every point has its own cell; a batch where two share one is quarantined (`validate`).

Nine variables, so no request is charged extra for them: `temperature_2m`, `relative_humidity_2m`, `precipitation`, `cloud_cover`, `pressure_msl`, `wind_speed_10m`, `wind_speed_100m` and `wind_direction_100m` (with `wind_speed_unit=ms`), `shortwave_radiation`. `timezone=GMT`. `parse` checks every unit and the GMT offset and fails the run otherwise, so a silent unit change can't reach raw.

### Call cost and pacing (the real numbers)

A request for all 7 locations costs **7 × (days / 14)** calls:

| Request | Calls |
|---|---|
| One yearly chunk (backfill) | 7 × 26 ≈ **182** |
| Full backfill 2015 → today (12 chunks) | ≈ **2,150** |
| Daily incremental (~20 days: the 14-day window plus ERA5's delay) | ≈ **10** |
| Monthly deep re-check (~126 days) | ≈ **63** |

The **per-minute** limit binds, not the hourly one: four yearly requests within a minute would already be 728 calls, over 600. `HttpPolicy(requests_per_second=1/25)` sends at most one request per 25 s, which is ~440 calls/minute during a backfill. The whole history then takes ~5 minutes and ~2,150 calls, under the 5,000/hour limit. Two full backfills within one hour would exceed it, so don't repeat one casually. Daily operation uses ~10 of the 10,000 daily calls.

### Planning

- `time_window_plan` with yearly chunks on a **UTC-midnight grid** from `backfill_start` = 2014-12-31 00:00 UTC, a day before Elia's first interval. Every window is then a whole number of API dates.
- `end_date` is capped at today (UTC).
- A backfill range that doesn't start at midnight (the CLI reads naive times as Brussels) still asks for whole dates. `fetch` keeps only rows inside `[start, end)`, so requests in one run never overlap.
- **All-null rows are dropped** in `fetch`: they are hours ERA5 hasn't produced yet, not observations. So the watermark is the last hour with a value.
- Schedule daily at 12:00 Brussels; `revision_window` 14 days.
- `deep_refetch` every 30 days over 120 days. This is **provisional**: Copernicus is said to replace its preliminary ERA5T data with final values after 2–3 months, but Open-Meteo doesn't document it. The `revision_lag` check will show whether such revisions happen and how old the revised rows are. If none appear, drop the deep re-check.

### Time alignment (applied in the marts, step 2)

Raw keeps the API's timestamps unchanged. Elia's hourly mart is keyed by `hour_start_utc` and describes `[h, h+1)`. Open-Meteo values don't all mean the same thing at their timestamp:

- **Preceding-hour values:** `precipitation` (sum) and `shortwave_radiation` (mean) at `time = h+1` cover `[h, h+1)`. They are assigned to `hour_start_utc = time − 1 h`.
- **Instantaneous values** (temperature, humidity, cloud cover, pressure, wind speed) at `time` are snapshots. The hour `[h, h+1)` gets the **mean of the snapshots at h and h+1**, the trapezoid estimate of the hour's mean, so these line up with the same interval as Elia's mean of four quarters. The hour is NULL unless both snapshots exist.
- **Wind direction** can't be averaged as a number (350° and 10° average to 180°). It is the direction of the mean wind vector: average the u/v components of the two snapshots (weighted by speed), then `atan2`.

The offshore point is the built-in check of this: Elia's offshore wind output should follow offshore `wind_speed_100m` closely. A one-hour misalignment shows up as a lower correlation at lag 0 than at lag ±1, which step 2 tests.

### Rollout

The scheduled task runs the working copy. Registering the source would start the whole 2015 → today download at the next 5-minute tick, before staging exists. So the stream was paused in the owner's `sources.toml` first, and the history download happens deliberately in step 3. `sources.toml` is now gitignored: it holds machine-specific operating state, such as paused streams and console schedule edits.

## Consequences and follow-ups

- **Step 2 is bigger than one SQL file.** The quality layer was built for Elia only:
  - `checks()` lists Elia models as inputs.
  - `freshness.sql` and `revision_lag.sql` read Elia views.
  - `RAW_INPUTS` lists Elia streams.
  - The `freshness` threshold is one number (72 h), and `revision_lag` warns at 0.8 × the smallest revision window across all streams (~11 days).

  ERA5 is ~6 days behind by design, and its preliminary-to-final corrections (if any) would arrive 60–90 days old through the deep re-check. Both checks would warn every day or month.

  Step 2 therefore makes both thresholds **per stream**: freshness from each source's expected delay, and `revision_lag` from each stream's deep window where it has one. It also makes the raw and quality wiring generic per source.
- **The combined energy and weather table will have empty weather columns for the last ~6 days.** That is expected, not a gap to fix.
- Credits are needed in the README (Open-Meteo, Copernicus ERA5) wherever the data is shown.

## Steps

1. Connector, recorded fixture, tests, this ADR; stream registered but paused (built).
2. Staging, a weather mart with the alignment above, per-stream freshness and `revision_lag` thresholds, and the offshore-wind alignment test.
3. Download the history (resume the stream), the combined energy and weather hourly table, and the console showing the new source.
