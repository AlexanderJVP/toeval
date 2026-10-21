# 0009. IRCEL-CELINE air quality: NO2, PM10, PM2.5 and O3 at 15 stations

- Status: accepted (steps 1 and 2 of 3 built: connector, staging, `mart_air_hourly`, quality checks; step 3 in progress: history downloaded and the stream resumed on 2026-10-21, no-data codes and the join built on 2026-10-22, the console follows)
- Date: 2026-10-20

## Context

The next Phase 2 source, after Open-Meteo (ADR 0008). Air quality is driven by traffic, industry, heating and the weather, so it lines up with the energy and weather tables in time. It also adds many plausible-looking series for the correlation tracker. The owner chose a curated set of about 15 stations, the four core pollutants, and a daily run at 12:00.

## Facts (verified 2026-10-20)

From IRCEL's open data page, its `irceline/open_data` README on GitHub, the API documentation and live requests:

- **API:** the 52°North timeseries REST API at `https://geo.irceline.be/sos/api/v1/`, no key. Licence **CC BY 4.0** ("All data on this website is published via open data endpoints under the Creative Commons Attribution 4.0 license"). No rate limit is documented.
- **Series:** a *timeseries* is one pollutant at one station. `/timeseries?phenomenon=<id>&expanded=true` lists them, with the station label, coordinates, unit (`µg/m³`) and first and last value. Today every station has **exactly one series per pollutant**, and no series overlap in time.
- **Data:** `/timeseries/<id>/getData?timespan=<start>/<end>` returns `{"values": [{"timestamp": <epoch ms, UTC>, "value": <number|null>}]}`.
  - **Both ends are included.**
  - A timespan must be **shorter than 367 days**: P366D works, P370D gives HTTP 400 "please use a period shorter than 'P367D'".
  - Ranges before the history or in the future return an empty list, not an error.
  - No ETag or Last-Modified headers.
- **Timestamps mark the end of the hour.** IRCEL's README: "hmean: hourly mean (timestamp = end-time of hour)". The data agrees: 48 of the 56 NO2 series that start in 2011 start at exactly 2011-01-01 **01:00** UTC. The realtime WFS layer (`realtime:no2_hmean_station`) shows the same timestamps and values.
- **Missing hours** come back as `null` (about 4% of 3 days in the fixture). The newest values are under ~3 hours old: at 23:21 UTC the latest was 22:00.
- **History:** NO2 and O3 since 2011, PM10 and PM2.5 mostly since 2012 (at about half the stations). Nationally there are 92 NO2, 96 PM10, 86 PM2.5 and 38 O3 series.
- **Validation is not documented.** The regional agencies validate hourly data for EU reporting, normally the following year. Nothing says whether this API then replaces the provisional values.

## Decision

### Source and stream

Source `irceline`, one stream `hourly` (`src/toeval/connectors/irceline.py`). Natural key `(station, pollutant, time)`. Raw keeps the series id, station label and coordinates per row, so it describes itself.

15 stations (`STATIONS`), 54 series:

| Near (Open-Meteo point) | Stations |
|---|---|
| antwerp | 42R801 Borgerhout (all four), 42R802 Borgerhout, 42M802 Antwerpen, 42R804 Antwerpen Ring (since 2017), 40AL01 Linkeroever (PM only) |
| brussels | 41R001 Molenbeek-Saint-Jean, 41R012 Uccle, 43N073 Vezin (Namur) |
| ghent | 44R701 Gent, 44R740 Sint-Kruiswinkel (PM2.5 since 2023) |
| liege | 43R222 Liège ISSeP, 43R240 Engis |
| ostend | 44N029 Houtem, 44M705 Roeselare |
| libramont | 43N121 Offagne |

Chosen for long history, all four pollutants where measured, and closeness to the weather points. No station near Ostend has a long NO2 record, so the west is covered by Houtem (near De Panne) and Roeselare. The official station types (traffic, background, industrial) weren't checked, so the code describes stations by place only. 42R801 and 42R802 have identical coordinates.

**All Belgian stations** (~310 series, ~33M rows, ~1.3 GB of JSON) was rejected for now: about 5× Elia's staging, an hour or more of downloading from an API with undocumented limits, and a slower rebuild. Adding stations later is a change to `STATIONS` plus a backfill.

### Requests

- The series are **looked up on every run**: 4 listing GETs, about 0.5 MB in total. A replaced instrument would start a new series id, and the lookup picks it up without a code change. A curated station without any series is logged as a warning.
- One `FetchRequest` per time window, with one GET per series inside `fetch`. So there is **one raw file per window per run**, not one per series (54 a day would bloat raw). A series is skipped for windows outside its first and last value.
- `time_window_plan` with 365-day chunks from `backfill_start` = 2011-01-01 00:00 UTC, so every timespan stays under the 367-day limit. The last window ends on the chunk grid, possibly in the future, which the API answers with nothing.
- `fetch` keeps values with `start <= time < end`, because the API includes both ends. **Null hours are dropped**, so the watermark is the latest hour with a value.
- `HttpPolicy(requests_per_second=2)`, polite because limits are undocumented. The live test fetched 54 series in 30 s. The history is about 16 windows × 54 ≈ 860 GETs, roughly 10–15 minutes. A daily run is 4 + 54 GETs.
- Hard checks: units (`µg/m³`, otherwise the run fails), timestamps on the hour, and the generic key checks. Value ranges are not a hard check: one sensor spike must not quarantine a whole window. A soft check follows in step 2.

### Schedule and revisions

- Daily at 12:00 Brussels time, like Elia and the weather.
- `revision_window` 14 days. `deep_refetch` every 30 days over **400 days** (provisional), to catch values replaced after the yearly validation if that happens. It costs 2 windows × 54 GETs a month. `revision_lag` will show whether revisions happen and how old the revised values are.
- Quality limits: `freshness_limit` **36 h** (published within ~3 h and fetched daily, so ~27 h old just before a run) and `revision_lag_limit` **320 days** (80% of the deep window).

### Time alignment (applied in the marts, step 2)

The value at `time` covers `[time − 1 h, time)`, so `hour_start_utc = time − 1 h`. This is the same shift as Open-Meteo's precipitation and radiation. The other alignment rules (snapshots, wind direction) don't apply.

### Staging, mart and quality checks (step 2)

- `stg_irceline_hourly`: the latest ingested version per `(station, pollutant, time)`. The API timestamp is renamed `hour_end_utc`, so nobody mistakes it for an hour start; the value is `value_ugm3`, as published.
- `mart_air_hourly`: one row per station and UTC hour, `hour_start_utc = hour_end_utc − 1 h`, with the four pollutants as columns (`no2_ugm3`, `pm10_ugm3`, `pm25_ugm3`, `o3_ugm3`). The grid is gap-free per station from its first to its last hour, so a missing hour is a NULL row. A pollutant the station doesn't measure is NULL throughout. Label and coordinates are the latest published. No imputation, and out-of-range values stay.
- Wide rather than long, like `mart_weather_hourly`: one row per place and hour is what the joins and the correlation scan read.

The thresholds were calibrated on 90 live days (2026-07-22 to 2026-10-20, 114,393 values, run through the real transform):

| Check | Value | Limit | Why |
|---|---|---|---|
| `duplicate_keys` | duplicate staging rows | 0 | as for the other sources |
| `freshness` | hours since the newest value of the **freshest** series | 36 h | "is the source delivering?" One quiet instrument must not look like a dead source. Live: 2.5 h. |
| `stale_series` | series whose last value is over 48 h older than the newest value of any series, among those that went quiet in the last 30 days | 0 | a stopped instrument. Measured against the newest value, not now, so a dead source raises only `freshness`. A day of maintenance is normal: 42R801's NO2 and O3 were 25 h quiet on 2026-10-20. Series quiet for over 30 days (retired) are listed in the details only. |
| `coverage` | series with values for fewer than 75% of the hours in the 30 days before the newest value | 0 | intermittent instruments. Live: median ~100%, ~97% over 90 days; 44M705 O3 was the only one below, at 74%. |
| `value_range` | values below −5 or above 1000 µg/m³ in those 30 days | 0 | broken sensors or sentinels only. Live: PM2.5 down to −0.33 (noise near zero; 11 negatives in 90 days) and PM10 up to 473 (44R740, Ghent port area) are real. Soft: the values stay in staging and the marts. |
| `revision_lag` | age in days of the oldest hour revised in the last 30 days | 320 d | the evidence for the 400-day re-check |
| `no_data_codes` | values below −1000 left in staging or the mart, whole history (added 2026-10-22) | 0 | staging drops them; this guards that rule (see "No data codes") |

The live data also shows PM2.5 values quantised in steps of about 0.43 µg/m³ at several stations, and many exact zeros (1,485 in 90 days). Both are as published; nothing in the warehouse depends on them.

### Rollout

1. Write `[irceline.streams.hourly] paused = true` in the owner's `sources.toml`.
2. Register the connector.

This order is the safe one. A tick between the two steps fails with "unknown source" and writes nothing. The opposite order could start the 15-minute history download before staging exists. The history download happens in step 3, as an explicit `ingest` while the stream is still paused, as with Open-Meteo.

### History download (2026-10-21)

The owner ran the download from the console ("fetch now", an explicit `ingest` of the paused stream) and then resumed the stream. One run: 804 GETs plus 4 listings, 6,267,787 values, 16 raw files, ~10 minutes. `mart_air_hourly` has 2,000,763 station-hours (15 stations, 2011-01-01 to now). The transform now takes ~37 s (it was ~18–21 s on Elia and weather). Every station still has one series id per pollutant.

| Pollutant | Values | First hour (end) |
|---|---|---|
| NO2 | 1,726,643 | 2011-01-01 01:00 UTC |
| O3 | 1,256,478 | 2011-01-01 01:00 UTC |
| PM10 | 1,693,687 | 2012-05-31 01:00 UTC |
| PM2.5 | 1,590,979 | 2012-05-31 01:00 UTC |

Start dates as expected: 42R804 from 2017-10-31, 44R740 PM2.5 from 2023-12-15. Some PM series start later than 2012-05-31 (40AL01 PM2.5 2015-01-29, 42M802 and 44M705 PM2.5 2014-11-03).

Findings, all confirmed against the live API (not artefacts of our 365-day chunks):

- **A national hole from 2012-01-01 to 2012-05-31.** Every NO2 and O3 series has no values in that period (the API returns an empty list for, e.g., 42R801 NO2 in February 2012, and values up to 2011-12-31 23:00). PM10 and PM2.5 start when it ends, so 2011 holds NO2 and O3 only.
- **Instrument gaps over 30 days:** 40AL01 PM10 2013-06 to 2016-01 and 40AL01 PM10 and PM2.5 2024-01 to 2025-11; 44R740 PM10 2022-02 to 2023-12; 44M705 PM10 and PM2.5 2022-02 to 2023-11; 42M802 PM10 2015-01 to 2016-01; 44M705 all pollutants 2012-07 to 2012-08; 41R001 PM10 and PM2.5 for 1–2 months in 2014–2015. They are NULL rows in the mart.
- **A sentinel value: PM2.5 = −9399.06 µg/m³**, 1,893 hours at 14 stations between 2021-01-04 and 2022-01-06 (876 of them at 41R012). The API publishes it as a value. It is probably a −9999 "no data" code multiplied by a PM2.5 correction factor of 0.94. Any mean or correlation over 2021 would be wrecked by it, and `value_range` counted it only under "all history", because it checks the last 30 days. Handled on 2026-10-22, see "No data codes" below.
- **Two PM10 spikes** at 40AL01 on 2013-03-19 (2,432 and 2,946.5 µg/m³, 10:00 and 11:00): physically implausible as hourly means, but single hours. They stay, like other out-of-range values.
- **Negatives down to −10 µg/m³** (184 values, mostly PM2.5 at 41R001 and 41R012 in 2015–2019). IRCEL's own colour scale for PM2.5 starts at −10, so these are measurement noise near zero, as published.
- On 2026-10-21 two O3 analysers were offline at the source (42R801 since 2026-10-19 05:00, 44M705 since 12:00; the stations' other pollutants are current), and `stale_series` and `coverage` warned as designed.

### No data codes (2026-10-22)

The owner chose a range rule over matching −9399.06 exactly: **any value below −1000 µg/m³ is "no data"**, for every pollutant. No air pollutant can be that negative, so dropping it isn't imputation, and a rule on the range also catches a future raw −9999 or another corrected code.

- `stg_irceline_hourly` drops such hours after picking the latest version, as `fetch` drops null hours. A later real value replaces a code as usual, and a later code hides an older real value. The mart's grid shows the hour as NULL. Raw keeps the published value.
- Dropped rather than kept as a NULL row in staging, because `coverage`, `freshness` and `stale_series` count staging rows: a no-data hour must not count as measured.
- `value_range` keeps its −5 lower bound for warnings; small negatives down to −10 stay.
- A new check, `no_data_codes`, counts values below −1000 left in staging or `mart_air_hourly` over the **whole history** (limit 0), and reports how many raw values the rule drops. On the real data: 6,267,787 → 6,265,894 staging rows (1,893 dropped), the mart's lowest PM2.5 is now −10, and `value_range`'s history count went from 2,079 to 186.

### Join into the combined hourly table (2026-10-22)

`mart_energy_weather_hourly` gains `air_antwerp_no2_ugm3`, `air_antwerp_pm10_ugm3`, `air_antwerp_pm25_ugm3` and `air_antwerp_o3_ugm3`, taken from **one fixed reference station, 42R801 Borgerhout** (Antwerp, all four pollutants since 2011). An hour without its value stays NULL. Station detail stays in `mart_air_hourly`.

The owner rejected the earlier proposal, a mean of the Antwerp stations present plus a station count, and a national median for the same reason. The station set changes: 42R804 is a roadside station on the Ring that starts in 2017, and stations have outages. A mean over whichever stations report shifts the level when one joins or drops out, for reasons that have nothing to do with the air. Phase 3 is a correlation tracker, and such steps produce exactly the spurious correlations it is meant to expose; a count column would explain the steps, not remove them. The weather rule (a mean over a fixed set, NULL unless all have a value) would work, but 42R804 would limit it to 2017 onwards.

The price is a single station's outages: 42R801 has the lowest NO2 coverage of the 15 stations (87.6% of its span; O3 89.3%), and its O3 has been offline since 2026-10-19. Over the combined table's hours (2015 onwards) it fills NO2 90%, PM10 98%, PM2.5 99% and O3 92%. If that proves too thin: a fixed set of stations, NULL unless all have a value, from 2017.

The combined table now needs `mart_air_hourly` to be built (every model input is required). The SQL hard-codes `42R801`; `tests/test_air.py` asserts it stays in `STATIONS`.

The station → weather point mapping (`STATIONS.near`) is not used by the join and stays in Python until an analysis needs it; it then becomes a reference view built in the transform, never a raw column.

## Consequences

- Attribution: credit IRCEL-CELINE (CC BY 4.0) in the README and wherever the data is shown.
- `freshness` follows the freshest series; a single station that stops measuring is `stale_series`' job (step 2).
- `STATIONS.near` maps each station to an Open-Meteo point. The step 3 join doesn't use it (see "Join").

## Steps

1. Connector, recorded fixtures, tests, this ADR; stream registered but paused (built).
2. Staging, `mart_air_hourly` with the −1 h shift, per-series quality checks, and a soft value-range check (built).
3. Download the history (resume the stream) (done 2026-10-21), join air quality into the combined hourly table, and the console.
