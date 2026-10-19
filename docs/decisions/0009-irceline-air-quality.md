# 0009. IRCEL-CELINE air quality: NO2, PM10, PM2.5 and O3 at 15 stations

- Status: accepted (step 1 of 3 built: connector and raw data; the stream starts paused)
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

### Rollout

1. Write `[irceline.streams.hourly] paused = true` in the owner's `sources.toml`.
2. Register the connector.

This order is the safe one. A tick between the two steps fails with "unknown source" and writes nothing. The opposite order could start the 15-minute history download before staging exists. The history download happens in step 3, as an explicit `ingest` while the stream is still paused, as with Open-Meteo.

## Consequences

- Attribution: credit IRCEL-CELINE (CC BY 4.0) in the README and wherever the data is shown.
- A station that stops measuring makes `freshness` warn if the check uses the stalest station. Step 2 decides whether freshness follows the freshest station (the source is delivering) and adds a separate per-station staleness check.
- `STATIONS.near` maps each station to an Open-Meteo point, for joins in step 3.

## Steps

1. Connector, recorded fixtures, tests, this ADR; stream registered but paused (built).
2. Staging, `mart_air_hourly` with the −1 h shift, per-station quality checks, and a soft value-range check.
3. Download the history (resume the stream), join air quality into the combined hourly table, and the console.
