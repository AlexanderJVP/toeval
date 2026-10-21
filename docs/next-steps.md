# Next steps

The working backlog: where the last session stopped and what comes next, in order. Decisions go to `docs/decisions/`; this file only tracks work. Update it at the end of every session (last update: **2026-10-21**).

## Where we are

- Phase 1 (foundation + Elia) is done. Elia ingests daily; history since 2015.
- Phase 2, Open-Meteo (ADR 0008) is done. It ingests daily at 12:00; history since 2014-12-31.
- Phase 2, IRCEL-CELINE (ADR 0009): steps 1–2 done, step 3 in progress.
  - Built: the connector, `stg_irceline_hourly`, `mart_air_hourly` and 6 quality checks.
  - 2026-10-21: the owner downloaded the history from the console ("fetch now") and resumed the stream. 6.27M values since 2011; it ingests daily at 12:00. The stale `sources.toml` comment is removed.
  - The download was profiled; the findings are in ADR 0009, "History download". The transform now takes ~37 s.
- The station → weather point mapping was **not** decided before the download, but no harm was done: raw has no `near` column, so the recommended reference view is still open.
- 235 tests pass and CI is green.

## Next: IRCEL-CELINE step 3 (rest)

Confirm the plan with the owner before building, as with every step.

1. ~~**PM2.5 sentinel −9399.06**~~ (done 2026-10-22): staging drops values below −1000 µg/m³, guarded by the `no_data_codes` check (ADR 0009, "No data codes").
2. **Station → weather point mapping: deferred** (owner, 2026-10-22). The join below doesn't need it, and building it now would be code without a user. When a per-station weather analysis needs it: `transform.py` registers a small reference view (e.g. `ref_irceline_stations`: station, near) built from `STATIONS` (`connectors/irceline.py`). Not in raw (curation must not go into immutable raw).
3. ~~**Join air quality into `mart_energy_weather_hourly`**~~ (done 2026-10-22, ADR 0009 "Join"; the decision is kept here for context):
   - **One fixed reference station, 42R801 Borgerhout** (all four pollutants, since 2011), as columns like `air_antwerp_no2`, so Phase 3 reads one wide hourly table. Station detail stays in `mart_air_hourly`.
   - **Not** a mean of the stations present, nor a national median: the station set changes (42R804, a roadside station, starts in 2017; outages), which shifts the level for reasons unrelated to the air and creates exactly the spurious correlations Phase 3 is meant to expose. A count column would explain the jumps, not remove them.
   - Hours without a value stay NULL: the 2012-01 to 2012-05 hole, PM before 2012-05-31, the O3 outage since 2026-10-19. 42R801 has the lowest NO2 coverage of the 15 stations (87.6% of its span; O3 89.3%), so expect somewhat more NULLs.
   - Fallback if one station is too thin: the mean of a fixed set of stations, only when all have a value (the weather rule), only from 2017.
   - Test it like `tests/test_weather.py` (synthetic raw through `RawWriter`), with a mutation check.
4. **Console** (`src/toeval/ui/static/`):
   - `app.js:228` marts label ("energy · weather · energy + weather hourly") should mention air.
   - Check the stream card shows `irceline/hourly` with its 36 h freshness limit (should be generic).
5. **Docs:**
   - ADR 0009 status → done.
   - CLAUDE.md Status and the IRCEL facts section.
   - README status, marts list and roadmap.
   - This file.

## After that (Phase 2, owner's order)

Each one starts with questions and a plan, then its own ADR.

1. **Shared calendar mart:** daily/weekly grains aligned across sources (Brussels days, ISO weeks, holidays/school holidays?). It is the base for Phase 3's correlation scan.
2. **Statbel / Antwerp open data:** monthly files keyed by reference period (the `period` watermark shape; `tests/fakes.py` has a fake for it).
3. **iRail last:** live-only polling with ETags, gaps while the PC is off (accepted by the owner). The per-*source* rate limit deferred in ADR 0005 is needed here.

Telraam is set aside. Phases 3–5 (correlation tracker, JEPA + baselines, public report) must not be scaffolded early.

## Open threads

- **Revision windows are provisional** for all sources: Elia 14 d + 90 d deep, Open-Meteo 120 d deep, IRCEL 400 d deep. Revisit them once `revision_lag` has collected a few months of evidence.
- **IRCEL validation:** whether validated values replace provisional ones is undocumented. The first monthly 400-day deep re-check after the download will show it.
- **O3 outages (2026-10-21):** 42R801 and 44M705 O3 have been quiet at the source since 2026-10-19 (`stale_series`, `coverage` warn). If they are still quiet around 2026-11-18, `stale_series` lists them as quiet for over 30 days: then decide whether to drop or replace them in `STATIONS`.
- **Known limitation (ADR 0004):** rows a source withdraws stay in raw and staging.
