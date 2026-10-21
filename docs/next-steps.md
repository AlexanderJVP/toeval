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

1. **Decide how to handle the PM2.5 sentinel −9399.06** (1,893 hours, 14 stations, 2021-01-04 to 2022-01-06; ADR 0009). It is in staging and `mart_air_hourly` now, and would wreck any 2021 mean or correlation.
   - Options to put to the owner: NULL it in staging (raw keeps it; a named rule, not imputation), NULL it only in the mart, or a general rule (e.g. any value below −1000 is "no data").
   - Then a quality check or test that the mart has none, and a mutation check.
2. **Station → weather point mapping:** `STATIONS.near` is in Python (`connectors/irceline.py`), and SQL can't read it.
   - Recommended: `transform.py` registers a small reference view (e.g. `ref_irceline_stations`: station, near) built from `STATIONS`. Not in raw (curation must not go into immutable raw).
3. **Join air quality into the combined hourly table** (`mart_energy_weather_hourly`, or a new mart; ask the owner).
   - The station set changes over time (42R804 starts in 2017), so the Open-Meteo rule "NULL unless all points" doesn't carry over.
   - Options to put to the owner: an Antwerp mean of the stations present, plus a count column; per-point columns; or the national median.
   - Antwerp O3 rests on 42R801 alone (42R802, 42M802, 42R804 and 40AL01 don't measure O3), and that analyser was offline from 2026-10-19.
   - The 2012-01 to 2012-05 hole (NO2/O3, all stations) and 2011 without PM must show as NULLs, not as a smaller mean.
   - Test it like `tests/test_weather.py` (synthetic raw through `RawWriter`).
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
