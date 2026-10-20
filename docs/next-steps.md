# Next steps

The working backlog: where the last session stopped and what comes next, in order. Decisions go to `docs/decisions/`; this file only tracks work. Update it at the end of every session (last update: **2026-10-20**, after commit 05ada72).

## Where we are

- Phase 1 (foundation + Elia) is done. Elia ingests daily; history since 2015.
- Phase 2, Open-Meteo (ADR 0008) is done. It ingests daily at 12:00; history since 2014-12-31.
- Phase 2, IRCEL-CELINE (ADR 0009): steps 1–2 of 3 are done.
  - Built: the connector, `stg_irceline_hourly`, `mart_air_hourly` and 6 quality checks.
  - The stream `irceline/hourly` is **paused** in the local `sources.toml`, and **no IRCEL data is in raw yet**.
- 235 tests pass and CI is green.

## Next: IRCEL-CELINE step 3

Confirm the plan with the owner before building, as with every step.

1. **Decide the station → weather point mapping first (before the download).**
   - `STATIONS.near` is in Python (`connectors/irceline.py`), and SQL can't read it.
   - Recommended: `transform.py` registers a small reference view (e.g. `ref_irceline_stations`: station, near) built from `STATIONS`.
   - Alternative: add `near` to the raw schema. That bakes a curation choice into immutable raw, so it isn't recommended. If it is chosen anyway, it must happen **before** the history download.
2. **Download the history** with `uv run toeval ingest irceline` (an explicit ingest runs although the stream is paused).
   - About 16 windows × 54 series ≈ 860 GETs, 10–15 min at 2 req/s, plus 4 listing GETs.
   - Expect a few million rows. The transform runs automatically afterwards.
   - Check:
     - row counts per pollutant;
     - `toeval status` quality results;
     - `mart_air_hourly` start dates (NO2/O3 2011, PM mostly 2012, 42R804 2017, 44R740 PM2.5 2023);
     - the transform time. CLAUDE.md says "~11 s on Elia and weather"; update it.
   - Historical holes and odd values found now belong in ADR 0009, as Elia's did in ADR 0006.
3. **Resume:** `uv run toeval resume irceline hourly`, then delete the now-stale comment line in `sources.toml` (`resume` doesn't remove comments). `due()` should then list it at 12:00.
4. **Join air quality into the combined hourly table** (`mart_energy_weather_hourly`, or a new mart; ask the owner).
   - The station set changes over time (42R804 starts in 2017), so the Open-Meteo rule "NULL unless all points" doesn't carry over.
   - Options to put to the owner: an Antwerp mean of the stations present, plus a count column; per-point columns; or the national median.
   - Test it like `tests/test_weather.py` (synthetic raw through `RawWriter`).
5. **Console** (`src/toeval/ui/static/`):
   - `app.js:228` marts label ("energy · weather · energy + weather hourly") should mention air.
   - Check the stream card shows `irceline/hourly` with its 36 h freshness limit (should be generic).
6. **Docs:**
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
- **Stale reports:** the `docs/repo-analysis-*.md` files (local, gitignored) list findings that are fixed now. Delete them, or run a fresh analysis, so an agent doesn't act on them.
- **Known limitation (ADR 0004):** rows a source withdraws stay in raw and staging.
