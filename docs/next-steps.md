# Next steps

The working backlog: where the last session stopped and what comes next, in order. Decisions go to `docs/decisions/`; this file only tracks work. Update it at the end of every session (last update: **2026-10-22**).

## Where we are

- Phase 1 (foundation + Elia) is done. Elia ingests daily; history since 2015.
- Phase 2, Open-Meteo (ADR 0008) is done. It ingests daily at 12:00; history since 2014-12-31.
- Phase 2, IRCEL-CELINE (ADR 0009) is done (2026-10-22). It ingests daily at 12:00; history since 2011.
  - `stg_irceline_hourly`, `mart_air_hourly` and 7 quality checks.
  - Staging drops "no data" codes below −1000 µg/m³ (IRCEL's −9399.06 in 2021), guarded by `no_data_codes`.
  - `mart_energy_weather_hourly` has `air_antwerp_*_ugm3` from one fixed reference station, 42R801 Borgerhout (the owner's decision and its reasons: ADR 0009, "Join").
  - The console's marts label mentions air; the IRCEL stream card was checked (36 h freshness limit).
- 254 tests pass and CI is green. The transform takes ~25–37 s.

## Next: the shared calendar mart (ADR 0010, approved 2026-10-22)

The plan, the 90% day rule's bias calibration and the week rule are in ADR 0010.

1. ~~**Step 1, `mart_calendar_day`**~~ (done 2026-10-22; every school year 2014–15 to 2029–30 verified) (from 2015-01-01): ISO week, weekday, weekend, `day_hours` (23/24/25), the 10 public holidays computed in Python (Easter algorithm) and tested against published dates, Flemish school holidays from a hand-checked CSV (school years 2014–15 onwards, each year verified on onderwijs.vlaanderen.be or listed as unverified in the ADR), and the `calendar/school_holidays` horizon check (warns < 180 days ahead).
2. ~~**Step 2, `mart_daily` and `mart_weekly`**~~ (done 2026-10-22) from `mart_energy_weather_hourly`: means need ≥ 90% of the day's real hours, precipitation sums need all; weeks need all 7 days. `*_hours` / `*_days` counts for the main series. Tests on synthetic raw (23/25-hour days, the 90% boundary, a missing Saturday) with mutation checks.
3. **Step 3:** docs and the console's marts label.

## After that (Phase 2, owner's order)

Each one starts with questions and a plan, then its own ADR.

1. **Statbel / Antwerp open data:** monthly files keyed by reference period (the `period` watermark shape; `tests/fakes.py` has a fake for it).
2. **iRail last:** live-only polling with ETags, gaps while the PC is off (accepted by the owner). The per-*source* rate limit deferred in ADR 0005 is needed here.

Telraam is set aside. Phases 3–5 (correlation tracker, JEPA + baselines, public report) must not be scaffolded early.

## Open threads

- **O3 outages (since 2026-10-19):** 42R801 and 44M705 O3 are quiet at the source (`stale_series`, `coverage` warn). 42R801 is the reference station, so `air_antwerp_o3_ugm3` is NULL until it returns. If it is still quiet around 2026-11-18 (`stale_series` then lists it as quiet for over 30 days), decide with the owner: wait, or the fallback in ADR 0009 "Join" (a fixed set of stations, NULL unless all have a value, from 2017).
- **Station → weather point mapping: deferred** (owner, 2026-10-22): no user yet. When a per-station weather analysis needs it, `transform.py` registers a small reference view (e.g. `ref_irceline_stations`: station, near) built from `STATIONS`. Never a raw column.
- **Revision windows are provisional** for all sources: Elia 14 d + 90 d deep, Open-Meteo 120 d deep, IRCEL 400 d deep. Revisit them once `revision_lag` has collected a few months of evidence.
- **IRCEL validation:** whether validated values replace provisional ones is undocumented. The first monthly 400-day deep re-check after the download (around 2026-11-20) will show it in `revision_lag`.
- **Known limitation (ADR 0004):** rows a source withdraws stay in raw and staging.
