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
- Phase 2, shared calendar (ADR 0010) is done (2026-10-22).
  - `mart_calendar_day`: Brussels days 2015-01-01 to 2030-08-31, ISO weeks, 23/25-hour DST days, the 10 public holidays (computed) and the Flemish school holidays (CSV, every school year 2014–15 to 2029–30 verified on official sources).
  - `mart_daily` (a mean needs ≥ 90% of the day's real hours; rain sums need every hour) and `mart_weekly` (from days; all 7 needed). The 90% rule's bias was measured on the real data (ADR 0010).
  - The console's marts label lists them.
- 255 tests pass and CI is green. The transform takes ~35–40 s.

## Next: Statbel / Antwerp open data

Start with questions to the owner and a plan, then an ADR (0011), as with every step.

- Monthly (or yearly) files keyed by reference period: the `period` watermark shape, which no real connector uses yet (`tests/fakes.py` has a fake for it).
- Which datasets? They must carry a time dimension that lines up with the daily/weekly marts or needs a monthly grain (ADR 0010 left months until Statbel needs them).
- Verify endpoints, licences and update schedules at the source, not from memory.

## After that (Phase 2, owner's order)

1. **iRail last:** live-only polling with ETags, gaps while the PC is off (accepted by the owner). The per-*source* rate limit deferred in ADR 0005 is needed here.

Telraam is set aside. Phases 3–5 (correlation tracker, JEPA + baselines, public report) must not be scaffolded early.

## Open threads

- **O3 outages (since 2026-10-19):** 42R801 and 44M705 O3 are quiet at the source (`stale_series`, `coverage` warn). 42R801 is the reference station, so `air_antwerp_o3_ugm3` is NULL until it returns. If it is still quiet around 2026-11-18 (`stale_series` then lists it as quiet for over 30 days), decide with the owner: wait, or the fallback in ADR 0009 "Join" (a fixed set of stations, NULL unless all have a value, from 2017).
- **Station → weather point mapping: deferred** (owner, 2026-10-22): no user yet. When a per-station weather analysis needs it, `transform.py` registers a small reference view (e.g. `ref_irceline_stations`: station, near) built from `STATIONS`. Never a raw column.
- **Revision windows are provisional** for all sources: Elia 14 d + 90 d deep, Open-Meteo 120 d deep, IRCEL 400 d deep. Revisit them once `revision_lag` has collected a few months of evidence.
- **IRCEL validation:** whether validated values replace provisional ones is undocumented. The first monthly 400-day deep re-check after the download (around 2026-11-20) will show it in `revision_lag`.
- **Known limitation (ADR 0004):** rows a source withdraws stay in raw and staging.
- **School-holiday table:** add each new school year when the Flemish government publishes it (6 rows in `warehouse/reference/school_holidays_flanders.csv`, verified on the official page). The table now ends 2030-08-31; `calendar/school_holidays` warns from 2030-03-05 (fewer than 180 days left).
- **For Phase 3 (ADR 0010, Consequences):** COVID lockdowns (2020–2021) are not in the calendar, so the tracker will find "2020" itself as a strong correlation; raise it with the owner then. NULL weeks cluster during instrument outages: treat them as gaps, never draw across them.
