# 0006. Staging, marts and quality checks: full rebuilds in SQL, warnings not failures

- Status: accepted
- Date: 2026-10-19
- Amends: 0002 §3 (how `revision_lag` is measured)

## Context

Raw holds every fetched batch, so recent intervals exist in several versions (ADR 0002). Downstream consumers (the correlation tracker, JEPA) need one clean value per quarter-hour, aligned across datasets, and need to know when the data is incomplete. ADR 0001 requires SQL in `.sql` files that can be ported to another engine, and layers that can be rebuilt from raw.

Measured on the real data on 2026-10-19 (5.4 M raw rows): a full rebuild of all staging and mart models plus every quality check takes about 9 s and writes 136 MB.

## Decision

### Full rebuild, not incremental

`toeval transform` (and `run-due` / `ingest` after anything was written) rebuilds every model from raw (`src/toeval/warehouse/transform.py`). At 9 s, incremental merge logic would cost more complexity than it saves. Each model runs as `COPY (<sql>) TO <tmp>` in an in-memory DuckDB, downstream models read the temporary files, and only when every model and check succeeded are all files swapped in: `data/staging/stg_*.parquet`, `data/marts/mart_*.parquet`. A failure leaves all previous outputs in place, never fresh staging next to stale marts. Every attempt is recorded in the state DB's `transforms` table, and a failed one is retried with backoff (ADR 0005).

SQL files reference only view names: `raw_<source>_<stream>` (every file under `raw/<source>/<stream>/`, never quarantine), `stg_*` and `mart_*`. Paths are bound in Python. A model whose inputs were never ingested is skipped. `run-due` transforms only when raw changed, or when a buildable model has no output yet.

### Staging (`sql/staging/`)

One row per natural key: the **latest `_ingested_at`**, ties broken by file name. Columns are renamed to snake case with units (`load_mw`, `solar_forecast_day_ahead_mw`, ...), `datetime` becomes `interval_start_utc` (`TIMESTAMPTZ`), and all forecast columns are kept. Solar gets a `level` column (country / region / province).

### Marts (`sql/marts/`)

- `mart_energy_15min` sits on a **gap-free 15-minute grid** from the first to the last interval of any input, so missing data is a NULL row, never a missing row. It has load, solar (the `Belgium` row only) and wind (sum of the 5 parts, **NULL unless all 5 are present**), each measured and day-ahead forecast, plus `wind_parts_present` (ADR 0003).
- `mart_energy_hourly` uses UTC hours; a value is the mean of the four quarter-hours and is NULL unless all four are present. `*_quarters` columns give the counts.
- Keys are UTC. `*_local` columns are Brussels wall-clock time for grouping by local day (23, 24 or 25 hours). They repeat in the October fall-back hour, so they are never keys.
- No imputation: a NULL stays NULL. Consumers decide (e.g. night-time solar could reasonably be 0).

### Quality checks (`sql/quality/`): warn, store, never block

Hard checks already ran at ingestion (ADR 0004), and what remains is what the source published. So these checks produce **warnings**: they're logged, shown by `toeval status`, and stored in `quality_results` in the state DB. They never fail the transform. Each SQL file returns `subject, value, details`; the threshold lives in Python (`Check.max_ok`).

| Check | Value | Warns when |
|---|---|---|
| `duplicate_keys` | duplicate natural keys in staging | > 0 |
| `grid_gaps` | quarter-hours missing from staging between a stream's first and last interval | > 0 |
| `measured_holes` | NULL measured values in the **last 30 days** of each national series' measured range | > 0 |
| `wind_parts` | quarter-hours with 1–4 or more than 5 wind parts | > 0 |
| `freshness` | hours since the last measured quarter-hour | > 72 |
| `revision_lag` | age in days of the oldest value Elia changed in the **last 30 days**, when it changed | > 80% of the revision window |

`measured_holes` and `revision_lag` only warn on recent events. Old ones are fixed history; reporting them forever would teach everyone to ignore warnings. They are counted in the details, and the known holes are documented below.

### `revision_lag` comes from raw (amends 0002 §3)

0002 planned to compare each run's re-fetched rows against staging. Raw already holds every version, so the check compares **successive raw versions of the same key** instead. A revision is a measured value that differs from the previous version, which already had one; a forecast row getting its first measurement doesn't count. This needs no per-run state, and it can be recomputed over all history at any time. Its blind spot: revisions older than the revision window are only seen when the monthly 90-day deep re-fetch picks them up.

## Known source issues (found by these checks on 2026-10-19)

- **Solar (ods032):** the last quarter-hour of each Brussels day (23:45 local) is NULL in all 14 regions on 638 days, from 2022-11-16 to 2024-10-15. It is always at night, with 0.0 on both sides.
- **Load (ods001):** NULL for 2021-10-31 00:00–00:45 UTC (the first of the two 02:00 hours at the DST change), and for single quarter-hours on 2023-08-10, 2023-08-11 and 2023-09-11 at 09:00 UTC.
- **Wind (ods031):** NULL for 2015-03-29 01:00–01:45 UTC (the hour after the spring-forward jump) and 2021-10-31 00:00–00:45 UTC.
- There are no missing rows (`grid_gaps` = 0) and no duplicates; every wind quarter-hour has all 5 parts.

## Consequences

- Downstream code reads one Parquet file per model and never sees raw versions or the Elia hierarchy.
- On Windows, a process that keeps a staging or mart file open (e.g. an interactive DuckDB session holding it) makes the swap fail. The transform then fails without changing any output, is recorded and shown by `status`, and is retried with backoff. Open the files only for the duration of a query.
- Rebuild time grows with raw. At about 1 GB of raw (years more data, or Phase 2 sources) it's worth measuring again before considering incremental models.
