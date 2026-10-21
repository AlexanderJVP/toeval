# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Project **toeval** (Dutch for "coincidence"). Python package `toeval`, CLI `toeval`. Phase 1 (foundation + Elia) is complete (2026-10-19): Elia ingests daily via Task Scheduler and the full history since 2015 is loaded. Phase 2 is planned with the owner (2026-10-19): Open-Meteo first (ADR 0008, done 2026-10-20: connector, staging, `mart_weather_hourly`, `mart_energy_weather_hourly`, per-stream quality limits; history since 2014-12-31 loaded, ingests daily at 12:00), then IRCEL-CELINE (ADR 0009, done 2026-10-22: connector, `stg_irceline_hourly`, `mart_air_hourly`, quality checks, 42R801 joined into `mart_energy_weather_hourly`; history since 2011 loaded, ingests daily at 12:00), then a shared calendar mart, Statbel/Antwerp, and iRail last (live only, gaps while the PC is off; the owner wants to try it anyway). Telraam is set aside for now. The original brief is `claude-code-kickoff-prompt.md` (local only, gitignored, like the `docs/repo-analysis-*.md` reports). **Keep this file updated as the project evolves.** Only document commands that exist and work.

**Start here when resuming:** read `docs/next-steps.md`, which has where the last session stopped, the detailed next step (the shared calendar mart: questions and a plan first) and open threads. Update it at the end of each session. Before building anything, confirm the plan for the step with the owner.

## Commands

uv lives in `~/.local/bin`. If `uv` isn't found, prepend it to PATH.

```powershell
uv sync                          # install deps (incl. dev group)
uv run pytest                    # tests; `live`-marked tests (real APIs) are deselected by default
uv run pytest -m live            # only live API tests (hit the real Elia API)
uv run pytest tests/test_cli.py::test_version_command_prints_version   # single test
uv run ruff check . ; uv run ruff format .
uv run pre-commit run --all-files
uv run toeval --help

# Operating the pipeline (exit codes: 0 ok, 1 a stream failed, 2 usage error, 3 lock held)
uv run toeval ingest elia [load wind solar]     # incremental now, ignoring schedule and backoff
uv run toeval backfill elia wind --from 2025-01-01 --to 2026-01-01   # naive times = Brussels
uv run toeval run-due                           # what the scheduled task runs every 5 min
uv run toeval transform                         # rebuild staging + marts from raw, run quality checks
uv run toeval status                            # streams + the last transform's quality warnings
uv run toeval pause elia wind                   # scheduled runs skip the stream (sources.toml); `resume` undoes it
uv run python -m toeval ui                      # local admin console on http://127.0.0.1:8765
.\scripts\scheduled-task.ps1 [-Uninstall]       # install/remove the Task Scheduler entry
```

`ingest`, `backfill` and `run-due` transform automatically when raw changed. Logs: `data/logs/toeval.log` (rotating). Optional overrides: `sources.toml` (see the connector section).

GitHub remote: `AlexanderJVP/toeval` (private until Phase 1 is presentable).

## Project vision

A personal portfolio data platform (owner: applied informatics / AI-ML student in Antwerp, targeting data engineering roles). It should look and run like a production system, not a notebook. It continuously ingests live Belgian / Antwerp public data into one local warehouse that will be reused by future projects. Two applications sit on top:

1. **Spurious correlation tracker**: automatically finds absurd cross-dataset correlations and tracks week by week how they break down on new data. Playful tone, rigorous statistics (multiple-testing correction, detrending, out-of-sample tracking). Planned framing: "a naive scan finds X, rigorous methods keep Y, and here is how both decay".
2. **JEPA on unlabeled time series**: self-supervised pretraining that masks time spans and predicts their *latent* representation, then fine-tunes small heads on few-label tasks. It **must be benchmarked against simple baselines trained on the same few labels**. The corpus is small, so "baselines win" is a legitimate result that must be reported honestly.

## Phases

1. **Foundation + Elia** (done): tooling, connector interface, Elia connector (backfill + incremental), raw → staging → one mart, quality checks, tests, Task Scheduler job, README.
2. More sources, in this order (owner, 2026-10-19): Open-Meteo (done), IRCEL-CELINE (done), a shared calendar mart (aligned daily/weekly grains), Statbel/Antwerp, iRail last. Telraam is set aside for now.
3. Correlation tracker (weekly job, static report or small dashboard).
4. JEPA + baselines, results in README.
5. Public dashboard/report.

Do not scaffold folders or code for later phases ahead of time.

## Architecture decisions (details in `docs/decisions/`)

- **0001:** DuckDB compute over Parquet storage under `TOEVAL_DATA_DIR` (default `./data`, gitignored). Layers: `raw` (immutable, one file per fetch, Hive-style partitions) → `staging` (typed, UTC, latest version per natural key, rebuildable) → `marts` (15 min / hourly / daily / weekly). SQL lives in `.sql` files so it can be ported to Spark or a cloud warehouse. Single writer, protected by a lock file.
- **0002 (amended by 0004):** Incremental fetch range = `watermark − revision_window` (floored to a day grid) → `now + forecast horizon`, so missed runs (laptop off) catch up automatically and still re-check the window before the old watermark. The window is 14 days by default plus a monthly 90-day deep re-fetch. Both are provisional: the `revision_lag` quality check (ADR 0006) measures how old revised rows are, which is the evidence for tuning them. The watermark is the latest *measured* (non-null) timestamp and advances only after the raw writes and quality checks of **all** requests succeed. It does not wait for staging, which is a separate step.
- **0003:** Elia hierarchy. Solar (`ods032`) has a `region='Belgium'` total plus regions and provinces, so **never sum all rows**. Wind (`ods031`) has 5 disjoint parts with no total, so sum them and track `parts_present`. Load (`ods001`) is one national series.
- **0004 (connector interface):** see the next section. Empty batches aren't written, so rows a source *withdraws* stay in raw and staging (known limitation).
- **0005 (scheduled operation):** one Task Scheduler entry runs `toeval run-due` every 5 min via `.venv\Scripts\pythonw.exe -m toeval` (no console window). A stream is due when its schedule says so **and** the retry backoff after failed incremental runs has passed (5 min doubling to 6 h; `consecutive_failures` / `last_failure_at` in `stream_state`). Backfills don't touch `last_success_at` or the failure count. The lock holder's PID is checked, so a killed run's lock is taken over at once, and its `running` rows are marked failed (`StateStore.abort_orphaned`). A failed transform is recorded in `transforms` and retried with the same backoff while the warehouse is behind raw (`Pipeline.needs_transform`). The rate limit is per stream run, not yet per source (deferred to iRail).
- **0007 (admin console):** `toeval ui` (`src/toeval/ui/`, FastAPI on 127.0.0.1 + plain JS/SVG, no Node). The pipeline stays the **only writer**: the UI reads the state DB read-only, changes config only via `sources.toml` (`sources_file.set_stream_option(s)`: tomlkit keeps comments, validated with `pipeline.resolve_specs` before an atomic replace), and triggers work only by starting CLI commands as separate processes (`ui/actions.py`, output in `data/logs/console/`). POST actions need the per-session token embedded in the page (`X-Toeval-Token`) and a local `Origin`. Live progress comes from `data/state/events.jsonl` (`events.py`), appended by the lock holder (connectors, transform and the CLI emit through an `EventSink`; `NULL_SINK` by default) and tailed by `/api/events`; animations only show real events. `paused = true` on a stream makes `run-due` skip it; explicit `ingest`/`backfill` still run it. Even a read-only connection blocks DuckDB writers, so writing CLI commands retry opening the state DB for up to 30 s (`cli._open_state`), then exit 3. The console logs to its terminal only, never to `toeval.log` (holding it open would block rotation on Windows). The schedule editor (`/api/actions/schedule`) writes `schedule` / `revision_window` overrides, removes an override equal to the connector default, and refuses intervals under 5 min (the `run-due` tick). All 4 planned steps are built.
- **0006 (staging, marts, quality):** see the warehouse section. Amends 0002: `revision_lag` is computed from successive raw versions, not per run.

## Connector architecture (`src/toeval/connectors/`, ADR 0004)

The interface must fit three source shapes without special cases: queryable history with revisions (Elia), live-only polling with ETags (iRail), and monthly files keyed by reference period (Statbel). `tests/fakes.py` has one fake connector per shape, and new base-class behaviour should be tested against all three.

- `spec.py`: a `SourceSpec` (auth, `HttpPolicy`, streams) holds `StreamSpec`s (schedule interval|daily|monthly in Europe/Brussels, watermark kind timestamp|period|none, natural key, optional `revision_window` / `deep_refetch` / `backfill_start`). Defaults are declared on the connector class. Operational knobs can be overridden in an optional `sources.toml` (`[<source>.http]`, `[<source>.streams.<name>]`), merged and re-validated by `SourceSpec.with_overrides`. Auth names an env var; secrets are never in config.
- `base.py`: `Connector.run()` is the template method (no lock inside; the CLI takes it): plan → fetch → generic + `validate()` checks (a hard failure quarantines the batch and fails the run) → raw write → fold `next_watermark()`. State (watermark, `last_success_at`, deep-refetch time, HTTP validators) is committed atomically **only if every request succeeded**. A backfill keeps `last_success_at` unchanged, so it never makes a scheduled run look done. Everything after `start_run`, including the state commit itself, is guarded: any exception, including `KeyboardInterrupt` (which is re-raised), marks the run `failed`, so no row is left `running`. Watermarks never move backwards. Subclasses implement only `plan`, `fetch`, `next_watermark` (plus optional `validate`).
- `http.py`: `HttpClient` owns the token-bucket rate limit, retries (transport errors, 429, 5xx) with capped full-jitter backoff, `Retry-After` (a value above `max_retry_after`, default 5 min, fails the request instead of sleeping), auth injection, User-Agent, and conditional GET via `conditional_key`. Other 4xx errors fail without retry. Connectors never use httpx directly.
- `planning.py`: `time_window_plan` is an optional helper for timestamp streams: `start = watermark − revision_window` (deep window when due) rounded down to an `align` grid (default 1 day), chunk boundaries on a `chunk` grid, and the end rounded up to that grid, all anchored at `backfill_start`. Keys stay identical for re-runs within the same day. Once the start crosses a day boundary the first key changes, which is fine because that window holds new data.
- `FetchRequest.key` must be stable across runs for the same logical request. Requests within one run must cover disjoint natural keys. The raw dedup rule (`StateStore.is_unchanged`) depends on both.
- Raw (`warehouse/raw.py`): `<raw>/<source>/<stream>/ingest_date=YYYY-MM-DD/<HHMMSS>_<run8>_<sha(request_key)8>_<hash12>.parquet`, with `_ingested_at` and `_request_key` columns added. Empty batches are not written. A batch is skipped only if the latest file for its key has the same hash **and** no other run wrote to the stream since; this keeps A → B → A correct. Batches that fail the quality gate go to `<data>/quarantine/` (`Settings.quarantine_path`), **never under raw**, so staging globbing `raw/**` can't pick them up.
- `lock.py`: single-writer `FileLock` (stale after 6 h), taken at the CLI level, not inside `run()`.
- `registry.py` maps source names to connector classes. `src/toeval/pipeline.py` (`Pipeline`) applies `sources.toml` overrides (validated up front; unknown sources fail), decides what is due (`next_due`, `retry_delay`) and runs streams. `cli.py` adds settings, logging, the lock and the state store around it. Tests swap `registry.CONNECTORS` for fakes wired to mock servers with `fakes.bind`.
- Tests inject `transport=httpx.MockTransport(...)`, a fake `clock`, and `http_options={"sleep": ...}`, so they never sleep or touch the network.

## Warehouse (`src/toeval/warehouse/`, ADR 0006)

- `transform.py` rebuilds everything from raw on every run (~37 s with Elia, weather and IRCEL, 2026-10-21). It runs the SQL in `sql/staging/`, `sql/marts/` and `sql/quality/` in an in-memory DuckDB, `SET TimeZone = 'UTC'`, and writes one Parquet file per model; all are swapped in only after every model and check succeeded (a failure keeps every old output): `data/staging/stg_elia_{load,wind,solar}.parquet`, `stg_openmeteo_hourly.parquet`, `stg_irceline_hourly.parquet`, `data/marts/mart_energy_{15min,hourly}.parquet`, `mart_weather_hourly.parquet`, `mart_energy_weather_hourly.parquet`, `mart_air_hourly.parquet`. Models and checks are listed in `MODELS` / `CHECKS`; a model or check whose inputs don't exist yet is skipped. A new source adds its `RawInput`, models and checks there.
- SQL files reference only views (`raw_<source>_<stream>`, `stg_*`, `mart_*`); Python binds paths. Raw views read only `raw/` (never quarantine), with `filename = true, union_by_name = true`.
- Staging = latest `_ingested_at` per natural key (`QUALIFY row_number()`), renamed columns with units, `interval_start_utc` as `TIMESTAMPTZ`.
- Marts: gap-free 15-min grid (missing data = NULL row). Solar = `Belgium` row only. Wind = sum of the 5 parts, NULL unless all 5 are present (`wind_parts_present`). Hourly = mean of 4 quarters, NULL unless all 4 are present (`*_quarters`). Keys are UTC; `*_local` (Brussels) repeats in the fall-back hour. No imputation.
- Quality checks **warn, never fail**, even when a subject has no limit (a naming bug: logged as an error and stored as a warning with "no limit for this subject"). SQL is per source (`sql/quality/<source>/<check>.sql`); each returns `subject` (`<source>/<stream>`), `value`, `details`. The threshold is `Check.max_ok`, or per stream for `freshness` / `revision_lag` (`StreamSpec.freshness_limit` / `revision_lag_limit`, default 72 h / 80% of `revision_window`; `Pipeline.transform` passes the effective ones as `limits`, `transform()` defaults to the connectors'). The console's freshness bar uses the same per-stream limit. Results go to the log, `toeval status` and `quality_results` in the state DB. `measured_holes` and `revision_lag` only warn on the last 30 days; known historical holes are listed in ADR 0006.
- Tests (`tests/test_transform.py`) build raw files with the recorded fixtures' exact schemas and write them through `RawWriter`.

## Elia API facts (verified 2026-10-18)

Implemented in `src/toeval/connectors/elia.py`: source `elia`, streams `load` (ods001), `wind` (ods031), `solar` (ods032). Fixtures and how to re-record them: `tests/fixtures/elia/README.md`.

- Opendatasoft Explore v2.1 at `https://opendata.elia.be/api/explore/v2.1/`. No key. Rate limit 100M calls/day.
- `/records` caps `offset + limit` at 10,000, so every request is `/exports/parquet?where=datetime >= '<Z>' and datetime < '<Z>'`. An empty range returns a valid file with the full schema.
- Exports are slow: one year of ods001 ≈ 2 MB / 7 s, ods031 ≈ 9 MB / 48 s, ods032 ≈ 27 MB / 119 s. Hence chunks of 365 / 182 / 91 days and a 180 s timeout.
- `datetime` is the interval **start**, `timestamp[ms, tz=UTC]`. Resolution PT15M everywhere. Values are in MW.
- History: ods001 and ods031 from 2014-12-31T23:00Z (2015-01-01 Brussels), ods032 from 2020-09-30T22:00Z.
- ods001 has ~7 days of forecast rows (`totalload` null) and measured values up to the morning of the current day. ods031 and ods032 have **no forecast rows** and end at the last completed Brussels day.
- Exports send no ETag/Last-Modified. Rows come back unsorted, so `fetch` sorts by the natural key. The same window downloads byte-identically, so the raw dedup works.
- Natural keys: load `datetime`; wind `datetime, offshoreonshore, region, gridconnectiontype`; solar `datetime, region` (see ADR 0003).
- Licence: Elia Open Data Licence.
- DuckDB needs `pytz` to return `TIMESTAMP WITH TIME ZONE` values to Python (`fetchall`). It isn't installed, so keep timestamps in SQL/Arrow, or cast them to text.

## Open-Meteo facts (verified 2026-10-19, ADR 0008)

Implemented in `src/toeval/connectors/openmeteo.py`: source `openmeteo`, stream `hourly`, natural key `(location, time)`, 7 locations × 9 variables. Fixture: `tests/fixtures/openmeteo/README.md`.

- `https://archive-api.open-meteo.com/v1/archive`, no key, `models=era5` (0.25° grid, covers the sea), `wind_speed_unit=ms`, `timezone=GMT`. Non-commercial use only, CC-BY 4.0 (credit Open-Meteo and Copernicus ERA5).
- Limits 600 calls/min, 5,000/h, 10,000/day. A request costs one call per location per 2 weeks (more above 10 variables), so one yearly 7-location request ≈ 182 calls. Hence `requests_per_second=1/25`: a full backfill ≈ 2,150 calls in ~5 min. The per-minute limit is the binding one.
- ERA5 runs ~5–6 days behind. Hours not covered yet come back as nulls (dropped in `fetch`), not an error. `start_date`/`end_date` are inclusive dates; a date after today is HTTP 400, so `plan` caps `end_date` at today.
- `precipitation` and `shortwave_radiation` cover the hour *before* `time`; the other variables are instantaneous. Raw keeps API timestamps; the alignment to `hour_start_utc` belongs in the marts (rules in ADR 0008).
- `mart_weather_hourly` (one row per location and UTC hour `[h, h+1)`, gap-free per location): `precipitation` / `shortwave_radiation` from the value at h+1; snapshots averaged over h and h+1 (NULL unless both); wind direction from the mean u/v vector (NULL in calm air). Verified on synthetic and real data (ADR 0008, "Alignment, verified"): real ERA5 snapshots line up with Elia quarter-hours within 15 min.
- `mart_energy_weather_hourly`: one row per `mart_energy_hourly` hour; land weather = mean of the 6 land points, NULL unless all 6 (the SQL hard-codes 6; `tests/test_weather.py` asserts `LOCATIONS` has 6 land points). Offshore wind is separate. The last ~6 days have energy but no weather (ERA5 delay), as expected. Air quality: `air_antwerp_{no2,pm10,pm25,o3}_ugm3` from **one fixed station, 42R801** (hard-coded; `tests/test_air.py` asserts it is in `STATIONS`), never a mean of the stations present (ADR 0009, "Join"); NULL when 42R801 has no value. The mart now also needs `mart_air_hourly` to be built.
- A full backfill costs ~2,150 of the 5,000 calls/hour: never run two within an hour.
- Limits: freshness 8 days, `revision_lag` 96 days (80% of the 120-day deep re-check, which is what catches ERA5T corrections).

## IRCEL-CELINE facts (verified 2026-10-20, ADR 0009)

Implemented in `src/toeval/connectors/irceline.py`: source `irceline`, stream `hourly`, natural key `(station, pollutant, time)`, 15 curated `STATIONS` × NO2/PM10/PM2.5/O3 = 54 series. Fixtures: `tests/fixtures/irceline/README.md`.

- 52°North timeseries API `https://geo.irceline.be/sos/api/v1/`, no key, CC BY 4.0 (credit IRCEL-CELINE), no documented rate limit (we use 2 req/s).
- Series are looked up every run (`/timeseries?phenomenon=<id>&expanded=true`, 4 GETs); then one GET per series per window inside `fetch`, so one raw file per window. Timespans include both ends and must be shorter than 367 days (HTTP 400), hence 365-day chunks; future or pre-history ranges return an empty list.
- **Timestamps are the end of the hour** (UTC): the value at `time` covers `[time − 1 h, time)`. Raw keeps it, staging calls it `hour_end_utc`, and `mart_air_hourly` (one row per station and hour, pollutants as columns, gap-free per station) shifts to `hour_start_utc = hour_end_utc − 1 h`.
- Quality (`sql/quality/irceline/`, thresholds calibrated on 90 live days, ADR 0009): `freshness` follows the *freshest* series; `stale_series` (quiet > 48 h behind the newest value, only for the first 30 days), `coverage` (< 75% of the last 30 days' hours) and `value_range` (< −5 or > 1000 µg/m³; small negatives and peaks of several hundred are real, and the values stay) warn per series.
- Missing hours are `null` (dropped in `fetch`). Data is under ~3 h behind. Whether validated values later replace provisional ones is undocumented: deep re-check 400 days every 30 days, `revision_lag_limit` 320 days, `freshness_limit` 36 h.
- History (downloaded 2026-10-21, ~6.3M values, ~10 min): NO2/O3 from 2011, PM from 2012-05-31. IRCEL itself has **no NO2/O3 values 2012-01-01 to 2012-05-31** at any station. IRCEL publishes **PM2.5 = −9399.06** as a value (1,893 hours, 14 stations, 2021-01-04 to 2022-01-06): staging drops any value below −1000 µg/m³ as "no data" (raw keeps it, the mart shows a NULL hour), guarded by the whole-history `no_data_codes` check. Details and the long instrument gaps: ADR 0009, "History download".
- Rollout order for a new source: write `paused = true` to `sources.toml` first, then register (an unknown source in `sources.toml` fails every tick harmlessly; the other order could start the full download).

## Conventions

- Type hints everywhere; ruff (config in `pyproject.toml`); pytest; pre-commit (ruff runs via `uv run` so its version matches the lock file); standard `logging`.
- Config via `.env` with the `TOEVAL_` prefix (template: `.env.example`); never commit `.env`.
- State DB schema changes: **append** a step to `MIGRATIONS` in `state.py` (idempotent SQL; DuckDB can't add a column with constraints, so use `DEFAULT` only), never edit a released step. The scheduled task runs the working copy, so the real `data/state/state.duckdb` is migrated on its next tick: rehearse on a copy first.
- Tests never hit the network unless they're marked `@pytest.mark.live`; use recorded fixtures with `httpx.MockTransport`.
- One file per significant decision in `docs/decisions/NNNN-title.md`, including the why.
- Verify data source endpoints, terms and rate limits yourself rather than trusting the brief.
- Calibrate quality thresholds on real data before fixing them: fetch a sample with the connector's `fetch` into a throwaway `Settings(data_dir=<tmp>)`, write it with `RawWriter`, and run the real `transform` (done for IRCEL, ADR 0009). Never touch `data/` for experiments.
- Mutation-check new tests: break the SQL rule under test (e.g. the hour shift) and confirm a test fails.
- Docs: CLAUDE.md, the README (status, marts list, ADR table, file tree, roadmap, attribution) and the source's ADR are updated in the same commit as the code.

## Working notes (Windows dev machine)

- In Git Bash, uv needs `export PATH="$HOME/.local/bin:$PATH"`. The repo is at `/m/Documenten/Other/coding/unlabled data` (the path contains a space, so quote it).
- Multi-line edits: write a small Python script to the session scratchpad (a **new** file name each time) and run it with `uv run python <script>`, asserting each old string occurs exactly once. Long heredocs into Python are unreliable; `cat > file` with no input hangs.
- DuckDB quirks: `INTERVAL (-1) HOUR` needs the parentheses; timestamp literals as `TIMESTAMPTZ '2026-09-30 22:00:00+00'` (not `Z`); cast timestamps `::VARCHAR` before `fetchall()` (no `pytz`).
- pre-commit's end-of-file-fixer can rewrite new fixture files and fail the commit: `git add -A` and commit again (a new commit, not an amend).
- CI (GitHub Actions, windows-latest): `gh run list --limit 2` first. Right after a push, `gh run watch` on the latest run may still show the previous commit's run; watch the new run's id.
- Never commit `.env` or `sources.toml` (both gitignored). `sources.toml` is the owner's live operating state: the scheduled task reads it every 5 min.

## How the owner wants to work

- Ask clarifying questions first, then propose structure and plan before writing code.
- Build in small, working, tested steps. Commit each step with a clear message and push to GitHub.
- Be critical: flag bad ideas before building them.
