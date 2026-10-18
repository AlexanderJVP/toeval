# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Project **toeval** (Dutch for "coincidence"). Python package `toeval`, CLI `toeval`. Phase 1 (foundation + Elia) is in progress. The original brief is `claude-code-kickoff-prompt.md`. **Keep this file updated as the project evolves.** Only document commands that exist and work.

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
```

GitHub remote: `AlexanderJVP/toeval` (private until Phase 1 is presentable).

## Project vision

A personal portfolio data platform (owner: applied informatics / AI-ML student in Antwerp, targeting data engineering roles). It should look and run like a production system, not a notebook. It continuously ingests live Belgian / Antwerp public data into one local warehouse that will be reused by future projects. Two applications sit on top:

1. **Spurious correlation tracker**: automatically finds absurd cross-dataset correlations and tracks week by week how they break down on new data. Playful tone, rigorous statistics (multiple-testing correction, detrending, out-of-sample tracking). Planned framing: "a naive scan finds X, rigorous methods keep Y, and here is how both decay".
2. **JEPA on unlabeled time series**: self-supervised pretraining that masks time spans and predicts their *latent* representation, then fine-tunes small heads on few-label tasks. It **must be benchmarked against simple baselines trained on the same few labels**. The corpus is small, so "baselines win" is a legitimate result that must be reported honestly.

## Phases

1. **Foundation + Elia** (current): tooling, connector interface, Elia connector (backfill + incremental), raw → staging → one mart, quality checks, tests, Task Scheduler job, README.
2. More sources: Open-Meteo, iRail, Telraam (API token), IRCEL-CELINE, Antwerp/Statbel. Aligned time grains in marts.
3. Correlation tracker (weekly job, static report or small dashboard).
4. JEPA + baselines, results in README.
5. Public dashboard/report.

Do not scaffold folders or code for later phases ahead of time.

## Architecture decisions (details in `docs/decisions/`)

- **0001:** DuckDB compute over Parquet storage under `TOEVAL_DATA_DIR` (default `./data`, gitignored). Layers: `raw` (immutable, one file per fetch, Hive-style partitions) → `staging` (typed, UTC, latest version per natural key, rebuildable) → `marts` (15 min / hourly / daily / weekly). SQL lives in `.sql` files so it can be ported to Spark or a cloud warehouse. Single writer, protected by a lock file.
- **0002 (amended by 0004):** Incremental fetch range = `watermark − revision_window` (floored to a day grid) → `now + forecast horizon`, so missed runs (laptop off) catch up automatically and still re-check the window before the old watermark. The window is 14 days by default plus a monthly 90-day deep re-fetch. Both are provisional: every run will log `revision_lag`, the age of the oldest changed row (not built yet; needs staging). The watermark is the latest *measured* (non-null) timestamp and advances only after the raw writes and quality checks of **all** requests succeed. It does not wait for staging, which is a separate step.
- **0003:** Elia hierarchy. Solar (`ods032`) has a `region='Belgium'` total plus regions and provinces, so **never sum all rows**. Wind (`ods031`) has 5 disjoint parts with no total, so sum them and track `parts_present`. Load (`ods001`) is one national series.
- **0004 (connector interface):** see the next section.

## Connector architecture (`src/toeval/connectors/`, ADR 0004)

The interface must fit three source shapes without special cases: queryable history with revisions (Elia), live-only polling with ETags (iRail), and monthly files keyed by reference period (Statbel). `tests/fakes.py` has one fake connector per shape, and new base-class behaviour should be tested against all three.

- `spec.py`: a `SourceSpec` (auth, `HttpPolicy`, streams) holds `StreamSpec`s (schedule interval|daily|monthly in Europe/Brussels, watermark kind timestamp|period|none, natural key, optional `revision_window` / `deep_refetch` / `backfill_start`). Defaults are declared on the connector class. Operational knobs can be overridden in an optional `sources.toml` (`[<source>.http]`, `[<source>.streams.<name>]`), merged and re-validated by `SourceSpec.with_overrides`. Auth names an env var; secrets are never in config.
- `base.py`: `Connector.run()` is the template method (no lock inside; the CLI takes it): plan → fetch → generic + `validate()` checks (a hard failure quarantines the batch and fails the run) → raw write → fold `next_watermark()`. State (watermark, `last_success_at`, deep-refetch time, HTTP validators) is committed atomically **only if every request succeeded**. A backfill keeps `last_success_at` unchanged, so it never makes a scheduled run look done. Everything after `start_run`, including the state commit itself, is guarded: any exception, including `KeyboardInterrupt` (which is re-raised), marks the run `failed`, so no row is left `running`. Watermarks never move backwards. Subclasses implement only `plan`, `fetch`, `next_watermark` (plus optional `validate`).
- `http.py`: `HttpClient` owns the token-bucket rate limit, retries (transport errors, 429, 5xx) with capped full-jitter backoff, `Retry-After` (a value above `max_retry_after`, default 5 min, fails the request instead of sleeping), auth injection, User-Agent, and conditional GET via `conditional_key`. Other 4xx errors fail without retry. Connectors never use httpx directly.
- `planning.py`: `time_window_plan` is an optional helper for timestamp streams: `start = watermark − revision_window` (deep window when due) rounded down to an `align` grid (default 1 day), chunk boundaries on a `chunk` grid, and the end rounded up to that grid, all anchored at `backfill_start`. Keys stay identical for re-runs within the same day. Once the start crosses a day boundary the first key changes, which is fine because that window holds new data.
- `FetchRequest.key` must be stable across runs for the same logical request. Requests within one run must cover disjoint natural keys. The raw dedup rule (`StateStore.is_unchanged`) depends on both.
- Raw (`warehouse/raw.py`): `<raw>/<source>/<stream>/ingest_date=YYYY-MM-DD/<HHMMSS>_<run8>_<sha(request_key)8>_<hash12>.parquet`, with `_ingested_at` and `_request_key` columns added. Empty batches are not written. A batch is skipped only if the latest file for its key has the same hash **and** no other run wrote to the stream since; this keeps A → B → A correct. Batches that fail the quality gate go to `<data>/quarantine/` (`Settings.quarantine_path`), **never under raw**, so staging globbing `raw/**` can't pick them up.
- `lock.py`: single-writer `FileLock` (stale after 6 h), taken at the CLI level, not inside `run()`.
- Tests inject `transport=httpx.MockTransport(...)`, a fake `clock`, and `http_options={"sleep": ...}`, so they never sleep or touch the network.

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

## Conventions

- Type hints everywhere; ruff (config in `pyproject.toml`); pytest; pre-commit (ruff runs via `uv run` so its version matches the lock file); standard `logging`.
- Config via `.env` with the `TOEVAL_` prefix (template: `.env.example`); never commit `.env`.
- Tests never hit the network unless they're marked `@pytest.mark.live`; use recorded fixtures with `httpx.MockTransport`.
- One file per significant decision in `docs/decisions/NNNN-title.md`, including the why.
- Verify data source endpoints, terms and rate limits yourself rather than trusting the brief.

## How the owner wants to work

- Ask clarifying questions first, then propose structure and plan before writing code.
- Build in small, working, tested steps. Commit each step with a clear message and push to GitHub.
- Be critical: flag bad ideas before building them.
