# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Project **toeval** (Dutch for "coincidence"). Python package `toeval`, CLI `toeval`. Phase 1 (foundation + Elia) is in progress. The original brief is `claude-code-kickoff-prompt.md`. **Keep this file updated as the project evolves.** Only document commands that exist and work.

## Commands

uv lives in `~/.local/bin`. If `uv` isn't found, prepend it to PATH.

```powershell
uv sync                          # install deps (incl. dev group)
uv run pytest                    # tests; `live`-marked tests (real APIs) are deselected by default
uv run pytest -m live            # only live API tests
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
- **0002:** Incremental fetch range = `min(watermark, now − revision_window) → now + forecast horizon`, so missed runs (laptop off) catch up automatically. The window is 14 days by default plus a monthly 90-day deep re-fetch. Both are provisional: every run logs `revision_lag`, the age of the oldest changed row. The watermark is the latest *measured* (non-null) timestamp and only advances after raw write, quality checks and staging all succeed. Identical re-fetches are skipped by content hash.
- **0003:** Elia hierarchy. Solar (`ods032`) has a `region='Belgium'` total plus regions and provinces, so **never sum all rows**. Wind (`ods031`) has 5 disjoint parts with no total, so sum them and track `parts_present`. Load (`ods001`) is one national series.

## Elia API facts (verified 2026-10-18)

- Opendatasoft Explore v2.1 at `https://opendata.elia.be/api/explore/v2.1/`. No key. Rate limit 100M calls/day.
- `/records` caps `offset + limit` at 10,000. For bulk data use `/exports/parquet?where=...` (one year of ods001 ≈ 2 MB, ~7 s).
- `datetime` is the interval **start**, in UTC. Resolution PT15M. Values are in MW.
- History: ods001 and ods031 from 2015-01-01 (Brussels time), ods032 from 2020-10-01. ods001 includes week-ahead forecast rows where `totalload` is null.
- Licence: Elia Open Data Licence.

## Conventions

- Type hints everywhere; ruff (config in `pyproject.toml`); pytest; pre-commit (ruff runs via `uv run` so its version matches the lock file); standard `logging`.
- Config via `.env` with the `TOEVAL_` prefix (template: `.env.example`); never commit `.env`.
- Tests never hit the network unless they're marked `@pytest.mark.live`; use recorded fixtures and `respx` to mock httpx.
- One file per significant decision in `docs/decisions/NNNN-title.md`, including the why.
- Verify data source endpoints, terms and rate limits yourself rather than trusting the brief.

## How the owner wants to work

- Ask clarifying questions first, then propose structure and plan before writing code.
- Build in small, working, tested steps. Commit each step with a clear message and push to GitHub.
- Be critical: flag bad ideas before building them.
