# Repo analysis: toeval (2026-10-18, commit b3350b1)

## Snapshot
- Purpose: local Belgian/Antwerp open-data warehouse (DuckDB + Parquet, raw → staging → marts) feeding a spurious-correlation tracker and a time-series JEPA (later phases).
- Stack: Python >=3.12, uv (uv_build, uv.lock), duckdb, httpx, pyarrow, pydantic-settings, typer, tzdata; dev: pytest, ruff, pre-commit. CI: GitHub Actions windows-latest (.github/workflows/ci.yml).
- Phase/milestone: Phase 1 (foundation + Elia) — ~55%: framework, raw layer, Elia connector done; no CLI ingest, staging, mart, scheduler, README.
- Branch state: main == origin/main at b3350b1, clean.

## Verified commands
| Command | Result | Notes |
|---|---|---|
| `uv run pytest` | 80 passed, 1 deselected, 5.28s | documented |
| `uv run pytest -m live` | 1 passed, 1.49s (real Elia API) | documented; needs network |
| `uv run ruff check .` | All checks passed | documented |
| `uv run ruff format --check .` | 35 files already formatted | check variant of documented `ruff format .` |
| `uv run pre-commit run --all-files` | 8/8 Passed | documented |
| `gh run list -L 3` | 3× success (31–40s) | undocumented |

uv needs `PATH="$HOME/.local/bin:$PATH"` (CLAUDE.md:11).

## Architecture as built
- `src/toeval/config.py` — `Settings` (prefix `TOEVAL_`); raw_path=data/raw, quarantine_path=data/quarantine, state_db_path=data/state/state.duckdb, lock_path=data/state/ingest.lock, sources_path.
- `src/toeval/log.py` — `setup_logging` (console + rotating file, idempotent); not called by CLI yet.
- `src/toeval/state.py` — `StateStore`: `stream_state`, `runs`, `raw_files`; naive-UTC storage; `finish_run_success` transaction rolls back on BaseException (state.py:219).
- `src/toeval/connectors/spec.py` — frozen pydantic specs; `HttpPolicy.max_retry_after` 5 min.
- `src/toeval/connectors/http.py` — `HttpClient`: token bucket, retries (transport/429/5xx), capped jitter backoff, Retry-After cap, env-var auth, conditional GET.
- `src/toeval/connectors/planning.py` — `time_window_plan(chunk, horizon, align=1d)`, grids anchored at `backfill_start`.
- `src/toeval/connectors/base.py` — `Connector.run()`: start_run → try(HttpClient, plan, fetch, checks→quarantine, raw write, watermark fold) → `_fail` on BaseException (re-raises non-Exception) → state commit guarded by `_fail` (l.226-229). Backfill keeps `last_success_at` (l.~221).
- `src/toeval/connectors/elia.py` — `EliaConnector`, source `elia`, streams `load` (ods001, measured col `totalload`, chunk 365d, horizon 8d), `wind` (ods031, `measured`, 182d, 0), `solar` (ods032, `measured`, 91d, 0) (l.67-71). All: DailySchedule 12:00 Brussels, revision_window 14d, deep 90d every 30d (l.74-84), HttpPolicy timeout 180s (l.90). fetch = `/exports/parquet?where=datetime >= 'Z' and datetime < 'Z'`, sorted by natural key (l.108-112). validate: resolution warning, unknown wind parts warning, solar Belgium≈Σregions within max(1%, 1 MW) hard check via DuckDB (l.140-178).
- `src/toeval/warehouse/raw.py` — raw path `<raw>/<source>/<stream>/ingest_date=…/<HHMMSS>_<run8>_<sha(key)8>_<hash12>.parquet`; 0-row batches skipped (l.55); quarantine outside raw.
- `src/toeval/lock.py` — token-owned `FileLock`; not wired.
- `src/toeval/cli.py` — `version` only.
- Tests: tests/fakes.py (3 source shapes), tests/test_elia.py (14 tests + 1 live), fixtures tests/fixtures/elia/{ods001,ods031,ods032}.parquet with README describing ranges.

## Source-of-truth conflicts
1. docs/decisions/0002-incremental-loads-revision-window.md:21 "Backfill … chunked by year" vs elia.py:67-71 (365/182/91-day chunks) and CLAUDE.md:61 (explains why). Code + CLAUDE.md win → amend ADR 0002 §6.
2. docs/decisions/0004-connector-interface.md:47,72,76 and CLAUDE.md:54 ("the CLI takes it") reference CLI commands `run-due`, `ingest`, `transform` that don't exist (cli.py). Intended future state; implement rather than edit docs.

## Findings
1. [severity: medium] [progress/operability] Elia connector has no entry point
   - Where: src/toeval/cli.py (only `version`); src/toeval/lock.py, src/toeval/log.py unwired; spec.load_overrides/with_overrides unused outside tests.
   - Evidence: `uv run toeval --help` lists only `version`.
   - Failure scenario: no backfill or daily runs can happen; ADR 0002 §3 revision-window tuning depends on weeks of logged runs, which cannot start.
   - Suggested fix: Task 1 below.
2. [severity: low] [doc-drift] ADR 0002 backfill chunking (conflict 1).
3. [severity: low] [design] Empty batches dropped → retractions invisible (src/toeval/warehouse/raw.py:55-57); not documented in any ADR. Document in ADR 0001/0004 before staging; staging must not assume an absent raw file for a key range means "no data".
4. [severity: low] [hygiene] CI (.github/workflows/ci.yml) runs ruff + pytest but not pre-commit-only hooks (check-yaml/toml, detect-private-key, whitespace). Optional step `uv run pre-commit run --all-files`.

## Not yet built (per roadmap)
1. CLI: `ingest <source> [<stream>]`, `backfill <source> <stream> [--from --to]`, `run-due` (FileLock around all streams, setup_logging(settings.log_level, data/logs), sources.toml overrides, `is_due` per stream).
2. Staging SQL (`.sql` files): typed UTC, latest `_ingested_at` per natural key, read only data/raw.
3. `revision_lag` logging (ADR 0002 §3; needs staging).
4. One mart: national load, solar (`region='Belgium'`), wind (Σ5 parts + `parts_present`) at 15 min + hourly (ADR 0003).
5. Fuller logged quality checks (schema, nulls, gaps, duplicates).
6. Windows Task Scheduler entry (`run-due` every 5 min).
7. README (architecture, Spark/cloud trade-off, how to run).

## Recommended next tasks
1. CLI ingestion commands. Files: src/toeval/cli.py, possibly src/toeval/registry.py (source name → connector class), tests/test_cli.py. Accept: `toeval ingest elia` runs all 3 streams under FileLock; `toeval run-due` skips non-due streams (FakeClock), exits non-zero if any stream failed, exits cleanly with message on LockHeldError; tests use typer CliRunner + MockTransport (no network). Update CLAUDE.md Commands only with commands that work (CLAUDE.md:7).
2. Amend ADR 0002 §6 (chunk sizes per dataset, link elia.py) and document empty-batch/retraction limitation (ADR 0004 or 0001).
3. Staging SQL for Elia + `transform` command. Files: src/toeval/warehouse/staging.py, src/toeval/sql/staging/elia_*.sql, tests. Accept: latest version per natural key across multiple raw files (A→B→A test), quarantine ignored, rebuild idempotent. Note CLAUDE.md:68: DuckDB needs pytz to return TIMESTAMPTZ to Python; keep timestamps in SQL/Arrow.
4. Mart + quality checks + revision_lag; then Task Scheduler + README.

Each step: propose plan first, small commit, push (CLAUDE.md:80-81).

## Conventions an agent must follow here
- Ask clarifying questions and propose a plan before code; small tested steps; commit + push each step; flag bad ideas (CLAUDE.md:78-82).
- No scaffolding for later phases (CLAUDE.md:40).
- Type hints everywhere; ruff (line-length 100; E,F,W,I,B,UP,N,SIM,RUF,DTZ,PT) per pyproject.toml; module-level `log = logging.getLogger(__name__)`.
- No network in tests unless `@pytest.mark.live`; httpx.MockTransport, fake clock, `http_options={"sleep": ...}`; recorded fixtures under tests/fixtures/<source>/ with a README (CLAUDE.md:60,74; tests/fixtures/elia/README.md). New base-class behaviour tested against all three fakes (CLAUDE.md:51).
- Config via `.env`, prefix `TOEVAL_`; secrets only as env var names; never commit `.env` (CLAUDE.md:53,73).
- Aware UTC in code, naive UTC in state DB (state.py:4); quarantine never under raw (CLAUDE.md:58).
- Elia: never sum all solar rows; national solar = `region='Belgium'`; wind = Σ5 parts + parts_present (ADR 0003).
- SQL in `.sql` files; raw immutable; staging/marts rebuildable (ADR 0001).
- One ADR per significant decision; keep CLAUDE.md current; only document working commands (CLAUDE.md:7,75). Verify source facts yourself (CLAUDE.md:76).

## Coverage of this analysis
- Read fully: diff 5ed09fa..b3350b1 (src/toeval/connectors/elia.py, base.py, state.py, CLAUDE.md, ADR 0003, fixtures README); test names in tests/test_elia.py and new tests in tests/test_connector_base.py. Files unchanged since 10017f8/5ed09fa were read in earlier analyses the same day, not re-read.
- Ran: pytest, pytest -m live (network, 1 test), ruff check, ruff format --check, pre-commit, gh run list.
- Not run: a real backfill (no CLI; long network run).
