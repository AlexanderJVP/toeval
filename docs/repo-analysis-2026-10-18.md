# Repo analysis: toeval (2026-10-18, commit 5ed09fa)

## Snapshot
- Purpose: local Belgian/Antwerp open-data warehouse (DuckDB + Parquet, raw → staging → marts) feeding a spurious-correlation tracker and a time-series JEPA (later phases).
- Stack: Python >=3.12, uv (uv_build backend, uv.lock), duckdb, httpx, pyarrow, pydantic-settings, typer, tzdata; dev: pytest, ruff, pre-commit. CI: GitHub Actions windows-latest (.github/workflows/ci.yml).
- Phase/milestone: Phase 1 (foundation + Elia), steps 1–3 of 8 done (~40%).
- Branch state: main == origin/main at 5ed09fa; only change: docs/repo-analysis-2026-10-18.md (this file).

## Verified commands
| Command | Result | Notes |
|---|---|---|
| `uv run pytest` | 64 passed, 3.6s | documented (CLAUDE.md:15) |
| `uv run pytest -m live` | 64 deselected, 0 selected | documented; no live tests exist yet |
| `uv run ruff check .` | All checks passed | documented |
| `uv run ruff format --check .` | 32 files already formatted | check variant of documented `ruff format .` |
| `uv run pre-commit run --all-files` | 8/8 hooks Passed | documented |
| `uv run toeval --help` | works; only `version` command | documented |
| `gh run list -L 3` | 5ed09fa: success (37s); ce744e0: failure (setup-uv@v10 tag missing, fixed in 5ed09fa) | undocumented |
| ad-hoc probe (fake connector) | backfill sets last_success_at → `is_due` False; finish_run_success raising → runs row stays 'running' | evidence for Findings 2 and 3 |

uv needs `export PATH="$HOME/.local/bin:$PATH"` in bash (CLAUDE.md:11).

## Architecture as built
- src/toeval/config.py: `Settings` (prefix TOEVAL_, env_file REPO_ROOT/.env, REPO_ROOT = parents[2] of the file, so it assumes an editable install). Paths: data_path, raw_path=data/raw, quarantine_path=data/quarantine, state_db_path=data/state/state.duckdb, lock_path=data/state/ingest.lock, sources_path. `get_settings()` lru_cached.
- src/toeval/log.py: `setup_logging(level, log_dir)`: console + RotatingFileHandler(toeval.log, 5 MB × 5); httpx at WARNING. Not called by the CLI yet.
- src/toeval/state.py: `Watermark` (TIMESTAMP aware datetime | PERIOD str; NONE ⇒ use None). `StateStore` on its own DuckDB with tables stream_state, runs, raw_files. Naive UTC in the DB, aware UTC in code (`_to_db` rejects naive, state.py:122). `finish_run_success` is one transaction (state.py:203–221; ROLLBACK only on Exception). `is_unchanged` (state.py:271): skip only if the latest file for the key has the same hash AND no other run (excluding latest_run and current_run) has ingested_at >= latest_at.
- src/toeval/connectors/spec.py: frozen pydantic specs. Schedules interval/daily/monthly (Europe/Brussels), `is_due(schedule, last_success, now)` (spec.py:95) uses only last_success. Auth none/api_key/bearer via env var name. HttpPolicy defaults: 1 rps, burst 1, 5 retries, backoff 1s→60s, max_retry_after 5m, timeout 60s. StreamSpec validators (spec.py:170). `SourceSpec.with_overrides` deep-merges; changing a union `kind` replaces wholesale. `load_overrides(path)`.
- src/toeval/connectors/http.py: `HttpClient` with token bucket, retries on TransportError + retry_statuses, full-jitter capped backoff, Retry-After (> max_retry_after ⇒ HttpError), auth injection, User-Agent, conditional GET by `conditional_key` (validators stored on 200, dict owned by caller).
- src/toeval/connectors/planning.py: `time_window_plan(stream, state, ctx, chunk, horizon=0, align=1d)`. Backfill: [backfill_from or origin, backfill_to or ceil(now+horizon)), unaligned. First run: origin. Later: floor(watermark − window (deep window when due), align). Chunks on a chunk grid anchored at backfill_start.
- src/toeval/connectors/base.py: `Connector.run()` order: start_run → try { HttpClient (l.172) → plan → fetch → `_generic_checks` + `validate` → quarantine + QualityGateError | raw write → `_max_watermark` fold } except BaseException → finish_run_failed (re-raise non-Exception) → finally http.close → **finish_run_success at l.237, outside the try** → new_state.last_success_at = now for every mode (l.233). One HttpClient (hence one token bucket) per run() call.
- src/toeval/warehouse/raw.py: `<raw>/<source>/<stream>/ingest_date=YYYY-MM-DD/<HHMMSS>_<run8>_<sha(key)8>_<hash12>.parquet`, adds `_ingested_at` (= run start) and `_request_key`. 0-row batches are not written (l.55). File is written atomically (tmp + os.replace), then the manifest row is inserted (not atomic with the file). Quarantine uses the same layout under quarantine_root, without metadata columns.
- src/toeval/lock.py: `FileLock` (O_EXCL, 3 attempts, stale after 6h, takeover only if a re-read matches, release only if the token matches). Not wired up yet.
- src/toeval/cli.py: `version` only.
- tests/fakes.py: FakeSeriesConnector (hourly, daily 06:30, 2d window, deep 30d/10d, 7d chunk), FakeLiveConnector (ETag, 5m interval, NONE watermark), FakePeriodConnector (Last-Modified, monthly, PERIOD watermark).

## Source-of-truth conflicts
1. CLAUDE.md:54 and base.py:170 ("Everything after start_run is inside the try … so no row is left running") vs base.py:237 (finish_run_success outside the try). Code is wrong: fix the code (Finding 3).
2. docs/decisions/0004-connector-interface.md:51 ("a token bucket per source") vs base.py:172 (one HttpClient per run(), i.e. per stream run). Decide in step 7 (Finding 4) and update whichever is wrong.
3. ADR 0004:47,72,76 and CLAUDE.md reference `run-due` / `ingest` / `transform`, which don't exist yet (cli.py has only `version`). This describes intended future state; acceptable, but don't add them to the CLAUDE.md Commands section until they work (CLAUDE.md:7).
4. tests/test_connector_base.py:291 heading "regressions from docs/repo-analysis-2026-10-18.md" refers to findings of the earlier analysis (at 10017f8) that this file no longer contains. Reword it to describe the regressions themselves.

## Findings
1. [severity: medium] [design] Failed streams are re-run on every scheduler tick with no backoff
   - Where: src/toeval/connectors/spec.py:95-100; base.py:214-223 (the failure path doesn't touch stream_state)
   - Evidence: `is_due` depends only on last_success; failures record nothing in stream_state.
   - Failure scenario: Elia ods001 first run (full history since 2015) fails a quality gate → `run-due` every 5 min re-downloads the full history and writes a new quarantine file each time, indefinitely.
   - Suggested fix (step 7, with run-due): add `consecutive_failures` and `last_failure_at` to stream_state, written by finish_run_failed. Make a due-check that also requires `now >= last_failure_at + min(schedule period, 5m·2^(n−1))` (cap e.g. 6h). Test against all three fakes.
2. [severity: medium] [correctness] Backfill runs suppress the next scheduled incremental run
   - Where: src/toeval/connectors/base.py:229-236
   - Evidence: probe: fresh stream, backfill ORIGIN..2026-01-03 at 2026-01-20T07:00Z → last_success_at=2026-01-20T07:00Z, watermark 2026-01-02T23:00Z, `is_due(daily 06:30)` = False.
   - Failure scenario: owner backfills an old range during the day → today's daily run is skipped, data after the backfill range waits until tomorrow.
   - Suggested fix: in BACKFILL mode keep `state.last_success_at`, i.e. `last_success_at=now if mode is RunMode.INCREMENTAL else state.last_success_at`. Watermark, validators and runs row are still committed. Add a test in tests/test_connector_base.py asserting last_success_at unchanged after backfill.
3. [severity: low] [correctness] State commit failure leaves the run 'running'; interrupt mid-commit leaves a transaction open
   - Where: src/toeval/connectors/base.py:237; src/toeval/state.py:219
   - Evidence: probe with `finish_run_success` raising OSError → exception propagates, runs statuses [('success',), ('running',)]. state.py:219 catches `Exception` only, so KeyboardInterrupt skips ROLLBACK.
   - Failure scenario: DuckDB I/O error or Ctrl+C during commit → stale 'running' row; with KeyboardInterrupt the connection stays inside an open transaction.
   - Suggested fix: wrap l.237 in try/except BaseException → finish_run_failed(error="state commit failed: …"), re-raise non-Exception, return a failed result otherwise. Change state.py:219 to `except BaseException`. Test with a monkeypatched store.
4. [severity: low] [design/doc drift] Rate limit is per run, not per source
   - Where: src/toeval/connectors/base.py:172; ADR 0004:51
   - Failure scenario: run-due runs several streams of one source back to back (e.g. iRail, 3 rps burst 5) → each run starts with a full burst, exceeding the limit at boundaries → 429s.
   - Suggested fix: when building run-due, let the CLI create one TokenBucket per source and inject it (e.g. an optional `bucket` argument on Connector/HttpClient), or correct the ADR to "per run". Irrelevant for Elia.
5. [severity: low] [design] Retractions are invisible
   - Where: src/toeval/warehouse/raw.py:367-369
   - Failure scenario: a request key that previously returned rows returns 0 rows (source withdrew data) → nothing is written; staging's "latest per natural key" keeps the withdrawn rows.
   - Suggested fix: document as a known limitation in ADR 0004 now. If a source needs it, record an empty-batch marker in raw_files and make staging treat the latest file per request key as authoritative for its range.
6. [severity: low] [hygiene] CI (.github/workflows/ci.yml:17-20) runs ruff + pytest but not the pre-commit-only hooks (check-yaml/toml, detect-private-key, whitespace). Add `uv run pre-commit run --all-files`, or accept it.

Checked and fine: path collisions (run8 + request hash), A→B→A dedup, quarantine outside raw, Retry-After cap, lock token handling, DST handling in DailySchedule/MonthlySchedule (nonexistent local times resolve without error), watermark never moving backwards (base.py:276), validators only persisted on success.

## Not yet built (per roadmap)
1. Elia connector: ods001, ods031, ods032 via `/exports/parquet?where=...`; next_watermark = max datetime with a non-null measured value; validate() checks the solar hierarchy (Belgium ≈ Σ regions within 1%, ADR 0003); wind parts_present.
2. Fuller quality checks (schema, nulls, gaps, duplicates) with logged results.
3. Staging SQL in `.sql` files (typed, UTC, latest `_ingested_at` per natural key, reading data/raw only); `revision_lag` logging (ADR 0002); one mart (mart_energy_15min + hourly) with DST tests.
4. CLI `ingest` / `backfill` / `transform` / `run-due`: FileLock, setup_logging(log_dir=data/logs), load_overrides + with_overrides, is_due + failure backoff (Finding 1), shared per-source rate limit (Finding 4).
5. Windows Task Scheduler entry (`run-due` every 5 min).
6. Full README (architecture, Spark/cloud trade-off, how to run).

## Recommended next tasks
1. Fix Findings 2 and 3. Files: src/toeval/connectors/base.py, src/toeval/state.py, tests/test_connector_base.py. Acceptance: new tests (backfill leaves last_success_at unchanged; commit failure ⇒ runs row 'failed'); all tests, ruff and format pass. Reword the heading at tests/test_connector_base.py:291. One commit, push.
2. Elia connector, ods001 first. Files: new src/toeval/connectors/elia.py, tests/test_elia.py, tests/fixtures/elia/ (small recorded export). Use time_window_plan (chunk ≈ 1 year for history, horizon ≈ 7 days for forecast rows), natural key ("datetime",). Acceptance: MockTransport tests for first run, revision window, null-totalload rows not advancing the watermark; one @pytest.mark.live smoke test. Re-verify the API facts live before coding (CLAUDE.md:62-68,76). Propose the plan to the owner first; ask before adding ods031/ods032 in the same step.
3. Then staging + mart, then the CLI (including Findings 1 and 4), then scheduler + README.

## Conventions an agent must follow here
- Ask clarifying questions and propose a plan before coding; small tested steps; commit + push each step; flag bad ideas (CLAUDE.md:78-82).
- No scaffolding for later phases (CLAUDE.md:40).
- Type hints everywhere; ruff line-length 100, rules E,F,W,I,B,UP,N,SIM,RUF,DTZ,PT (pyproject.toml); module-level `log = logging.getLogger(__name__)`.
- Tests: no network unless `@pytest.mark.live`; use httpx.MockTransport, a fake clock and `http_options={"sleep": ...}` (CLAUDE.md:60,74). New base-class behaviour is tested against all three fakes (CLAUDE.md:51).
- Config via `.env` with prefix TOEVAL_; secrets only referenced by env var name; never commit `.env` (CLAUDE.md:73).
- Aware UTC in code, naive UTC in the state DB (state.py:4). Quarantine never under raw. FetchRequest.key stable across runs; requests within a run cover disjoint natural keys (CLAUDE.md:57).
- SQL in `.sql` files; raw immutable; staging/marts rebuildable (ADR 0001).
- One ADR per significant decision in docs/decisions/NNNN-title.md; keep CLAUDE.md current; document only commands that exist and work (CLAUDE.md:7,75).
- Verify data source endpoints, terms and limits yourself (CLAUDE.md:76).
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Coverage of this analysis
- Read fully: all of src/ (12 files), tests/test_connector_base.py (start, end and grep), test_lock.py, test_log.py, CLAUDE.md, ADR 0004, README.md, .env.example, ci.yml. Sampled: tests/fakes.py and the other test files (via test names and grep). ADRs 0001–0003 were not re-read this pass (unchanged since their last review).
- Ran: pytest, pytest -m live, ruff check, ruff format --check, pre-commit, toeval --help, gh run list, an ad-hoc probe script (scratchpad) for Findings 2 and 3.
- Not run: live API calls (no live tests; Elia facts last verified 2026-10-18 per CLAUDE.md).
