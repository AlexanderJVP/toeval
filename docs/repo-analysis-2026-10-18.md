# Repo analysis: toeval (2026-10-18, commit 10017f8)

## Snapshot
- Purpose: local Belgian open-data warehouse (DuckDB + Parquet) feeding a spurious-correlation tracker and a time-series JEPA (later phases).
- Stack: Python >=3.12 (.python-version), uv (uv_build backend, uv.lock present), duckdb, httpx, pyarrow, pydantic-settings, typer, tzdata; dev: pytest, ruff, pre-commit.
- Phase/milestone: Phase 1 (foundation + Elia) — ~35–40%: framework done, no Elia connector / staging / mart / scheduler.
- Branch state: main == origin/main at 10017f8, working tree clean.

## Verified commands
| Command | Result | Notes |
|---|---|---|
| `uv run pytest` | 57 passed, 2.79s | documented |
| `uv run pytest -m live` | 0 selected, exit code 5 | documented; no live tests exist yet, so it "fails" in scripts |
| `uv run ruff check .` | All checks passed | documented |
| `uv run ruff format --check .` | 30 files already formatted | documented as `ruff format .` (write mode); I ran check mode |
| `uv run pre-commit run --all-files` | all 8 hooks Passed | documented |
| `uv run toeval --help` | works; only `version` command | documented |
| uv requires `PATH="$HOME/.local/bin:$PATH"` | — | per CLAUDE.md:11 |

## Architecture as built
- `src/toeval/config.py` — `Settings` (env prefix `TOEVAL_`, `.env` at REPO_ROOT = parents[2] of the file); derived paths: `raw_path`=data/raw, `state_db_path`=data/state/state.duckdb, `lock_path`=data/state/ingest.lock, `sources_path`.
- `src/toeval/log.py` — `setup_logging` (console + rotating file); not called anywhere yet.
- `src/toeval/state.py` — `StateStore` over a separate DuckDB file: tables `stream_state`, `runs`, `raw_files`. Invariant: timestamps stored naive UTC, converted at `_to_db`/`_from_db` (state.py:122-131); naive input rejected. `finish_run_success` commits stream_state + run row in one transaction (state.py:203-221). `is_unchanged` implements the A→B→A-safe dedup (state.py:271-295).
- `src/toeval/connectors/spec.py` — pydantic frozen models: schedules (interval/daily/monthly, Europe/Brussels), auth (none/api_key/bearer, env-var only), `HttpPolicy`, `DeepRefetch`, `StreamSpec`, `SourceSpec.with_overrides` + `load_overrides` (sources.toml).
- `src/toeval/connectors/http.py` — `HttpClient`: token bucket, retries on transport/429/5xx with full-jitter capped backoff, Retry-After, auth injection, conditional GET keyed by `conditional_key`; `requests_sent` counter.
- `src/toeval/connectors/planning.py` — `time_window_plan` (grid anchored at `backfill_start`, end ceiled to grid).
- `src/toeval/connectors/base.py` — `Connector.run()` template: state → plan → per request: fetch → generic+validate checks (fail ⇒ quarantine + QualityGateError) → raw write → fold watermark (never backwards, `_max_watermark` base.py:266). State (watermark, last_success_at, last_deep_refetch_at, validators) committed only if all requests succeed.
- `src/toeval/warehouse/raw.py` — `RawWriter`: `<raw>/<source>/<stream>/ingest_date=YYYY-MM-DD/<HHMMSS>_<run8>_<hash12>.parquet`, atomic tmp+replace, adds `_ingested_at`, `_request_key`; registers in `raw_files`.
- `src/toeval/lock.py` — `FileLock` (O_EXCL, stale after 6h by mtime). Not wired anywhere (no CLI ingest command yet).
- `src/toeval/cli.py` — typer app with `version` only.
- `tests/fakes.py` — three fake connectors (series/live/period) per ADR 0004 shapes; `tests/conftest.py` — `settings`, `store` fixtures in tmp_path.

## Source-of-truth conflicts
1. CLAUDE.md:45 (0002 summary) says fetch range `min(watermark, now − revision_window)` and watermark advances after "raw write, quality checks and staging". docs/decisions/0002:16,19 and 0004:61-67 say `watermark − revision_window` and no staging dependency. Code (planning.py:59, base.py:218-227) matches 0004. → Update CLAUDE.md:45.
2. docs/decisions/0004:41 pipeline is `lock → ... → write raw → validate()`. Code validates before writing (base.py:186-201) and takes no lock in run(); CLAUDE.md:54,59 match the code. → Update 0004:41.
3. CLAUDE.md:56 / planning.py:35-37 claim re-runs produce identical request keys. Only true when the watermark is unchanged: first window starts at unaligned `watermark − window` (planning.py:59,66-70). test_planning.py:31 tests only the unchanged-watermark case. → Fix code or wording (see Finding 5).
4. raw.py:76 docstring "outside the raw layer" vs implementation `self._root / "_quarantine"` where root is raw_path (raw.py:77,91; base.py:115). Code is wrong relative to intent.
5. CLAUDE.md:46 lists 0004 before 0003 (ordering only).

## Findings
1. [severity: high] [correctness] Identical-content requests within one run collide on raw path → run fails
   - Where: src/toeval/warehouse/raw.py:58,97; src/toeval/state.py:109-110,297-310
   - Evidence: probe with FakeLiveConnector, both stations publishing `[]` → `ConstraintException: Duplicate key "path: live/departures/ingest_date=2026-10-18/060000_60b2d044_0643b8371301.parquet"`; run status failed. All files in a run share `ingested_at=now`; name = HHMMSS + run_id[:8] + hash[:12]; `is_unchanged` excludes current run, so second identical batch is written to same path (os.replace overwrites) then PK insert fails.
   - Failure scenario: Elia plan with multiple empty windows (forecast horizon beyond data, or windows before a stream's real start), iRail station with empty board → every run fails, watermark never advances.
   - Suggested fix: include a per-request discriminator in the file name (e.g. sha256(request_key)[:8] or request index), and/or skip writing 0-row tables. Add a test in tests/test_connector_base.py with two empty results in one run.
2. [severity: medium] [correctness] Exceptions before the try leave runs stuck at 'running'
   - Where: src/toeval/connectors/base.py:155 (start_run), 169-176 (HttpClient construction), 177 (try), 207 (`except Exception`)
   - Evidence: probe with ApiKeyAuth env var unset → `MissingSecretError` escapes run(); runs table shows `('running', 1)`.
   - Failure scenario: Telraam without TOEVAL_ key, or KeyboardInterrupt/SystemExit mid-run → orphan 'running' rows; later reporting/`run-due` logic that reads runs sees phantom in-progress runs.
   - Suggested fix: construct HttpClient inside try; catch BaseException for failure bookkeeping then re-raise non-Exception; or resolve auth before start_run. Test: missing secret ⇒ run row status 'failed'.
3. [severity: medium] [correctness] Quarantine files live under raw root
   - Where: src/toeval/warehouse/raw.py:77,91
   - Evidence: path = `<data>/raw/_quarantine/<source>/<stream>/ingest_date=.../*.parquet`.
   - Failure scenario: staging reading `raw/**/*.parquet` (or `raw/*/*/...` with hive partitioning) ingests rejected batches (e.g. duplicate keys) into staging.
   - Suggested fix: add `Settings.quarantine_path` (= data/quarantine) and pass it to RawWriter; test asserts quarantine path is not under raw_path.
4. [severity: medium] [doc-drift] CLAUDE.md and ADR 0004 contradict code (see Source-of-truth conflicts 1–2). Agents follow CLAUDE.md:45.
5. [severity: low] [correctness/docs] First-window request key changes whenever the watermark moves
   - Where: src/toeval/connectors/planning.py:59,66-70
   - Evidence: `cursor = start` (unaligned) → key `f"{start}/{boundary}"`.
   - Failure scenario: daily Elia runs never dedup the first window; raw grows by one redundant file per run per stream; conditional-GET validators keyed by it would never match.
   - Suggested fix: floor `start` to a fine grid (e.g. day, anchored at origin) before chunking, or document the limitation; add a test where watermark advances by <1 day and the key set is stable.
6. [severity: low] [robustness] Retry-After uncapped
   - Where: src/toeval/connectors/http.py:99-111,190
   - Failure scenario: `Retry-After: 86400` → process sleeps a day holding the lock; lock becomes stale after 6h (lock.py:21) → second writer.
   - Suggested fix: `min(delay, backoff_max)` or raise HttpError when Retry-After exceeds a threshold.
7. [severity: low] [concurrency] FileLock takeover race and unconditional release
   - Where: src/toeval/lock.py:31-41,48-51
   - Failure scenario: two processes both see a stale lock; A unlinks+creates, B unlinks A's fresh lock and creates its own → two holders. Also `stat()` at line 32 can raise FileNotFoundError if the holder releases between open and stat.
   - Suggested fix: verify pid/contents before unlink and on release; catch FileNotFoundError and retry. Low priority (single Task Scheduler entry).
8. [severity: low] [hygiene] No CI; `pytest -m live` exits 5 with no live tests; `.env.example` lacks TOEVAL_SOURCES_FILE/TOEVAL_USER_AGENT; `setup_logging` (log.py) untested and unwired.

## Not yet built (per roadmap)
1. Elia connector (ods001, ods031, ods032) using `/exports/parquet?where=...`, `next_watermark` = max datetime with non-null measured value; validate() hierarchy check (ADR 0003).
2. Staging SQL (`.sql` files): typed, UTC, latest `_ingested_at` per natural key; exclude quarantine.
3. `revision_lag` logging (ADR 0002 §3).
4. One mart (e.g. national load/solar/wind at 15 min/hourly), wind `parts_present`.
5. Fuller quality checks (schema, nulls, gaps, duplicates) with logged results.
6. CLI: `ingest`/`backfill`, `transform`, `run-due` (takes FileLock, calls setup_logging, applies sources.toml overrides).
7. Windows Task Scheduler job (`run-due` every 5 min).
8. Full README (architecture, Spark/cloud trade-off, how to run).

## Recommended next tasks
1. Fix raw path collision. Files: src/toeval/warehouse/raw.py, tests/test_connector_base.py. Accept: new test with two empty batches in one run succeeds; existing 57 tests pass; ruff clean.
2. Make run bookkeeping exception-safe. Files: src/toeval/connectors/base.py, tests/test_connector_base.py. Accept: missing-secret test ⇒ result/exception handled and runs row status 'failed', no 'running' rows remain.
3. Move quarantine out of raw. Files: src/toeval/config.py (add quarantine_path), src/toeval/warehouse/raw.py, src/toeval/connectors/base.py, test. Accept: quarantine file path not under settings.raw_path; update CLAUDE.md raw section.
4. Sync docs. Files: CLAUDE.md:45-46,56; docs/decisions/0004-connector-interface.md:41. Accept: no contradiction with base.py run order and planning formula.
5. (Optional) Stabilise first-window key in planning.py; add test for advancing watermark.
6. Build Elia connector per CLAUDE.md "Elia API facts" with recorded fixtures + httpx.MockTransport; one `@pytest.mark.live` smoke test. Verify API facts live before relying on them.
Each step: small commit, push to origin (CLAUDE.md:81).

## Conventions an agent must follow here
- Ask clarifying questions and propose a plan before writing code; small tested steps; commit + push each step; flag bad ideas (CLAUDE.md:78-82).
- Do not scaffold later-phase folders/code (CLAUDE.md:40).
- Type hints everywhere; ruff config in pyproject.toml (line-length 100, rules E,F,W,I,B,UP,N,SIM,RUF,DTZ,PT); standard `logging` with module-level `log = logging.getLogger(__name__)` (observed).
- Tests never hit the network unless `@pytest.mark.live`; use `httpx.MockTransport`, fake `clock`, `http_options={"sleep": ...}` (CLAUDE.md:60,74). New base-class behaviour must be tested against all three fakes in tests/fakes.py (CLAUDE.md:51).
- Config only via `.env` with `TOEVAL_` prefix; secrets referenced by env var name, never in config; never commit `.env` (CLAUDE.md:73,53).
- Timestamps: aware UTC in code, naive UTC in DuckDB state (state.py:4); DTZ ruff rules enforce aware datetimes.
- SQL transformations live in `.sql` files (ADR 0001). Raw is immutable; staging/marts rebuildable.
- One ADR per significant decision in docs/decisions/NNNN-title.md; keep CLAUDE.md updated and only document commands that work (CLAUDE.md:7,75).
- Verify data source endpoints/terms/limits yourself (CLAUDE.md:76).
- Pre-commit runs ruff via `uv run` (.pre-commit-config.yaml:13-25).

## Coverage of this analysis
- Read fully: all docs (CLAUDE.md, README, kickoff brief, ADRs 0001–0004), pyproject.toml, configs, every file in src/ (11 files), tests/conftest.py, tests/fakes.py, tests/test_planning.py.
- Sampled: other test files by test name only (test_connector_base, test_http, test_spec, test_state, test_lock, test_cli, test_config).
- Probed: findings 1 and 2 reproduced with a scratch script against the fakes in a temp data dir (deleted afterwards).
- Not run: live API tests (none exist); no network checks of Elia endpoints.
