# 0005. Scheduled operation: one `run-due` task, retry backoff, no console

- Status: accepted
- Date: 2026-10-18

## Context

The pipeline runs unattended on a Windows laptop that is regularly off, asleep or on battery. Streams have different cadences (Elia daily; iRail every few minutes; Statbel monthly). ADR 0004 decided on one scheduler entry, `toeval run-due` every 5 minutes, which runs whatever is due.

Due-ness was defined only by the last *success*. A failing stream therefore stayed due and was retried on every tick: for Elia's first run that means re-downloading ten years of history every 5 minutes, writing a quarantine file each time if a quality gate is what fails.

## Decision

- **Retry backoff.** `stream_state` records `consecutive_failures` and `last_failure_at`. A stream is due when its schedule says so **and** `last_failure_at + retry_delay` has passed, with `retry_delay` = 5 min × 2^(failures − 1), capped at 6 h (`src/toeval/pipeline.py`). A successful incremental run resets the counter.
- **Backfills are manual and separate.** A backfill never sets `last_success_at` (it must not make the scheduled run look done), its failure doesn't count towards the backoff, and its success doesn't clear it.
- **Lock contention is normal for `run-due`**: if the previous run is still busy, it logs and exits 0. `ingest` and `backfill` exit 3 instead, because a person is waiting for them.
- **Exit codes:** 0 ok, 1 a stream failed, 2 usage error, 3 lock held.
- **Task Scheduler** (`scripts/scheduled-task.ps1`): runs as the logged-on user (no stored password), via `.venv\Scripts\pythonw.exe -m toeval run-due` so no console window flashes every 5 minutes, with `StartWhenAvailable` (missed starts run on wake-up), `MultipleInstances IgnoreNew` and a 2 h execution limit. Output goes to `data/logs/toeval.log`; `toeval status` shows each stream.
- **`status` reads the state DB read-only.** DuckDB allows one writing process, so while a run is busy `status` says so (exit 3) instead of crashing.

## Consequences

- A broken source costs at most a few requests per 6 hours instead of one full fetch every 5 minutes. Recovery after a fix takes up to 6 h unless someone runs `toeval ingest` by hand, which ignores the backoff.
- Nothing runs while the user is logged off. That's acceptable for a laptop; Elia's revision window catches up afterwards. A "run whether logged on or not" task would need a stored password.
- **Not done yet:** the rate limit is per run (one `HttpClient` per stream run), not per source as ADR 0004 says. For Elia (1 request/s, streams run one after another, each export taking seconds) this makes no difference. Before iRail (3 req/s, burst 5, many short requests), share one token bucket per source across a `run-due` invocation.
