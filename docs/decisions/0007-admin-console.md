# 0007. A local admin console that never becomes a second writer

- Status: accepted (steps 1–2 of 4 built: status page, live run feed)
- Date: 2026-10-19

## Context

The owner wants a UI to manage the warehouse: status indicators, a live view of data flowing in, a schedule editor, "fetch now", pausing a stream, and adding streams. The pipeline runs unattended from Task Scheduler (ADR 0005), and DuckDB allows **one writing process** per database file. A UI that writes state directly would compete with scheduled runs for the state DB, and could leave it inconsistent.

This is a private operations tool on the owner's laptop, separate from the planned public dashboard (Phase 5).

## Decision

- **`toeval ui`** starts a FastAPI app (`src/toeval/ui/`) on **127.0.0.1 only** and opens the browser. The frontend is plain HTML/CSS/JS in `ui/static/`, with no Node toolchain or build step, so the repo stays uv-only.
- **The pipeline stays the only writer.** The UI:
  - reads the state DB **read-only**, for the duration of one snapshot;
  - (later steps) changes configuration only through `sources.toml`, which the existing override code validates;
  - (later steps) triggers work only by starting the normal CLI commands (`toeval ingest`, `toeval transform`) as separate processes, so they take the usual lock and log as usual.
- **While a run holds the state DB**, the snapshot endpoint serves the last readable snapshot, marked `state_available: false`, with the lock holder read live. The page says so instead of failing.
- **Health** is computed in Python (`ui/snapshot.py:health`) from the same facts `toeval status` uses:
  - *error*: a stream failing (with its retry time), failed transforms, the scheduled task missing, disabled or failing;
  - *warn*: quality warnings, a stream overdue by more than 30 minutes while nothing runs (the scheduler isn't working);
  - *running*: the lock is held by a live process.
- **Security:** binding to 127.0.0.1 keeps other machines out. `TrustedHostMiddleware` rejects other `Host` headers, so a malicious web page can't read the API through DNS rebinding. Actions (later steps) will also require a per-session token, so a cross-site request can't trigger them.
- **Live progress** can't come from the state DB, which a run holds for its whole duration. The process holding the ingest lock appends **progress events** to `data/state/events.jsonl` (`toeval/events.py`): `run_started`, `planned`, `request_started`, `request_done` (rows, and whether the batch was stored, unchanged, not modified or quarantined), `run_finished`, `transform_started`, `model_built`, `transform_finished`, and `idle` once the state DB is closed. The console tails the file (`/api/events?cursor=`) every second.
  - Events are informational: the state DB stays the source of truth, and a failure to write an event never fails a run.
  - Only the lock holder writes, so there is one writer. A `run-due` tick with nothing due writes nothing.
  - Each file starts with a random `epoch`; a cursor is `<epoch>:<offset>`. At 1 MB the next process renames the file to `events.1.jsonl`, and a reader finishes the old file before moving to the new one. Only complete lines are read, since the writer may be mid-line.
  - **Animations only reflect real events.** Particles travel when a request returned rows (their number grows with the log of the row count), fade halfway when the batch was unchanged and not stored, and turn red when it was quarantined; a pipe is animated while a request is really in flight. Events that happened before the page was opened are listed, never animated. Nothing moves while nothing runs.
- **The Task Scheduler state** comes from PowerShell (`ui/scheduler.py`), which takes about 2 s. It is cached for 30 s and refreshed in a background thread, so no request waits for it.

## Planned steps

1. Read-only status page (built).
2. Live run feed (built): progress events, a data-flow diagram and a feed.
3. Actions: fetch now, pause/resume a stream (`run-due` skips paused streams), run transform.
4. Schedule editor writing `sources.toml`.

"Add stream" is limited to what configuration can express: a new *source* needs a connector, because every API differs.

## Consequences

- The UI can't corrupt the warehouse, and the scheduled task works the same whether the UI is open or not.
- During a long run (the first full load takes ~27 minutes), the status sections show the state from before the run; the live diagram and feed show its progress.
- On Windows, a running `toeval ui` keeps `.venv\Scripts\toeval.exe` open, so `uv sync` (or a `uv run` that reinstalls after a `pyproject.toml` change) fails until the UI is stopped. `uv run python -m toeval ui` avoids holding the executable.
