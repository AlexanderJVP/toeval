# 0007. A local admin console that never becomes a second writer

- Status: accepted (all 4 steps built: status page, live run feed, actions, schedule editor)
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
- **Security:** binding to 127.0.0.1 keeps other machines out. `TrustedHostMiddleware` rejects other `Host` headers, so a malicious web page can't read the API through DNS rebinding. Every action (POST) needs the per-session token that `toeval ui` generates and embeds in the page, sent as `X-Toeval-Token`; another site can't read the page, so it can't know the token, and the custom header forces a CORS preflight the app never approves. A foreign `Origin` header is refused too. A page left open from an earlier console session gets "reload the page".
- **Actions (step 3):**
  - *Fetch now* and *Rebuild now* start `toeval ingest <source> <stream>` and `toeval transform` as separate processes (`ui/actions.py`). They take the ingest lock, log and emit events like any run. Their output goes to `data/logs/console/` (last 50 kept) and the page shows the tail and the exit code. They run in their own process group, so stopping the console doesn't stop a fetch halfway.
  - One job at a time, and none while the lock is held by a live process (409). A scheduled run can still take the lock first; the job then exits 3 and the page says so.
  - *Pause / resume* (the "break connection" request) sets or removes `paused = true` under `[<source>.streams.<stream>]` in `sources.toml`. `run-due` skips paused streams; explicit `ingest` and `backfill` still run them, because asking for a run by name is a clear intent. Paused streams are never "overdue", but show as a warning, since their data goes stale. The same edit is available as `toeval pause/resume`.
  - Edits to `sources.toml` keep comments and formatting (tomlkit), are validated exactly like a run validates the file (`pipeline.resolve_specs`) before anything is written, and replace the file atomically, so a run never reads a half-written or invalid file.
- **Schedule editor (step 4):** a dialog per stream edits its `schedule` (every N minutes/hours, daily at a Brussels time, or monthly on day 1–28) and, for streams that have one, its `revision_window` in whole days. It posts the full desired values to `/api/actions/schedule`, which writes them to `[<source>.streams.<stream>]` with the same validated, atomic edit as pausing.
  - A value equal to the connector's default **removes** the override instead of writing it, so `sources.toml` only holds real deviations and a later change of the default in code still applies. "Use defaults" refills the form with them.
  - Intervals under 5 minutes are refused: `run-due` only ticks every 5 minutes, so a shorter interval would only look shorter. Days above 28 are refused, as by `MonthlySchedule`, so the day exists in every month.
  - The new schedule takes effect at the next `run-due` tick: `next_due` is computed from the last success and the schedule as it is then. Deep re-fetch settings and the HTTP policy stay file-only; they are rarely changed, and a form for every knob would hide the few that matter.
- **The console logs to its terminal only.** Holding `data/logs/toeval.log` open in the long-running console process would make the pipeline's log rotation fail on Windows.
- **Live progress** can't come from the state DB, which a run holds for its whole duration. The process holding the ingest lock appends **progress events** to `data/state/events.jsonl` (`toeval/events.py`): `run_started`, `planned`, `request_started`, `request_done` (rows, and whether the batch was stored, unchanged, not modified or quarantined), `run_finished`, `transform_started`, `model_built`, `transform_finished`, and `idle` once the state DB is closed. The console tails the file (`/api/events?cursor=`) every second.
  - Events are informational: the state DB stays the source of truth, and a failure to write an event never fails a run.
  - Only the lock holder writes, so there is one writer. A `run-due` tick with nothing due writes nothing.
  - Each file starts with a random `epoch`; a cursor is `<epoch>:<offset>`. At 1 MB the next process renames the file to `events.1.jsonl`, and a reader finishes the old file before moving to the new one. Only complete lines are read, since the writer may be mid-line.
  - **Animations only reflect real events.** Particles travel when a request returned rows (their number grows with the log of the row count), fade halfway when the batch was unchanged and not stored, and turn red when it was quarantined; a pipe is animated while a request is really in flight. Events that happened before the page was opened are listed, never animated. Nothing moves while nothing runs.
- **The Task Scheduler state** comes from PowerShell (`ui/scheduler.py`), which takes about 2 s. It is cached for 30 s and refreshed in a background thread, so no request waits for it.

## Planned steps

1. Read-only status page (built).
2. Live run feed (built): progress events, a data-flow diagram and a feed.
3. Actions (built): fetch now, pause/resume a stream, rebuild.
4. Schedule editor (built): schedule and revision window per stream, written to `sources.toml`.

"Add stream" is limited to what configuration can express: a new *source* needs a connector, because every API differs.

## Consequences

- The UI can't corrupt the warehouse, and the scheduled task works the same whether the UI is open or not.
- During a long run (the first full load takes ~27 minutes), the status sections show the state from before the run; the live diagram and feed show its progress.
- On Windows, a running `toeval ui` keeps `.venv\Scripts\toeval.exe` open, so `uv sync` (or a `uv run` that reinstalls after a `pyproject.toml` change) fails until the UI is stopped. `uv run python -m toeval ui` avoids holding the executable.
