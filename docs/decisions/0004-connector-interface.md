# 0004. A connector interface that doesn't assume Elia's patterns

- Status: accepted
- Date: 2026-10-18
- Amends: 0002 (fetch range formula, watermark commit point)

## Context

The first source (Elia) is a paginated historical API with revisions. Upcoming sources work differently (checked 2026-10-18):

| | Elia | iRail | Statbel |
|---|---|---|---|
| History | years, queryable by time | **none**: a past-date liveboard returns 0 departures | full series in each file |
| Cadence | daily | **every few minutes** (live only) | **monthly** |
| Progress marker | timestamp | none (each poll is a snapshot) | reference period (`2026-09`) |
| Revisions | yes, lag undocumented | n/a | yes, past periods |
| Limits | 100M calls/day | 3 req/s, burst 5, 429 | unknown, be polite |
| Change detection | none | `ETag` / `If-None-Match` → 304 | probably `Last-Modified` / `ETag` |
| Auth | none | none (User-Agent requested) | none; Telraam needs `X-Api-Key` |

An interface built around "time windows with a timestamp watermark" would force iRail and Statbel to fake it.

## Decision

### Configuration is data: `SourceSpec` → `StreamSpec`

A **source** is one provider (Elia). A **stream** is one dataset from it (ods001). Each stream has its own state.

- `SourceSpec`: `name`, `auth`, `http` (rate limit, retry/backoff, timeout, user agent), `streams`.
- `StreamSpec`: `name`, `schedule`, `watermark` kind, `revision_window` (or none), `deep_refetch` (or none), `backfill_start` (none means live-only, so backfill is refused), `natural_key`.
- `auth`: `none` | `api_key` (header or query param) | `bearer`. Each names an **env var**, and the secret is read at runtime, never stored in config. A missing secret fails before any request is sent.
- `schedule`: `interval` (`every: 5m`) | `daily` (`at: 06:30`) | `monthly` (`day: 5, at: 09:00`). Times are Europe/Brussels. `is_due(last_success, now)` means the next scheduled time after the last success has passed, so a laptop that was off runs on wake-up.
- `watermark`: `timestamp` | `period` | `none`.
- Defaults live in code next to each connector (they are facts about the source). Operational settings (schedule, windows, rate) can be overridden per source/stream in `sources.toml` without touching code.

### The base class owns everything that isn't source-specific

`Connector` (abstract) implements `run(stream, mode)` as a template method:

```
load state → plan() → for each request: fetch() via http → generic checks + validate()
     → (fail: quarantine batch outside raw, run fails) → write raw → fold next_watermark()
     → only if every request succeeded: commit watermark, last_success, HTTP validators
```

Validation runs *before* the raw write, so raw never holds a batch that failed a hard check.
The single-writer lock is taken by the CLI (`run-due`, `ingest`) around all streams, not inside
`run()`.

It owns the `HttpClient`, which provides:
- **Rate limiting:** a token bucket per source (`requests_per_second`, `burst`). As built it is per stream run; sharing it per source is deferred to iRail (ADR 0005).
- **Retries:** on connection errors, timeouts, 429 and 5xx, with exponential backoff, full jitter and a cap. `Retry-After` is honoured. Other 4xx errors fail immediately (retrying a 404 just wastes requests).
- **Auth** injection and a project User-Agent.
- **Conditional GET:** `ETag` / `Last-Modified` per request key, stored in state. A 304 means "nothing new", which isn't an error. This covers iRail polling and Statbel's monthly checks without connector code.

### Connectors implement only three source-specific methods

- `plan(stream, state, ctx) -> list[FetchRequest]`: what to fetch. Elia returns time windows, iRail returns one request per station, Statbel returns one request per file.
- `fetch(stream, request, http) -> FetchResult`: turns a response into an Arrow table (or "not modified").
- `next_watermark(stream, state, table) -> Watermark | None`: Elia uses the max timestamp with a measured value, Statbel uses the max period, iRail has no watermark.

Optional hook: `validate(stream, table)` for source-specific quality checks (e.g. Belgium = sum of regions). Generic checks run for every stream.

Time-window planning is a **helper function** (`time_window_plan`) that time-series connectors call. It is not part of the base class.

### Fetch range for time-window streams (amends 0002)

`start = watermark − revision_window` (or `backfill_start` on the first run), `end = now + forecast_horizon`, chunked. 0002's `min(watermark, now − window)` missed re-fetching the window *before* an old watermark after missed runs.

### Ingestion is decoupled from transformation (amends 0002)

The watermark advances after raw writes and quality checks succeed. Staging and marts are rebuilt by a separate `transform` step, because they are pure functions of raw. This keeps a 5-minute iRail poll cheap. The scheduler runs `toeval run-due`, which ingests all due streams and then transforms whatever changed.

## Consequences

- One scheduler entry (`run-due` every 5 minutes) serves every cadence. Polls that aren't due exit in milliseconds. Retry backoff for failing streams and the Task Scheduler setup: ADR 0005.
- **Retractions are invisible.** Empty batches are not written, so if a source withdraws rows (a window that returned rows now returns none), raw keeps the old rows as their latest version and staging keeps them too. None of the Phase 1 sources is known to withdraw rows. If one does, record an empty-batch marker per request key in `raw_files` and let staging treat the latest file per request key as authoritative for its range. Staging must never assume that a missing raw file for a range means "no data".
- Live-only streams have permanent gaps when the laptop is off. That is a source property, not a bug. Coverage gets logged so the gaps are visible.
- **Open issue (Phase 2):** iRail at 5-minute polling makes about 100k small raw files per year. Options: one file per run covering all stations (already planned), and later an append-only daily rollup layer between raw and staging. Decide when we build it.
