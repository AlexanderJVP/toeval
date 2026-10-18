# 0002. Incremental loads use a watermark plus a measured revision window

- Status: accepted, amended by 0004 (fetch range formula; watermark no longer waits for staging). Window size is provisional and will be tuned from logged data.
- Date: 2026-10-18

## Context

Elia revises published data: measured values arrive and get corrected after the quarter-hour, and forecast columns extend about a week into the future with the measured column still null. A naive "fetch everything after the max timestamp" watermark would freeze early, incomplete values forever.

Elia does not document how far back revisions can happen (checked dataset descriptions and portal on 2026-10-18). The wind dataset notes known offshore deltas for 2018–2023, which shows that even old history can change.

The ingestion runs on a laptop via Task Scheduler, so runs will be missed when it is off.

## Decision

1. **Fetch range** = `watermark - revision_window` → `now + forecast_horizon`. After any number of missed runs, the next run catches up automatically and still re-checks the window before the old watermark. A test covers this. (Originally `min(watermark, now - revision_window)`, corrected in 0004.)
2. **Default revision window: 14 days.** Monthly, a **deep re-fetch of 90 days**.
3. **Measure instead of guessing:** each run compares re-fetched rows with the current staging values and logs the age of the oldest changed row (`revision_lag`). Window sizes get tuned from these logs after a few weeks.
4. **Watermark** = latest timestamp with a non-null *measured* value, not the latest row (forecast rows run ahead). It is updated only after the raw writes and quality checks succeed. (Originally it also waited for the staging rebuild. 0004 decouples transformation from ingestion.)
5. **Idempotency:** a fetch whose content hash equals the previous fetch for the same range is not written again. Staging keeps the latest ingested version per natural key, so duplicate fetches never duplicate rows.
6. **Backfill** uses the same code path, chunked per dataset so one export stays well under the HTTP timeout: 365 days for load, 182 for wind, 91 for solar (exports of the multi-row datasets are slow; `src/toeval/connectors/elia.py`). (Originally "chunked by year".)

## Consequences

- Each scheduled run re-downloads 14+ days per dataset (a few hundred KB). Negligible.
- Raw holds several versions of recent rows. That is intended: it is the evidence for how Elia revises data.
