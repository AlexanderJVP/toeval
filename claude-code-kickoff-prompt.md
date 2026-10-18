# Kickoff prompt for Claude Code

Paste everything below the line into Claude Code, opened in an empty project folder in VS Code.

---

I'm starting a long-running personal portfolio project. I'm an applied informatics student (AI/ML) in Antwerp, aiming for a data engineering role. I want this to look and run like a real production system, not a notebook. Read this whole brief, then **ask me any questions you need before writing code**. After that, set up the foundation and build **Phase 1 only**.

## The big picture (where this is going)

A personal data platform that continuously ingests live public datasets, mostly Belgian and Antwerp-focused, into one local warehouse. On top of it run two things:

1. **Spurious correlation tracker** (the "icebreaker"). It automatically searches across all the datasets for absurd correlations (e.g. Antwerp bike traffic vs. Belgian solar output vs. train delays). Then, week after week, it tracks how those correlations **break down** on new data. The point is to show, with live evidence, why correlation isn't causation. The tone is playful; the statistics must be rigorous (multiple-testing correction, detrending, out-of-sample tracking).
2. **JEPA on unlabeled time series** (the serious ML part). Self-supervised pretraining of a JEPA-style model on raw multivariate time series: mask spans of time and predict their *latent* representation (not raw values). Then fine-tune small heads on tasks with very few labels, such as anomaly detection, train delay prediction or unusual traffic days. It must be **benchmarked against simple baselines trained on the same few labels**. That comparison is the key result.

The warehouse is meant to be reused for future projects, so the ingestion layer must make adding a new source easy.

## Data sources (verify current endpoints, terms and rate limits yourself; don't trust my list blindly)

- **Elia Open Data**: Belgian grid load, solar and wind production, 15-minute resolution, years of history. **Phase 1 source.**
- **Open-Meteo**: weather, free, no key.
- **iRail**: live Belgian train departures and delays.
- **Telraam**: citizen traffic counters in Antwerp (needs an API token).
- **IRCEL-CELINE**: air quality.
- **Antwerp / Statbel open data**: slower-moving city and national stats.

## Technical direction

- **Python**, managed with `uv` (or ask me), with a `pyproject.toml`.
- **Warehouse: DuckDB + Parquet**, in layers: `raw` (immutable, as fetched) → `staging` (cleaned, typed, UTC timestamps) → `marts` (aligned on shared time grains: 15 min, hourly, daily, weekly). Data volume is small, so no Spark. Keep the design such that swapping in Spark or a cloud warehouse later would be possible, and explain that trade-off in the README.
- **One connector per source** behind a common interface (fetch, validate, write raw, incremental by watermark, idempotent re-runs, backfill support).
- **Data quality checks** on each load (schema, nulls, gaps, duplicates), with failures logged clearly.
- **Scheduling:** incremental ingestion on a schedule (daily or weekly per source). Ask me which OS I'm on and propose the simplest reliable option (Windows Task Scheduler / cron, or GitHub Actions). Not an orchestration monster.
- **ML (later phases):** PyTorch, experiment tracking (MLflow or similar), reproducible configs, fixed seeds, time-based train/val/test splits only (no leakage).
- **Quality:** type hints, `ruff`, `pytest`, pre-commit, logging, config via `.env` (never commit secrets), Git from the first commit with clear commit messages.

## Phases

1. **Foundation + Elia.** Repo structure, tooling, the connector interface, the Elia connector with a historical backfill and incremental loads, raw → staging → one mart, quality checks, tests, a scheduled job, a README explaining architecture and how to run it.
2. **More sources.** Open-Meteo, iRail, Telraam, IRCEL. Aligned time grains in the marts.
3. **Correlation tracker.** Weekly job that scans for correlations, applies proper statistics, logs them, and tracks their decay over time. Simple output (a static report or small dashboard).
4. **JEPA.** Time-series JEPA pretraining on the marts, few-label downstream tasks, baselines (e.g. logistic regression / gradient boosting on handcrafted features, a supervised model from scratch), honest comparison with results in the README.
5. **Presentation.** Public-facing dashboard or report I can link from LinkedIn and my portfolio site.

## How I want you to work

- Ask questions first, then propose the folder structure and Phase 1 plan before writing code.
- Create a `CLAUDE.md` in the repo that captures this vision, the architecture decisions and the conventions, so future sessions start with full context. Keep it updated as we go.
- Keep a short `docs/decisions/` log (one file per significant decision, with the why).
- Build in small, working, tested steps. Don't scaffold empty folders for Phase 4 now.
- Be critical: if something in this brief is a bad idea, say so before building it.
