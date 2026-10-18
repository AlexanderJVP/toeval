# 0001. Local DuckDB + Parquet warehouse in raw / staging / marts layers

- Status: accepted
- Date: 2026-10-18

## Context

The platform ingests a few public datasets (Elia first). Total volume is at most a few GB for years of 15-minute data. It runs on one Windows laptop. The warehouse must be reusable by later projects, and it should be possible to move it to Spark or a cloud warehouse later without a rewrite.

## Decision

- **Storage:** Parquet files under `TOEVAL_DATA_DIR`. **Compute:** DuckDB.
- **Layers:**
  - `raw`: one immutable Parquet file per fetch, exactly as received, partitioned by source/dataset/ingestion time. Never modified or deleted by the pipeline.
  - `staging`: typed, UTC timestamps, deduplicated to the latest ingested version of each natural key. Fully rebuildable from raw.
  - `marts`: aligned time grains (15 min, hourly, daily, weekly). Fully rebuildable from staging.
- Transformations are plain SQL files, so they run unchanged on any engine with SQL over Parquet.

## Why not Spark / a cloud warehouse

Spark adds a JVM, cluster semantics and seconds of startup to process data that DuckDB handles in milliseconds on one core. A cloud warehouse adds cost, credentials and a network dependency for a personal project. Neither earns its complexity at this scale.

## Keeping a migration possible

- Parquet is the storage format for raw data. Spark, BigQuery, Snowflake and Databricks all read it directly.
- Hive-style partitions (`key=value/`) are understood by all of them.
- SQL lives in `.sql` files rather than embedded in Python, so they port with only dialect fixes.
- Because raw is immutable and staging/marts are derived, a migration only needs to copy raw and re-run the SQL.

## Consequences

- Single writer only: two concurrent runs could conflict on the DuckDB file. The scheduler runs one job at a time, and the pipeline takes a lock file.
- Raw grows with every re-fetch (see 0002). At this scale that is fine. Compaction can be added later.
