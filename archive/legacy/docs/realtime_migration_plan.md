# Realtime pipeline status and operating model (April 2026)

## Current state on main

Implemented and preserved:
- LinuxLab Kafka startup/check scripts under `streaming/scripts/*`.
- Spark Structured Streaming `kafka -> foreachBatch -> Snowflake connector` ingestion.
- Proven Snowflake connector options: `quote_identifiers=False`, `use_logical_type=True`.
- Canonical append-only base table foundation: `article_company_match_base` + duplicate visibility view.
- GDELT backfill and live producer paths; smoke producer path.

Completed in this implementation:
- Upgraded Spark sentiment from tiny keyword baseline to VADER.
- Added resilient GDELT backfill behavior (rate limit, 429 handling, Retry-After, jittered backoff, resumable cursor).
- Added Snowflake unified dynamic-table reporting layer combining legacy NewsAPI/old data with realtime stream data.
- Updated runbooks for the 3-terminal LinuxLab session model.

## Final architecture

1. Producer(s) -> Kafka topic `raw_news_articles`
2. Spark Structured Streaming consumes Kafka and appends to Snowflake base sinks
3. Snowflake dynamic tables maintain unified dashboard objects
4. Snowsight dashboards query `rpt_*` dynamic tables

## Why dynamic tables here

Chosen approach: **append-only Spark base writes + Snowflake dynamic tables**.

Reasoning:
- Keeps Spark ingest path simple and durable.
- Removes per-session manual SQL for downstream marts.
- Handles restart/recompute predictably in Snowflake.
- Works with mixed legacy + realtime inputs through one unified reporting grain.

## LinuxLab session checklist

1. `server-airflow25 -c 4`
2. Start Kafka terminal
3. Start Spark terminal
4. Start backfill/live terminal
5. In Snowsight, refresh dashboard tiles only

## Session loss behavior

- Snowflake rows written before failure remain safe.
- Restart Spark with same checkpoint path.
- Restart backfill; cursor checkpoint resumes from last saved window boundary.
- Dynamic tables continue from persisted base tables.
