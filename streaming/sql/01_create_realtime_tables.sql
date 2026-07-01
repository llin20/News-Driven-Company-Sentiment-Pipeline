USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

CREATE TABLE IF NOT EXISTS article_stream_events (
    event_id STRING,
    provider STRING,
    provider_article_id STRING,
    published_at TIMESTAMP_NTZ,
    source_name STRING,
    title STRING,
    description STRING,
    content STRING,
    url STRING,
    url_hash STRING,
    author STRING,
    language STRING,
    country STRING,
    raw_json VARIANT,
    event_ingested_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS article_company_match (
    event_id STRING,
    url_hash STRING,
    published_at_ts TIMESTAMP_NTZ,
    company_id NUMBER,
    sentiment_score FLOAT,
    source_name STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Canonical append-only base sink for realtime streaming writes.
-- Canonical-key convention: (event_id, company_id)
CREATE TABLE IF NOT EXISTS article_company_match_base (
    event_id STRING,
    provider STRING,
    provider_article_id STRING,
    url STRING,
    url_hash STRING,
    published_at_ts TIMESTAMP_NTZ,
    event_ingested_at_ts TIMESTAMP_NTZ,
    company_id NUMBER,
    sentiment_score FLOAT,
    source_name STRING,
    article_text STRING,
    ingest_batch_id STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    comment STRING
);

CREATE TABLE IF NOT EXISTS mart_company_sentiment_minute (
    bucket_minute TIMESTAMP_NTZ,
    company_id NUMBER,
    article_count NUMBER,
    avg_sentiment FLOAT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS mart_company_sentiment_hour (
    bucket_hour TIMESTAMP_NTZ,
    company_id NUMBER,
    article_count NUMBER,
    avg_sentiment FLOAT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
