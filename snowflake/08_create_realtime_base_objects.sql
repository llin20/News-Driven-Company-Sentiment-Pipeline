USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- 08_create_realtime_base_objects.sql
--
-- Purpose:
--   Create canonical append-only realtime base objects used by
--   the Kafka -> Spark -> Snowflake streaming pipeline.
--
-- Notes:
--   - Safe to rerun.
--   - Keeps existing realtime tables intact.
--   - Does not change legacy Airflow/fact-table flow.
-- =========================================================

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
)
COMMENT = 'Canonical append-only base table for streaming article/company sentiment matches';

-- Optional helper view for canonical-key duplicate visibility.
-- Canonical key convention for this table is (event_id, company_id).
CREATE OR REPLACE VIEW v_article_company_match_base_dups AS
SELECT
    event_id,
    company_id,
    COUNT(*) AS duplicate_count,
    MIN(loaded_at) AS first_loaded_at,
    MAX(loaded_at) AS last_loaded_at
FROM article_company_match_base
GROUP BY 1, 2
HAVING COUNT(*) > 1;

-- Quick sanity output
SHOW TABLES LIKE 'ARTICLE_COMPANY_MATCH_BASE' IN SCHEMA MONKEY_DB.FINAL_PROJECT;
SHOW VIEWS LIKE 'V_ARTICLE_COMPANY_MATCH_BASE_DUPS' IN SCHEMA MONKEY_DB.FINAL_PROJECT;
