USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- 09_create_unified_realtime_reporting.sql
--
-- Purpose:
--   Build dashboard-facing unified reporting objects.
--   These objects combine:
--     1) legacy NewsAPI / historical project data
--     2) realtime Spark-loaded canonical base data
--
-- Design choice:
--   Keep the core reporting lineage as direct dynamic-table dependencies.
--   This avoids the unsupported pattern:
--     dynamic table -> view -> dynamic table
--   and preserves normal dynamic-table pipeline behavior.
--
-- Safe to rerun: YES
-- =========================================================


-- ---------------------------------------------------------------------
-- 1) Canonical realtime rows from the append-only base table.
--    One row per (event_id, company_id), keeping the latest loaded row.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE dt_realtime_article_company_match
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    event_id,
    provider,
    provider_article_id,
    url,
    url_hash,
    published_at_ts,
    event_ingested_at_ts,
    company_id,
    sentiment_score,
    source_name,
    article_text,
    ingest_batch_id,
    loaded_at
FROM article_company_match_base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, company_id
    ORDER BY loaded_at DESC
) = 1;


-- ---------------------------------------------------------------------
-- 2) Legacy compatibility layer.
--    This keeps already-loaded NewsAPI / historical warehouse data
--    compatible with the unified reporting grain.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_legacy_article_company_mentions AS
WITH sentiment_latest AS (
    SELECT
        article_id,
        company_id,
        sentiment_score,
        sentiment_label,
        model_name,
        ROW_NUMBER() OVER (
            PARTITION BY article_id, company_id
            ORDER BY scored_at DESC
        ) AS rn
    FROM fact_article_sentiment
)
SELECT
    CAST(a.article_id AS STRING) AS record_id,
    COALESCE(a.provider, 'newsapi') AS provider,
    a.source_name,
    a.url,
    COALESCE(a.url_hash, SHA2(LOWER(TRIM(a.url)), 256)) AS canonical_url_hash,
    a.published_at AS published_at_ts,
    m.company_id,
    COALESCE(s.sentiment_score, 0.0) AS sentiment_score,
    COALESCE(s.sentiment_label, 'neutral') AS sentiment_label,
    LEFT(
        TRIM(
            COALESCE(a.title, '') || ' ' ||
            COALESCE(a.description, '') || ' ' ||
            COALESCE(a.content, '')
        ),
        5000
    ) AS article_text,
    a.ingested_at AS loaded_at,
    COALESCE(s.model_name, 'legacy_unknown') AS sentiment_model,
    'legacy' AS ingest_path
FROM raw_articles a
JOIN fact_article_company_mentions m
  ON a.article_id = m.article_id
LEFT JOIN sentiment_latest s
  ON s.article_id = m.article_id
 AND s.company_id = m.company_id
 AND s.rn = 1;


-- ---------------------------------------------------------------------
-- 3) Unified reporting grain.
--    IMPORTANT:
--      This reads DIRECTLY from dt_realtime_article_company_match.
--      It does NOT read from a view on top of a dynamic table.
--      That keeps the dependency graph professional and supported.
--
--    Deduplication rule:
--      one row per (canonical_url_hash, company_id), keeping newest load.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE dt_unified_article_company_mentions
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
WITH realtime_rows AS (
    SELECT
        event_id AS record_id,
        COALESCE(provider, 'gdelt') AS provider,
        source_name,
        url,
        COALESCE(url_hash, SHA2(LOWER(TRIM(url)), 256)) AS canonical_url_hash,
        published_at_ts,
        company_id,
        COALESCE(sentiment_score, 0.0) AS sentiment_score,
        CASE
            WHEN sentiment_score >= 0.05 THEN 'positive'
            WHEN sentiment_score <= -0.05 THEN 'negative'
            ELSE 'neutral'
        END AS sentiment_label,
        article_text,
        loaded_at,
        'vader_v1' AS sentiment_model,
        'realtime' AS ingest_path
    FROM dt_realtime_article_company_match
),
unified AS (
    SELECT * FROM vw_legacy_article_company_mentions
    UNION ALL
    SELECT * FROM realtime_rows
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY canonical_url_hash, company_id
            ORDER BY loaded_at DESC
        ) AS rn
    FROM unified
)
SELECT
    record_id,
    provider,
    source_name,
    url,
    canonical_url_hash,
    published_at_ts,
    company_id,
    sentiment_score,
    sentiment_label,
    article_text,
    loaded_at,
    sentiment_model,
    ingest_path
FROM ranked
WHERE rn = 1;


-- ---------------------------------------------------------------------
-- 4) Dashboard-facing dynamic tables.
-- ---------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE rpt_company_article_volume
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    c.company_id,
    c.ticker,
    c.company_name,
    COUNT(*) AS article_count,
    MAX(u.loaded_at) AS refreshed_at
FROM dt_unified_article_company_mentions u
JOIN dim_companies c
  ON u.company_id = c.company_id
GROUP BY 1, 2, 3;


CREATE OR REPLACE DYNAMIC TABLE rpt_company_sentiment_summary
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    c.company_id,
    c.ticker,
    c.company_name,
    COUNT(*) AS scored_articles,
    ROUND(AVG(u.sentiment_score), 3) AS avg_sentiment,
    COUNT_IF(u.sentiment_label = 'positive') AS positive_count,
    COUNT_IF(u.sentiment_label = 'neutral') AS neutral_count,
    COUNT_IF(u.sentiment_label = 'negative') AS negative_count,
    MAX(u.loaded_at) AS refreshed_at
FROM dt_unified_article_company_mentions u
JOIN dim_companies c
  ON u.company_id = c.company_id
GROUP BY 1, 2, 3;


CREATE OR REPLACE DYNAMIC TABLE rpt_daily_trend
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    CAST(published_at_ts AS DATE) AS metric_date,
    COUNT(*) AS total_articles,
    ROUND(AVG(sentiment_score), 3) AS avg_sentiment_across_companies,
    MAX(loaded_at) AS refreshed_at
FROM dt_unified_article_company_mentions
GROUP BY 1;


CREATE OR REPLACE DYNAMIC TABLE rpt_sentiment_examples
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    c.ticker,
    c.company_name,
    u.provider,
    u.ingest_path,
    u.sentiment_label,
    u.sentiment_score,
    u.published_at_ts AS published_at,
    u.source_name,
    LEFT(u.article_text, 250) AS title,
    u.url,
    u.sentiment_model AS reasoning,
    u.loaded_at AS refreshed_at
FROM dt_unified_article_company_mentions u
JOIN dim_companies c
  ON u.company_id = c.company_id;


CREATE OR REPLACE DYNAMIC TABLE rpt_top_sources
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MONKEY_WH
AS
SELECT
    COALESCE(source_name, 'UNKNOWN') AS source_name,
    COUNT(*) AS article_count,
    MAX(loaded_at) AS refreshed_at
FROM dt_unified_article_company_mentions
GROUP BY 1;


-- ---------------------------------------------------------------------
-- 5) Optional analyst convenience views.
--    These are safe because they are NOT used as sources for downstream
--    dynamic tables in this file.
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_realtime_article_company_mentions AS
SELECT
    event_id AS record_id,
    COALESCE(provider, 'gdelt') AS provider,
    source_name,
    url,
    COALESCE(url_hash, SHA2(LOWER(TRIM(url)), 256)) AS canonical_url_hash,
    published_at_ts,
    company_id,
    COALESCE(sentiment_score, 0.0) AS sentiment_score,
    CASE
        WHEN sentiment_score >= 0.05 THEN 'positive'
        WHEN sentiment_score <= -0.05 THEN 'negative'
        ELSE 'neutral'
    END AS sentiment_label,
    article_text,
    loaded_at,
    'vader_v1' AS sentiment_model,
    'realtime' AS ingest_path
FROM dt_realtime_article_company_match;


CREATE OR REPLACE VIEW vw_unified_article_company_mentions AS
SELECT *
FROM dt_unified_article_company_mentions;


-- ---------------------------------------------------------------------
-- 6) Quick sanity checks.
-- ---------------------------------------------------------------------

SHOW DYNAMIC TABLES LIKE 'DT_REALTIME_ARTICLE_COMPANY_MATCH';
SHOW DYNAMIC TABLES LIKE 'DT_UNIFIED_ARTICLE_COMPANY_MENTIONS';
SHOW DYNAMIC TABLES LIKE 'RPT_%';

SHOW VIEWS LIKE 'VW_LEGACY_ARTICLE_COMPANY_MENTIONS';
SHOW VIEWS LIKE 'VW_REALTIME_ARTICLE_COMPANY_MENTIONS';
SHOW VIEWS LIKE 'VW_UNIFIED_ARTICLE_COMPANY_MENTIONS';