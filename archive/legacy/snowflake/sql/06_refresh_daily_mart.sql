-- =====================================================================
-- 06_refresh_daily_mart.sql
-- Purpose:
--   Refresh the daily aggregate reporting table.
-- Safe to re-run: YES
-- Notes:
--   The mart is the reporting layer used for quick analysis / screenshots.
-- =====================================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

MERGE INTO mart_company_sentiment_daily tgt
USING (
    SELECT
        CAST(a.published_at AS DATE) AS metric_date,
        m.company_id,
        COUNT(DISTINCT a.article_id) AS article_count,
        AVG(s.sentiment_score) AS avg_sentiment,
        COUNT_IF(s.sentiment_label = 'positive') AS positive_count,
        COUNT_IF(s.sentiment_label = 'neutral') AS neutral_count,
        COUNT_IF(s.sentiment_label = 'negative') AS negative_count,
        CURRENT_TIMESTAMP() AS updated_at
    FROM raw_articles a
    JOIN fact_article_company_mentions m
      ON a.article_id = m.article_id
    LEFT JOIN fact_article_sentiment s
      ON a.article_id = s.article_id
     AND m.company_id = s.company_id
     AND s.model_name = 'ai_sentiment_v1'
    WHERE a.published_at IS NOT NULL
    GROUP BY 1, 2
) src
ON tgt.metric_date = src.metric_date
AND tgt.company_id = src.company_id
WHEN MATCHED THEN UPDATE SET
    article_count   = src.article_count,
    avg_sentiment   = src.avg_sentiment,
    positive_count  = src.positive_count,
    neutral_count   = src.neutral_count,
    negative_count  = src.negative_count,
    updated_at      = src.updated_at
WHEN NOT MATCHED THEN INSERT (
    metric_date,
    company_id,
    article_count,
    avg_sentiment,
    positive_count,
    neutral_count,
    negative_count,
    updated_at
) VALUES (
    src.metric_date,
    src.company_id,
    src.article_count,
    src.avg_sentiment,
    src.positive_count,
    src.neutral_count,
    src.negative_count,
    src.updated_at
);