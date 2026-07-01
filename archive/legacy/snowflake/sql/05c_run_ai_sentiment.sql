USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- 05c_run_ai_sentiment.sql
--
-- Purpose:
-- Use Snowflake AI_SENTIMENT as the main sentiment method.
--
-- Important:
-- 1) This script removes old sentiment rows from:
--    - rule_based_v1
--    - cortex_sentiment_v1
--    - ai_sentiment_v1
--    so the project uses ONE sentiment model cleanly.
--
-- 2) AI_SENTIMENT returns labels, not numeric scores.
--    We map them to numeric scores for the mart:
--      positive ->  1.00
--      mixed    ->  0.25
--      neutral  ->  0.00
--      unknown  ->  0.00
--      negative -> -1.00
--
-- 3) For presentation simplicity:
--      mixed   is grouped into label = 'neutral'
--      unknown is grouped into label = 'neutral'
--
-- 4) Safe to rerun.
-- =========================================================

BEGIN;

-- Remove older sentiment rows so downstream mart logic stays clean
DELETE FROM fact_article_sentiment
WHERE model_name IN ('rule_based_v1', 'cortex_sentiment_v1', 'ai_sentiment_v1');

-- Insert AI_SENTIMENT results
INSERT INTO fact_article_sentiment (
    article_id,
    company_id,
    model_name,
    sentiment_score,
    sentiment_label,
    reasoning,
    scored_at
)
WITH base AS (
    SELECT
        m.article_id,
        m.company_id,
        TRIM(
            COALESCE(a.title, '') || ' ' ||
            COALESCE(a.description, '') || ' ' ||
            COALESCE(a.content, '')
        ) AS text_blob
    FROM fact_article_company_mentions m
    JOIN raw_articles a
      ON a.article_id = m.article_id
    WHERE TRIM(
            COALESCE(a.title, '') || ' ' ||
            COALESCE(a.description, '') || ' ' ||
            COALESCE(a.content, '')
          ) <> ''
),
scored AS (
    SELECT
        article_id,
        company_id,
        AI_SENTIMENT(text_blob) AS ai_obj
    FROM base
),
overall_only AS (
    SELECT
        s.article_id,
        s.company_id,
        LOWER(f.value:sentiment::string) AS overall_sentiment
    FROM scored s,
         LATERAL FLATTEN(input => s.ai_obj:categories) f
    WHERE LOWER(f.value:name::string) = 'overall'
)
SELECT
    article_id,
    company_id,
    'ai_sentiment_v1' AS model_name,
    CASE overall_sentiment
        WHEN 'positive' THEN  1.00
        WHEN 'mixed'    THEN  0.25
        WHEN 'neutral'  THEN  0.00
        WHEN 'unknown'  THEN  0.00
        WHEN 'negative' THEN -1.00
        ELSE 0.00
    END AS sentiment_score,
    CASE overall_sentiment
        WHEN 'positive' THEN 'positive'
        WHEN 'negative' THEN 'negative'
        ELSE 'neutral'
    END AS sentiment_label,
    'Snowflake AI_SENTIMENT overall category. Raw overall sentiment mapped to project score/label for reporting.' AS reasoning,
    CURRENT_TIMESTAMP() AS scored_at
FROM overall_only;

COMMIT;

-- =========================================================
-- Verification queries
-- =========================================================

SELECT
    sentiment_label,
    COUNT(*) AS row_count,
    ROUND(AVG(sentiment_score), 3) AS avg_score
FROM fact_article_sentiment
WHERE model_name = 'ai_sentiment_v1'
GROUP BY 1
ORDER BY row_count DESC;

SELECT
    COUNT(*) AS total_ai_rows
FROM fact_article_sentiment
WHERE model_name = 'ai_sentiment_v1';

SELECT
    article_id,
    company_id,
    model_name,
    sentiment_score,
    sentiment_label,
    reasoning
FROM fact_article_sentiment
WHERE model_name = 'ai_sentiment_v1'
LIMIT 25;