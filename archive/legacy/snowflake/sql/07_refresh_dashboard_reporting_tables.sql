USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- Refresh dashboard reporting tables for presentation
-- Uses ONLY ai_sentiment_v1
-- Safe to rerun
-- =========================================================

CREATE OR REPLACE TABLE rpt_company_article_volume AS
SELECT
    c.company_id,
    c.ticker,
    c.company_name,
    COUNT(DISTINCT m.article_id) AS article_count,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM fact_article_company_mentions m
JOIN dim_companies c
  ON m.company_id = c.company_id
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE rpt_company_sentiment_summary AS
SELECT
    c.company_id,
    c.ticker,
    c.company_name,
    COUNT(*) AS scored_articles,
    ROUND(AVG(s.sentiment_score), 3) AS avg_sentiment,
    COUNT_IF(s.sentiment_label = 'positive') AS positive_count,
    COUNT_IF(s.sentiment_label = 'neutral') AS neutral_count,
    COUNT_IF(s.sentiment_label = 'negative') AS negative_count,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM fact_article_sentiment s
JOIN dim_companies c
  ON s.company_id = c.company_id
WHERE s.model_name = 'ai_sentiment_v1'
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE rpt_daily_trend AS
SELECT
    metric_date,
    SUM(article_count) AS total_articles,
    ROUND(AVG(avg_sentiment), 3) AS avg_sentiment_across_companies,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM mart_company_sentiment_daily
GROUP BY 1;

CREATE OR REPLACE TABLE rpt_sentiment_examples AS
SELECT
    c.ticker,
    c.company_name,
    s.sentiment_label,
    s.sentiment_score,
    a.published_at,
    a.source_name,
    a.title,
    a.url,
    s.reasoning,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM fact_article_sentiment s
JOIN dim_companies c
  ON s.company_id = c.company_id
JOIN raw_articles a
  ON s.article_id = a.article_id
WHERE s.model_name = 'ai_sentiment_v1';

SELECT * FROM rpt_company_article_volume ORDER BY article_count DESC;
SELECT * FROM rpt_company_sentiment_summary ORDER BY scored_articles DESC, ticker;
SELECT * FROM rpt_daily_trend ORDER BY metric_date DESC;
SELECT * FROM rpt_sentiment_examples ORDER BY ABS(sentiment_score) DESC LIMIT 20;