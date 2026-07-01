USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    ticker,
    'positive' AS sentiment_label,
    positive_count AS cnt,
    refreshed_at
FROM rpt_company_sentiment_summary

UNION ALL

SELECT
    ticker,
    'neutral' AS sentiment_label,
    neutral_count AS cnt,
    refreshed_at
FROM rpt_company_sentiment_summary

UNION ALL

SELECT
    ticker,
    'negative' AS sentiment_label,
    negative_count AS cnt,
    refreshed_at
FROM rpt_company_sentiment_summary

ORDER BY ticker, sentiment_label;