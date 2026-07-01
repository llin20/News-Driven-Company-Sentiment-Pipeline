USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    metric_date,
    total_articles,
    avg_sentiment_across_companies,
    refreshed_at
FROM rpt_daily_trend
ORDER BY metric_date ASC;