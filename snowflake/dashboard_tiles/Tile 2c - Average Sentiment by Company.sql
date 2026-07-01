USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    ticker,
    company_name,
    scored_articles,
    avg_sentiment,
    refreshed_at
FROM rpt_company_sentiment_summary
ORDER BY avg_sentiment DESC, ticker;