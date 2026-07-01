USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    ticker,
    company_name,
    article_count,
    refreshed_at
FROM rpt_company_article_volume
ORDER BY article_count DESC, ticker;