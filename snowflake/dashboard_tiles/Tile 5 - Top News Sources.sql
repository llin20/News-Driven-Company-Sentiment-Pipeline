USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    source_name,
    article_count,
    refreshed_at
FROM rpt_top_sources
ORDER BY article_count DESC
LIMIT 15;
