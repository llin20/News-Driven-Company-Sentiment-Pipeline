USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    article_id,
    company_id,
    COUNT(*) AS row_count
FROM fact_article_company_mentions
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY row_count DESC, article_id, company_id;