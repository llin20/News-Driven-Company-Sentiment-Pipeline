USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    company_id,
    alias,
    COUNT(*) AS row_count
FROM dim_company_aliases
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY row_count DESC, company_id, alias;