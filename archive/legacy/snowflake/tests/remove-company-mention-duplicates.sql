USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

CREATE OR REPLACE TEMP TABLE dedup_mentions AS
SELECT *
FROM fact_article_company_mentions
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY article_id, company_id
    ORDER BY created_at DESC
) = 1;

TRUNCATE TABLE fact_article_company_mentions;

INSERT INTO fact_article_company_mentions
SELECT *
FROM dedup_mentions;