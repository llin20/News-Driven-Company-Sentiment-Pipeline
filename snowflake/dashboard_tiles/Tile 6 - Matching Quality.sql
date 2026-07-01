USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    COUNT(DISTINCT a.article_id) AS total_articles,
    COUNT(DISTINCT m.article_id) AS matched_articles,
    COUNT(DISTINCT a.article_id) - COUNT(DISTINCT m.article_id) AS unmatched_articles
FROM raw_articles a
LEFT JOIN fact_article_company_mentions m
  ON a.article_id = m.article_id;