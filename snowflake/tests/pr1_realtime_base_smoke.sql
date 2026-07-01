USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- PR1 acceptance checks: realtime base foundation
-- =========================================================

-- 1) Canonical base table exists
SHOW TABLES LIKE 'ARTICLE_COMPANY_MATCH_BASE' IN SCHEMA MONKEY_DB.FINAL_PROJECT;

-- 2) Canonical base table schema
DESC TABLE MONKEY_DB.FINAL_PROJECT.ARTICLE_COMPANY_MATCH_BASE;

-- 3) Duplicate helper view exists
SHOW VIEWS LIKE 'V_ARTICLE_COMPANY_MATCH_BASE_DUPS' IN SCHEMA MONKEY_DB.FINAL_PROJECT;

-- 4) Existing realtime tables still exist (backward compatibility)
SHOW TABLES LIKE 'ARTICLE_COMPANY_MATCH' IN SCHEMA MONKEY_DB.FINAL_PROJECT;
SHOW TABLES LIKE 'MART_COMPANY_SENTIMENT_MINUTE' IN SCHEMA MONKEY_DB.FINAL_PROJECT;
