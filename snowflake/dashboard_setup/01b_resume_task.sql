USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- No scheduled task required; dynamic tables refresh automatically.
SELECT 'No-op: dynamic tables auto-refresh. Use ALTER DYNAMIC TABLE ... REFRESH for manual run.' AS info;
