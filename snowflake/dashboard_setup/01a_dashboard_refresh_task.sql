USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- Dynamic tables now replace manual dashboard refresh tasks.
-- Re-run unified setup if needed.

SELECT
  name,
  text,
  target_lag,
  refresh_mode,
  refresh_mode_reason
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE table_schema = 'FINAL_PROJECT'
  AND name ILIKE 'RPT_%'
ORDER BY name;
