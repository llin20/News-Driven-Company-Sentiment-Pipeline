USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- Persistent cursor so each DAG run fetches a different batch
-- Safe to rerun
-- =========================================================

CREATE TABLE IF NOT EXISTS newsapi_fetch_cursor (
    cursor_name STRING,
    cursor_idx NUMBER,
    updated_at TIMESTAMP_NTZ
);

MERGE INTO newsapi_fetch_cursor t
USING (
    SELECT 'newsapi_main' AS cursor_name, 0 AS cursor_idx, CURRENT_TIMESTAMP() AS updated_at
) s
ON t.cursor_name = s.cursor_name
WHEN NOT MATCHED THEN
  INSERT (cursor_name, cursor_idx, updated_at)
  VALUES (s.cursor_name, s.cursor_idx, s.updated_at);

SELECT * FROM newsapi_fetch_cursor;


-- -- Reset cursor
-- UPDATE newsapi_fetch_cursor
-- SET cursor_idx = 0,
--     updated_at = CURRENT_TIMESTAMP()
-- WHERE cursor_name = 'newsapi_main';

-- SELECT * FROM newsapi_fetch_cursor;