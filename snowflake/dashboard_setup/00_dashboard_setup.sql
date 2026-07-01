USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- Bootstraps unified dashboard objects.
-- Run once (or re-run) after base schema + realtime base objects are present.

-- This script delegates object creation to the unified dynamic-table setup.
-- After creation, Snowflake auto-refreshes these dynamic tables.
-- In Snowsight you only need dashboard/tile refresh.

-- If your worksheet does not support !source, execute:
--   snowflake/09_create_unified_realtime_reporting.sql

SELECT 'Run snowflake/09_create_unified_realtime_reporting.sql in this worksheet session.' AS next_step;
