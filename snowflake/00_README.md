# Snowflake SQL run order (active workflow)

Use this order for a fresh Snowflake setup.

1. `01_setup_schema_objects.sql`
2. `02_seed_companies_and_aliases.sql`
3. `02b_expand_companies.sql` (optional alias expansion)
4. `streaming/sql/01_create_realtime_tables.sql`
5. `08_create_realtime_base_objects.sql`
6. `09_create_unified_realtime_reporting.sql`

## Dashboard refresh workflow

Dynamic tables are the reporting layer. For manual refresh or refresh checks:

- `dashboard_setup/01c_start_task_manually.sql`
- `dashboard_setup/01d_check_task.sql`

## Smoke checks

- `tests/pr1_realtime_base_smoke.sql`
- `tests/pr2_unified_reporting_smoke.sql`
