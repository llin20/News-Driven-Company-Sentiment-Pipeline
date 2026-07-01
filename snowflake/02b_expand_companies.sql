USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- =========================================================
-- Expand tracked companies for broader presentation/final analysis
-- Safe to rerun
-- =========================================================

MERGE INTO dim_companies t
USING (
    SELECT * FROM VALUES
        ('GOOGL', 'Alphabet Inc.', 'NASDAQ', 'Technology', 'Internet Content & Information', 'US', 'expanded seed'),
        ('META',  'Meta Platforms, Inc.', 'NASDAQ', 'Technology', 'Internet Content & Information', 'US', 'expanded seed'),
        ('AMD',   'Advanced Micro Devices, Inc.', 'NASDAQ', 'Technology', 'Semiconductors', 'US', 'expanded seed'),
        ('INTC',  'Intel Corporation', 'NASDAQ', 'Technology', 'Semiconductors', 'US', 'expanded seed'),
        ('NFLX',  'Netflix, Inc.', 'NASDAQ', 'Communication Services', 'Entertainment', 'US', 'expanded seed'),
        ('ORCL',  'Oracle Corporation', 'NYSE', 'Technology', 'Software', 'US', 'expanded seed'),
        ('IBM',   'International Business Machines Corporation', 'NYSE', 'Technology', 'Information Technology Services', 'US', 'expanded seed')
) s (ticker, company_name, exchange, sector, industry, country, comment)
ON t.ticker = s.ticker
WHEN NOT MATCHED THEN
  INSERT (ticker, company_name, exchange, sector, industry, country, comment)
  VALUES (s.ticker, s.company_name, s.exchange, s.sector, s.industry, s.country, s.comment);

MERGE INTO dim_company_aliases t
USING (
    SELECT c.company_id, v.alias, v.alias_type
    FROM dim_companies c
    JOIN (
        SELECT * FROM VALUES
            ('GOOGL', 'Google', 'short_name'),
            ('GOOGL', 'Alphabet', 'short_name'),
            ('META',  'Meta', 'short_name'),
            ('META',  'Facebook', 'legacy_name'),
            ('AMD',   'AMD', 'ticker_name'),
            ('AMD',   'Advanced Micro Devices', 'short_name'),
            ('INTC',  'Intel', 'short_name'),
            ('NFLX',  'Netflix', 'short_name'),
            ('ORCL',  'Oracle', 'short_name'),
            ('IBM',   'IBM', 'ticker_name'),
            ('IBM',   'International Business Machines', 'short_name')
    ) v (ticker, alias, alias_type)
      ON c.ticker = v.ticker
) s
ON t.company_id = s.company_id AND t.alias = s.alias
WHEN NOT MATCHED THEN
  INSERT (company_id, alias, alias_type)
  VALUES (s.company_id, s.alias, s.alias_type);

SELECT * FROM dim_companies ORDER BY ticker;
SELECT * FROM dim_company_aliases ORDER BY alias;