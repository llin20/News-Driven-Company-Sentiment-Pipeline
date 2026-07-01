-- =====================================================================
-- 02_seed_companies_and_aliases.sql
-- Purpose:
--   Insert a starter list of tracked companies and common aliases.
-- Safe to re-run: YES
-- =====================================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

-- ---------------------------------------------------------------------
-- Insert starter companies (idempotent pattern)
-- ---------------------------------------------------------------------
INSERT INTO dim_companies (ticker, company_name, exchange, sector, industry, country, comment)
SELECT 'AAPL', 'Apple Inc.', 'NASDAQ', 'Technology', 'Consumer Electronics', 'US', 'starter seed'
WHERE NOT EXISTS (SELECT 1 FROM dim_companies WHERE ticker = 'AAPL');

INSERT INTO dim_companies (ticker, company_name, exchange, sector, industry, country, comment)
SELECT 'MSFT', 'Microsoft Corporation', 'NASDAQ', 'Technology', 'Software', 'US', 'starter seed'
WHERE NOT EXISTS (SELECT 1 FROM dim_companies WHERE ticker = 'MSFT');

INSERT INTO dim_companies (ticker, company_name, exchange, sector, industry, country, comment)
SELECT 'NVDA', 'NVIDIA Corporation', 'NASDAQ', 'Technology', 'Semiconductors', 'US', 'starter seed'
WHERE NOT EXISTS (SELECT 1 FROM dim_companies WHERE ticker = 'NVDA');

INSERT INTO dim_companies (ticker, company_name, exchange, sector, industry, country, comment)
SELECT 'TSLA', 'Tesla, Inc.', 'NASDAQ', 'Consumer Cyclical', 'Auto Manufacturers', 'US', 'starter seed'
WHERE NOT EXISTS (SELECT 1 FROM dim_companies WHERE ticker = 'TSLA');

INSERT INTO dim_companies (ticker, company_name, exchange, sector, industry, country, comment)
SELECT 'AMZN', 'Amazon.com, Inc.', 'NASDAQ', 'Consumer Cyclical', 'Internet Retail', 'US', 'starter seed'
WHERE NOT EXISTS (SELECT 1 FROM dim_companies WHERE ticker = 'AMZN');

-- ---------------------------------------------------------------------
-- Insert common aliases used for article-company matching
-- ---------------------------------------------------------------------
INSERT INTO dim_company_aliases (company_id, alias, alias_type)
SELECT company_id, 'Apple', 'short_name'
FROM dim_companies
WHERE ticker = 'AAPL'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_company_aliases
      WHERE company_id = dim_companies.company_id
        AND alias = 'Apple'
  );

INSERT INTO dim_company_aliases (company_id, alias, alias_type)
SELECT company_id, 'Microsoft', 'short_name'
FROM dim_companies
WHERE ticker = 'MSFT'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_company_aliases
      WHERE company_id = dim_companies.company_id
        AND alias = 'Microsoft'
  );

INSERT INTO dim_company_aliases (company_id, alias, alias_type)
SELECT company_id, 'NVIDIA', 'short_name'
FROM dim_companies
WHERE ticker = 'NVDA'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_company_aliases
      WHERE company_id = dim_companies.company_id
        AND alias = 'NVIDIA'
  );

INSERT INTO dim_company_aliases (company_id, alias, alias_type)
SELECT company_id, 'Tesla', 'short_name'
FROM dim_companies
WHERE ticker = 'TSLA'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_company_aliases
      WHERE company_id = dim_companies.company_id
        AND alias = 'Tesla'
  );

INSERT INTO dim_company_aliases (company_id, alias, alias_type)
SELECT company_id, 'Amazon', 'short_name'
FROM dim_companies
WHERE ticker = 'AMZN'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_company_aliases
      WHERE company_id = dim_companies.company_id
        AND alias = 'Amazon'
  );

-- ---------------------------------------------------------------------
-- Verification output
-- ---------------------------------------------------------------------
SELECT * FROM dim_companies ORDER BY company_id;
SELECT * FROM dim_company_aliases ORDER BY alias_id;
