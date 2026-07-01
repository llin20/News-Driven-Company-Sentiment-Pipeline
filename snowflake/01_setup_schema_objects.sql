-- =====================================================================
-- 01_setup_schema_objects.sql
-- Purpose:
--   Create the FINAL_PROJECT schema, helper objects, core tables,
--   and a reporting view for the news sentiment pipeline.
-- Safe to re-run: YES
-- =====================================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;

CREATE SCHEMA IF NOT EXISTS FINAL_PROJECT
  COMMENT = 'CSE 5114 final project schema for company-news sentiment pipeline';

USE SCHEMA FINAL_PROJECT;

-- ---------------------------------------------------------------------
-- Helper objects for staged file loading (optional, useful later)
-- ---------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS fp_json_format
  TYPE = JSON
  COMMENT = 'JSON format for raw API payloads';

CREATE FILE FORMAT IF NOT EXISTS fp_csv_format
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  COMMENT = 'CSV format for seed/reference files';

CREATE STAGE IF NOT EXISTS fp_raw_stage
  FILE_FORMAT = fp_json_format
  COMMENT = 'Internal stage for raw files used by the final project';

-- ---------------------------------------------------------------------
-- Dimension tables
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    ticker VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL,
    exchange VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    country VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    comment VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_company_aliases (
    alias_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    company_id NUMBER NOT NULL,
    alias VARCHAR NOT NULL,
    alias_type VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Raw ingest tables
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_articles (
    article_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    provider VARCHAR NOT NULL,
    provider_article_id VARCHAR,
    source_name VARCHAR,
    author VARCHAR,
    title VARCHAR,
    description VARCHAR,
    content VARCHAR,
    url VARCHAR NOT NULL,
    url_hash VARCHAR,
    published_at TIMESTAMP_NTZ,
    language VARCHAR,
    country VARCHAR,
    raw_json VARIANT,
    load_batch_id VARCHAR,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_duplicate BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS raw_pipeline_runs (
    run_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    dag_name VARCHAR,
    run_type VARCHAR,
    source_name VARCHAR,
    run_started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    run_finished_at TIMESTAMP_NTZ,
    status VARCHAR,
    records_read NUMBER,
    records_written NUMBER,
    error_message VARCHAR
);

-- ---------------------------------------------------------------------
-- Curated / enrichment tables
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_article_company_mentions (
    mention_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    article_id NUMBER NOT NULL,
    company_id NUMBER NOT NULL,
    match_method VARCHAR NOT NULL,
    confidence FLOAT,
    matched_text VARCHAR,
    mention_context VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fact_article_sentiment (
    sentiment_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    article_id NUMBER NOT NULL,
    company_id NUMBER,
    model_name VARCHAR NOT NULL,
    sentiment_score FLOAT,
    sentiment_label VARCHAR,
    reasoning VARCHAR,
    scored_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Reporting / mart table
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_company_sentiment_daily (
    metric_date DATE NOT NULL,
    company_id NUMBER NOT NULL,
    article_count NUMBER DEFAULT 0,
    avg_sentiment FLOAT,
    positive_count NUMBER DEFAULT 0,
    neutral_count NUMBER DEFAULT 0,
    negative_count NUMBER DEFAULT 0,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- Reporting view to simplify inspection and screenshots
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_article_enriched AS
SELECT
    a.article_id,
    a.provider,
    a.source_name,
    a.title,
    a.description,
    a.content,
    a.url,
    a.published_at,
    m.company_id,
    c.ticker,
    c.company_name,
    m.match_method,
    m.confidence,
    s.model_name,
    s.sentiment_score,
    s.sentiment_label,
    a.ingested_at
FROM raw_articles a
LEFT JOIN fact_article_company_mentions m
    ON a.article_id = m.article_id
LEFT JOIN dim_companies c
    ON m.company_id = c.company_id
LEFT JOIN fact_article_sentiment s
    ON a.article_id = s.article_id
   AND m.company_id = s.company_id;

-- ---------------------------------------------------------------------
-- Quick object checks
-- ---------------------------------------------------------------------
SHOW TABLES IN SCHEMA MONKEY_DB.FINAL_PROJECT;
SHOW STAGES IN SCHEMA MONKEY_DB.FINAL_PROJECT;
SHOW FILE FORMATS IN SCHEMA MONKEY_DB.FINAL_PROJECT;

SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
