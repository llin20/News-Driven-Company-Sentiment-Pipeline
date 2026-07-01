-- =====================================================================
-- 04_run_company_matching.sql
-- Purpose:
--   Populate fact_article_company_mentions by matching article text
--   against company aliases.
-- Safe to re-run: YES
-- Notes:
--   This is a simple baseline matching method using ILIKE on aliases.
-- =====================================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

MERGE INTO fact_article_company_mentions tgt
USING (
    SELECT
        a.article_id,
        ca.company_id,
        'alias_ilike' AS match_method,
        0.80 AS confidence,
        LISTAGG(DISTINCT ca.alias, ', ') WITHIN GROUP (ORDER BY ca.alias) AS matched_text,
        LEFT(
            COALESCE(a.title, '') || ' ' ||
            COALESCE(a.description, '') || ' ' ||
            COALESCE(a.content, ''),
            500
        ) AS mention_context
    FROM raw_articles a
    JOIN dim_company_aliases ca
      ON COALESCE(a.title, '') ILIKE '%' || ca.alias || '%'
      OR COALESCE(a.description, '') ILIKE '%' || ca.alias || '%'
      OR COALESCE(a.content, '') ILIKE '%' || ca.alias || '%'
    GROUP BY
        a.article_id,
        ca.company_id,
        LEFT(
            COALESCE(a.title, '') || ' ' ||
            COALESCE(a.description, '') || ' ' ||
            COALESCE(a.content, ''),
            500
        )
) src
ON tgt.article_id = src.article_id
AND tgt.company_id = src.company_id
WHEN MATCHED THEN UPDATE SET
    match_method = src.match_method,
    confidence = src.confidence,
    matched_text = src.matched_text,
    mention_context = src.mention_context,
    created_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    article_id,
    company_id,
    match_method,
    confidence,
    matched_text,
    mention_context,
    created_at
)
VALUES (
    src.article_id,
    src.company_id,
    src.match_method,
    src.confidence,
    src.matched_text,
    src.mention_context,
    CURRENT_TIMESTAMP()
);

-- Verification output
SELECT
    m.mention_id,
    m.article_id,
    c.ticker,
    c.company_name,
    m.match_method,
    m.confidence,
    m.matched_text
FROM fact_article_company_mentions m
JOIN dim_companies c
  ON m.company_id = c.company_id
ORDER BY m.mention_id DESC;
