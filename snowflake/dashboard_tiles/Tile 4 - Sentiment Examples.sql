USE ROLE TRAINING_ROLE;
USE WAREHOUSE MONKEY_WH;
USE DATABASE MONKEY_DB;
USE SCHEMA FINAL_PROJECT;

SELECT
    ticker,
    sentiment_label,
    sentiment_score,
    published_at,
    source_name,
    title,
    url
FROM rpt_sentiment_examples
ORDER BY ABS(sentiment_score) DESC, published_at DESC
LIMIT 25;