#!/usr/bin/env bash
# LinuxLab environment template (no secrets).

export PROJECT_ROOT="$HOME/projects/cse5114_final_project"
export KAFKA_HOME="/opt/kafka"
export SPARK_HOME="/opt/spark"

export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export RAW_ARTICLES_TOPIC="raw_news_articles"
export BACKFILL_TOPIC="raw_news_articles"
export KAFKA_TOPIC_PARTITIONS="3"
export KAFKA_TOPIC_REPLICATION_FACTOR="1"

# LinuxLab Spark standalone master URL pattern:
#   spark://${SLURMD_NODENAME}:${SPARK_MASTER_PORT}
# Set this in your session after server-airflow25 -c 4 has started Spark services.
export SPARK_MASTER_URL="spark://${SLURMD_NODENAME}:${SPARK_MASTER_PORT}"

export SOURCE_TYPE="gdelt"
export POLL_SECONDS="300"
export LOG_LEVEL="INFO"

# GDELT live/backfill defaults (primary practical source for demo/backfill)
export GDELT_MODE="docapi"
export GDELT_QUERY='("stock market" OR earnings OR inflation OR "Federal Reserve" OR Apple OR Microsoft OR NVIDIA)'
export GDELT_LOOKBACK_MINUTES="60"
export GDELT_MAX_RECORDS_PER_POLL="25"
export GDELT_MAX_ATTEMPTS="6"
export GDELT_BACKOFF_BASE_SEC="5"
export GDELT_BACKOFF_CAP_SEC="120"
# export GDELT_RSS_FEED_URLS="https://example.com/gdelt-rss-feed.xml"

# NewsAPI is optional
# export NEWSAPI_API_KEY=""
# export NEWS_QUERY="stock OR shares OR earnings"

# Spark streaming settings
export PROCESSING_TIME="45 seconds"
export CHECKPOINT_PATH="$HOME/checkpoints/news-stream"
export KAFKA_STARTING_OFFSETS="latest"
export MAX_OFFSETS_PER_TRIGGER="2000"

# Backfill settings
export BACKFILL_DAYS="14"
export BACKFILL_WINDOW_HOURS="6"
export BACKFILL_MAX_RECORDS="250"
export BACKFILL_MAX_EVENTS="5000"
export BACKFILL_MIN_REQUEST_INTERVAL_SEC="2.5"
export BACKFILL_MAX_ATTEMPTS="8"
export BACKFILL_BACKOFF_BASE_SEC="3"
export BACKFILL_BACKOFF_CAP_SEC="90"
export BACKFILL_SKIP_FAILED_WINDOWS="1"
export BACKFILL_MAX_SKIPPED_WINDOWS="48"
export BACKFILL_MAX_CONSECUTIVE_FAILED_WINDOWS="12"
export BACKFILL_FAILED_WINDOWS_FILE="$HOME/.cache/gdelt_backfill_failed_windows.jsonl"
export BACKFILL_CURSOR_FILE="$HOME/.cache/gdelt_backfill_cursor.json"
export BACKFILL_RESUME="1"

# GDELT live producer cooldown after repeated poll failures (helps avoid 429 thrashing)
export GDELT_FAILURE_COOLDOWN_BASE_SEC="60"
export GDELT_FAILURE_COOLDOWN_CAP_SEC="900"

# Snowflake (key-pair auth)
# Keep SNOWFLAKE_AUTHENTICATOR as SNOWFLAKE_JWT for key-pair auth.
export SNOWFLAKE_ACCOUNT="<your_account>"
export SNOWFLAKE_USER="<your_user>"
export SNOWFLAKE_DATABASE="<your_database>"
export SNOWFLAKE_SCHEMA="<your_schema>"
export SNOWFLAKE_WAREHOUSE="<your_warehouse>"
export SNOWFLAKE_ROLE="<your_role>"
export SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT"
export SNOWFLAKE_PRIVATE_KEY_FILE="$HOME/path/to/rsa_key.p8"
# export SNOWFLAKE_PRIVATE_KEY_PWD=""

export COMPANY_ALIAS_TABLE="DIM_COMPANY_ALIASES"
export ARTICLE_MATCH_TABLE="ARTICLE_COMPANY_MATCH"
export MART_MINUTE_TABLE="MART_COMPANY_SENTIMENT_MINUTE"

export ARTICLE_MATCH_BASE_TABLE="ARTICLE_COMPANY_MATCH_BASE"

export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
