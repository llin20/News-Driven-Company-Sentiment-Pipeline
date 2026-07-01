"""Spark Structured Streaming processor for Kafka -> Snowflake.

Pipeline stages:
1. Read raw article events from Kafka topic raw_news_articles.
2. Clean/normalize fields and deduplicate by URL hash.
3. Perform company matching using alias dimension loaded from Snowflake.
4. Compute sentiment score with VADER (rule-based, robust for headlines).
5. Append batch outputs into Snowflake base and compatibility tables.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


LOGGER = logging.getLogger("spark_news_stream")
_SENTIMENT_ANALYZER: Optional[SentimentIntensityAnalyzer] = None


RAW_ARTICLE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("provider", StringType()),
        StructField("provider_article_id", StringType()),
        StructField("published_at", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("content", StringType()),
        StructField("source_name", StringType()),
        StructField("url", StringType()),
        StructField("author", StringType()),
        StructField("language", StringType()),
        StructField("country", StringType()),
        StructField("event_ingested_at", StringType()),
    ]
)


def sf_connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_JWT"),
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        private_key_file_pwd=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PWD"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("news-stream-processor")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def load_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", os.environ.get("RAW_ARTICLES_TOPIC", "raw_news_articles"))
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "latest"))
        .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "2000"))
        .load()
    )


def parse_and_normalize(raw_df: DataFrame) -> DataFrame:
    parsed = raw_df.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), RAW_ARTICLE_SCHEMA).alias("v"),
    ).select("kafka_timestamp", "v.*")

    normalized = (
        parsed.withColumn("published_at_ts", F.to_timestamp("published_at"))
        .withColumn("event_ingested_at_ts", F.to_timestamp("event_ingested_at"))
        .withColumn("url_hash", F.sha2(F.lower(F.trim(F.col("url"))), 256))
        .withColumn(
            "article_text",
            F.trim(F.concat_ws(" ", F.coalesce("title", F.lit("")), F.coalesce("description", F.lit("")), F.coalesce("content", F.lit("")))),
        )
        .dropna(subset=["url", "title"])
        .dropDuplicates(["url_hash"])
    )
    return normalized


def _vader_compound(text: Optional[str]) -> float:
    global _SENTIMENT_ANALYZER
    if _SENTIMENT_ANALYZER is None:
        _SENTIMENT_ANALYZER = SentimentIntensityAnalyzer()
    if not text:
        return 0.0
    return float(_SENTIMENT_ANALYZER.polarity_scores(text).get("compound", 0.0))


def add_vader_sentiment(df: DataFrame) -> DataFrame:
    sentiment_udf = F.udf(_vader_compound, DoubleType())
    return (
        df.withColumn("sentiment_score", sentiment_udf(F.col("article_text")))
        .withColumn(
            "sentiment_label",
            F.when(F.col("sentiment_score") >= F.lit(0.05), F.lit("positive"))
            .when(F.col("sentiment_score") <= F.lit(-0.05), F.lit("negative"))
            .otherwise(F.lit("neutral")),
        )
    )


def load_company_aliases(spark: SparkSession) -> DataFrame:
    table_name = os.environ.get("COMPANY_ALIAS_TABLE", "dim_company_aliases")
    sql = f"""
        SELECT company_id, LOWER(alias) AS alias_norm
        FROM {table_name}
    """

    conn = sf_connect()
    try:
        pdf = conn.cursor().execute(sql).fetch_pandas_all()
    finally:
        conn.close()

    return spark.createDataFrame(pdf)


def match_companies(df: DataFrame, aliases: DataFrame) -> DataFrame:
    return (
        df.crossJoin(F.broadcast(aliases))
        .where(F.instr(F.lower(F.col("article_text")), F.col("alias_norm")) > 0)
        .select(
            "event_id",
            "provider",
            "provider_article_id",
            "url",
            "url_hash",
            "published_at_ts",
            "event_ingested_at_ts",
            "company_id",
            "sentiment_score",
            "sentiment_label",
            "source_name",
            "article_text",
        )
    )


def write_batch_to_snowflake(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        LOGGER.info("Skipping empty micro-batch batch_id=%s", batch_id)
        return

    article_pdf = (
        batch_df.select(
            "event_id",
            "url_hash",
            "published_at_ts",
            "company_id",
            "sentiment_score",
            "source_name",
        )
        .dropDuplicates(["event_id", "company_id"])
        .toPandas()
    )

    base_pdf = (
        batch_df.select(
            "event_id",
            "provider",
            "provider_article_id",
            "url",
            "url_hash",
            "published_at_ts",
            "event_ingested_at_ts",
            "company_id",
            "sentiment_score",
            "source_name",
            "article_text",
        )
        .dropDuplicates(["event_id", "company_id"])
        .toPandas()
    )
    if not base_pdf.empty:
        base_pdf["ingest_batch_id"] = str(batch_id)

    minute_pdf = (
        batch_df.withColumn("bucket_minute", F.date_trunc("minute", F.col("published_at_ts")))
        .groupBy("bucket_minute", "company_id")
        .agg(
            F.countDistinct("url_hash").alias("article_count"),
            F.avg("sentiment_score").alias("avg_sentiment"),
        )
        .toPandas()
    )

    conn = sf_connect()
    try:
        if not article_pdf.empty:
            write_pandas(
                conn,
                article_pdf,
                os.environ.get("ARTICLE_MATCH_TABLE", "article_company_match"),
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not base_pdf.empty:
            write_pandas(
                conn,
                base_pdf,
                os.environ.get("ARTICLE_MATCH_BASE_TABLE", "article_company_match_base"),
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not minute_pdf.empty:
            write_pandas(
                conn,
                minute_pdf,
                os.environ.get("MART_MINUTE_TABLE", "mart_company_sentiment_minute"),
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
                use_logical_type=True,
            )
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    spark = build_spark()
    raw = load_kafka_stream(spark)
    normalized = parse_and_normalize(raw)
    scored = add_vader_sentiment(normalized)
    aliases = load_company_aliases(spark)
    matched = match_companies(scored, aliases)

    query = (
        matched.writeStream.trigger(processingTime=os.environ.get("PROCESSING_TIME", "45 seconds"))
        .option("checkpointLocation", os.environ.get("CHECKPOINT_PATH", "/tmp/news-stream-checkpoint"))
        .foreachBatch(write_batch_to_snowflake)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
