from __future__ import annotations

import hashlib
import json
import urllib.parse
import urllib.request
import urllib.error
from datetime import timedelta

import pendulum

try:
    from airflow.sdk import dag, task, Variable, get_current_context
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models import Variable
    from airflow.operators.python import get_current_context

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DAG_ID = "final_project_news_pipeline"
SNOWFLAKE_CONN_ID = "snowflake_monkey"

# # ---------------------------------------------------------
# # Request strategy:
# # - 4 requests per hourly run = 96 requests/day
# # - Developer plan = 100 requests/day
# # - We deliberately stay slightly under the cap
# # ---------------------------------------------------------
# REQUESTS_PER_RUN = 4
# PAGE_SIZE = 100
# MAX_PAGES_PER_QUERY = 5

# ---------------------------------------------------------
# Request strategy:
# NewsAPI Developer = 100 requests/day
# Goal: use almost all of it within ~4 hours on LinuxLab
#
# Schedule: every 5 minutes
# Requests per run: 2
# 4 hours = 48 runs
# 48 * 2 = 96 requests
# ---------------------------------------------------------
REQUESTS_PER_RUN = 2
PAGE_SIZE = 100
MAX_PAGES_PER_QUERY = 1

# Backfill older than 24h because Developer tier has 24h delay
DAY_OFFSETS = [2, 3, 4, 5]

TRACKED_COMPANIES = [
    {"ticker": "AAPL",  "query": "Apple OR AAPL"},
    {"ticker": "MSFT",  "query": "Microsoft OR MSFT"},
    {"ticker": "NVDA",  "query": "NVIDIA OR NVDA"},
    {"ticker": "TSLA",  "query": "Tesla OR TSLA"},
    {"ticker": "AMZN",  "query": "Amazon OR AMZN"},
    {"ticker": "GOOGL", "query": "Google OR Alphabet OR GOOGL"},
    {"ticker": "META",  "query": "Meta OR Facebook OR META"},
    {"ticker": "AMD",   "query": "AMD OR Advanced Micro Devices"},
    {"ticker": "INTC",  "query": "Intel OR INTC"},
    {"ticker": "NFLX",  "query": "Netflix OR NFLX"},
    {"ticker": "ORCL",  "query": "Oracle OR ORCL"},
    {"ticker": "IBM",   "query": "IBM OR International Business Machines"},
]


def _get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()


def _run_sql(sql_text: str) -> None:
    conn = _get_snowflake_conn()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_text)
        conn.commit()
    finally:
        if cur is not None:
            cur.close()
        conn.close()


def _build_request_plan(now_utc: pendulum.DateTime):
    plan = []
    for day_offset in DAY_OFFSETS:
        day = now_utc.subtract(days=day_offset)
        from_dt = day.start_of("day")
        to_dt = day.end_of("day")

        for company in TRACKED_COMPANIES:
            plan.append(
                {
                    "ticker": company["ticker"],
                    "query": company["query"],
                    "from": from_dt.to_date_string(),
                    "to": to_dt.to_date_string(),
                    "page": 1,
                }
            )
    return plan


def _get_cursor_and_plan_slice(cur, plan_length: int):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS newsapi_fetch_cursor (
            cursor_name STRING,
            cursor_idx NUMBER,
            updated_at TIMESTAMP_NTZ
        )
    """)

    cur.execute("""
        MERGE INTO newsapi_fetch_cursor t
        USING (
            SELECT 'newsapi_main' AS cursor_name, 0 AS cursor_idx, CURRENT_TIMESTAMP() AS updated_at
        ) s
        ON t.cursor_name = s.cursor_name
        WHEN NOT MATCHED THEN
          INSERT (cursor_name, cursor_idx, updated_at)
          VALUES (s.cursor_name, s.cursor_idx, s.updated_at)
    """)

    cur.execute("""
        SELECT cursor_idx
        FROM newsapi_fetch_cursor
        WHERE cursor_name = 'newsapi_main'
    """)
    row = cur.fetchone()
    cursor_idx = int(row[0]) if row and row[0] is not None else 0

    selected_indexes = []
    for i in range(REQUESTS_PER_RUN):
        selected_indexes.append((cursor_idx + i) % plan_length)

    return cursor_idx, selected_indexes


def _advance_cursor(cur, old_cursor_idx: int, requests_made: int, plan_length: int):
    new_cursor_idx = (old_cursor_idx + requests_made) % plan_length
    cur.execute(
        """
        UPDATE newsapi_fetch_cursor
        SET cursor_idx = %s,
            updated_at = CURRENT_TIMESTAMP()
        WHERE cursor_name = 'newsapi_main'
        """,
        (new_cursor_idx,),
    )


@dag(
    dag_id=DAG_ID,
    # schedule="15 * * * *",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "weikai",
        # "retries": 1,
        # "retry_delay": timedelta(minutes=5),
        "retries": 0,
    },
    tags=["cse5114", "final_project", "snowflake", "newsapi"],
)
def final_project_news_pipeline():
    @task()
    def fetch_and_upsert_raw_articles() -> int:
        context = get_current_context()
        api_key = Variable.get("newsapi_api_key")

        now_utc = pendulum.now("UTC")
        request_plan = _build_request_plan(now_utc)

        conn = _get_snowflake_conn()
        cur = None

        loaded_count = 0
        requests_made = 0
        requests_attempted = 0
        total_articles_seen = 0
        error_messages = []

        merge_sql = """
        MERGE INTO raw_articles t
        USING (
            SELECT
                %s AS provider,
                %s AS provider_article_id,
                %s AS source_name,
                %s AS author,
                %s AS title,
                %s AS description,
                %s AS content,
                %s AS url,
                %s AS url_hash,
                TRY_TO_TIMESTAMP_NTZ(%s) AS published_at,
                %s AS language,
                %s AS country,
                PARSE_JSON(%s) AS raw_json,
                %s AS load_batch_id
        ) s
        ON t.provider = s.provider
        AND t.url = s.url
        WHEN MATCHED THEN UPDATE SET
            provider_article_id = s.provider_article_id,
            source_name = s.source_name,
            author = s.author,
            title = s.title,
            description = s.description,
            content = s.content,
            url_hash = s.url_hash,
            published_at = s.published_at,
            language = s.language,
            country = s.country,
            raw_json = s.raw_json,
            load_batch_id = s.load_batch_id,
            ingested_at = CURRENT_TIMESTAMP(),
            is_duplicate = FALSE
        WHEN NOT MATCHED THEN INSERT (
            provider,
            provider_article_id,
            source_name,
            author,
            title,
            description,
            content,
            url,
            url_hash,
            published_at,
            language,
            country,
            raw_json,
            load_batch_id,
            ingested_at,
            is_duplicate
        )
        VALUES (
            s.provider,
            s.provider_article_id,
            s.source_name,
            s.author,
            s.title,
            s.description,
            s.content,
            s.url,
            s.url_hash,
            s.published_at,
            s.language,
            s.country,
            s.raw_json,
            s.load_batch_id,
            CURRENT_TIMESTAMP(),
            FALSE
        )
        """

        log_sql = """
        INSERT INTO raw_pipeline_runs (
            dag_name,
            run_type,
            source_name,
            run_started_at,
            run_finished_at,
            status,
            records_read,
            records_written,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        started_at = pendulum.now("UTC")

        try:
            cur = conn.cursor()

            old_cursor_idx, selected_indexes = _get_cursor_and_plan_slice(cur, len(request_plan))

            print(f"Old cursor index: {old_cursor_idx}")
            print(f"Selected request indexes for this run: {selected_indexes}")

            for idx in selected_indexes:
                item = request_plan[idx]

                params = {
                    "q": item["query"],
                    "from": item["from"],
                    "to": item["to"],
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": PAGE_SIZE,
                    "page": item["page"],
                }

                request_url = "https://newsapi.org/v2/everything?" + urllib.parse.urlencode(params)
                request = urllib.request.Request(
                    request_url,
                    headers={
                        "X-Api-Key": api_key,
                        "X-No-Cache": "true",
                    },
                    method="GET",
                )

                print(f"Fetching request idx={idx} ticker={item['ticker']} page={item['page']} from={item['from']} to={item['to']}")
                print("Request URL:", request_url)
                
                requests_attempted += 1

                try:
                    with urllib.request.urlopen(request, timeout=60) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="ignore")
                    msg = f"HTTPError idx={idx}: {e.code} body={body[:300]}"
                    print(msg)
                    error_messages.append(msg)

                    # Stop early if quota is exhausted or rate limited
                    if e.code == 429:
                        break
                    continue

                status = payload.get("status")
                total_results = payload.get("totalResults")
                articles = payload.get("articles", [])

                print("NewsAPI status:", status)
                print("NewsAPI totalResults:", total_results)
                print("NewsAPI len(articles):", len(articles))

                if status != "ok":
                    msg = f"NewsAPI error idx={idx}: {str(payload)[:300]}"
                    print(msg)
                    error_messages.append(msg)
                    code = payload.get("code")
                    if code in ("apiKeyExhausted", "rateLimited"):
                        break
                    continue

                requests_made += 1
                total_articles_seen += len(articles)
                load_batch_id = f"newsapi_{item['ticker']}_{item['from']}_p{item['page']}_{now_utc.format('YYYYMMDD_HHmmss')}"

                for article in articles:
                    source_name = (article.get("source") or {}).get("name")
                    author = article.get("author")
                    title = article.get("title")
                    description = article.get("description")
                    content = article.get("content")
                    url = article.get("url")
                    published_at = article.get("publishedAt")

                    if not url:
                        continue

                    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                    provider_article_id = url_hash
                    raw_json = json.dumps(article, ensure_ascii=False)

                    cur.execute(
                        merge_sql,
                        (
                            "NEWSAPI",
                            provider_article_id,
                            source_name,
                            author,
                            title,
                            description,
                            content,
                            url,
                            url_hash,
                            published_at,
                            "en",
                            None,
                            raw_json,
                            load_batch_id,
                        ),
                    )
                    loaded_count += 1

            if requests_attempted > 0:
                _advance_cursor(cur, old_cursor_idx, requests_attempted, len(request_plan))

            status_text = "success" if not error_messages else "partial_success"
            cur.execute(
                log_sql,
                (
                    DAG_ID,
                    "scheduled",
                    "NEWSAPI",
                    started_at.to_datetime_string(),
                    pendulum.now("UTC").to_datetime_string(),
                    status_text,
                    total_articles_seen,
                    loaded_count,
                    " | ".join(error_messages)[:500] if error_messages else None,
                ),
            )
            conn.commit()

        except Exception as exc:
            if cur is not None:
                try:
                    cur.execute(
                        log_sql,
                        (
                            DAG_ID,
                            "scheduled",
                            "NEWSAPI",
                            started_at.to_datetime_string(),
                            pendulum.now("UTC").to_datetime_string(),
                            "failed",
                            total_articles_seen,
                            loaded_count,
                            str(exc)[:500],
                        ),
                    )
                    conn.commit()
                except Exception:
                    pass
            raise

        finally:
            if cur is not None:
                cur.close()
            conn.close()

        print(f"Requests attempted this run: {requests_attempted}")
        print(f"Requests made this run: {requests_made}")
        print(f"Articles returned this run: {total_articles_seen}")
        print(f"Rows merged/upserted this run: {loaded_count}")
        return loaded_count

    @task()
    def merge_company_mentions() -> None:
        sql_text = """
        MERGE INTO fact_article_company_mentions tgt
        USING (
            WITH candidate_matches AS (
                SELECT
                    a.article_id,
                    ca.company_id,
                    'alias_ilike' AS match_method,
                    0.80 AS confidence,
                    ca.alias AS matched_text,
                    LEFT(
                        COALESCE(a.title, '') || ' ' ||
                        COALESCE(a.description, '') || ' ' ||
                        COALESCE(a.content, ''),
                        500
                    ) AS mention_context,
                    CASE
                        WHEN COALESCE(a.title, '') ILIKE '%' || ca.alias || '%' THEN 1
                        WHEN COALESCE(a.description, '') ILIKE '%' || ca.alias || '%' THEN 2
                        WHEN COALESCE(a.content, '') ILIKE '%' || ca.alias || '%' THEN 3
                        ELSE 99
                    END AS match_priority,
                    LENGTH(ca.alias) AS alias_length
                FROM raw_articles a
                JOIN dim_company_aliases ca
                  ON COALESCE(a.title, '') ILIKE '%' || ca.alias || '%'
                  OR COALESCE(a.description, '') ILIKE '%' || ca.alias || '%'
                  OR COALESCE(a.content, '') ILIKE '%' || ca.alias || '%'
            ),
            deduped_matches AS (
                SELECT
                    article_id,
                    company_id,
                    match_method,
                    confidence,
                    matched_text,
                    mention_context
                FROM candidate_matches
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY article_id, company_id
                    ORDER BY match_priority ASC, alias_length DESC, matched_text ASC
                ) = 1
            )
            SELECT *
            FROM deduped_matches
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
        )
        """
        _run_sql(sql_text)

    @task()
    def merge_rule_based_sentiment() -> None:
        print("Skipping DAG rule-based sentiment because the project now uses Snowflake AI_SENTIMENT via SQL.")

    @task()
    def refresh_daily_mart() -> None:
        sql_text = """
        MERGE INTO mart_company_sentiment_daily tgt
        USING (
            SELECT
                CAST(a.published_at AS DATE) AS metric_date,
                m.company_id,
                COUNT(DISTINCT a.article_id) AS article_count,
                AVG(s.sentiment_score) AS avg_sentiment,
                COUNT_IF(s.sentiment_label = 'positive') AS positive_count,
                COUNT_IF(s.sentiment_label = 'neutral') AS neutral_count,
                COUNT_IF(s.sentiment_label = 'negative') AS negative_count,
                CURRENT_TIMESTAMP() AS updated_at
            FROM raw_articles a
            JOIN fact_article_company_mentions m
              ON a.article_id = m.article_id
            LEFT JOIN fact_article_sentiment s
              ON a.article_id = s.article_id
             AND m.company_id = s.company_id
            WHERE a.published_at IS NOT NULL
            GROUP BY 1, 2
        ) src
        ON tgt.metric_date = src.metric_date
        AND tgt.company_id = src.company_id
        WHEN MATCHED THEN UPDATE SET
            article_count = src.article_count,
            avg_sentiment = src.avg_sentiment,
            positive_count = src.positive_count,
            neutral_count = src.neutral_count,
            negative_count = src.negative_count,
            updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT (
            metric_date,
            company_id,
            article_count,
            avg_sentiment,
            positive_count,
            neutral_count,
            negative_count,
            updated_at
        )
        VALUES (
            src.metric_date,
            src.company_id,
            src.article_count,
            src.avg_sentiment,
            src.positive_count,
            src.neutral_count,
            src.negative_count,
            src.updated_at
        )
        """
        _run_sql(sql_text)

    @task()
    def report_counts() -> None:
        conn = _get_snowflake_conn()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 'raw_articles' AS table_name, COUNT(*) AS row_count FROM raw_articles
                UNION ALL
                SELECT 'fact_article_company_mentions', COUNT(*) FROM fact_article_company_mentions
                UNION ALL
                SELECT 'fact_article_sentiment', COUNT(*) FROM fact_article_sentiment
                UNION ALL
                SELECT 'mart_company_sentiment_daily', COUNT(*) FROM mart_company_sentiment_daily
                ORDER BY 1
                """
            )
            rows = cur.fetchall()
            for row in rows:
                print(row)
        finally:
            if cur is not None:
                cur.close()
            conn.close()

    raw = fetch_and_upsert_raw_articles()
    mentions = merge_company_mentions()
    sentiment = merge_rule_based_sentiment()
    mart = refresh_daily_mart()
    counts = report_counts()

    raw >> mentions >> sentiment >> mart >> counts


final_project_news_pipeline()