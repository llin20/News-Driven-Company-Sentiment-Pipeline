"""Lightweight article producer.

Polls one or more upstream sources and publishes normalized article events to Kafka.
Designed for near-real-time ingestion (every 30-60 seconds).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote_plus
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from json import JSONDecodeError

from kafka import KafkaProducer


LOGGER = logging.getLogger("news_producer")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_gdelt_dt(value: str) -> str:
    if not value:
        return _now_utc_iso()
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return _now_utc_iso()


@dataclass
class Article:
    provider: str
    provider_article_id: str
    published_at: str
    title: str
    description: str
    content: str
    source_name: str
    url: str
    author: str
    language: str
    country: str
    raw_json: Dict

    @property
    def stable_key(self) -> str:
        return hashlib.sha256(self.url.encode("utf-8")).hexdigest()

    def as_event(self) -> Dict:
        payload = {
            "provider": self.provider,
            "provider_article_id": self.provider_article_id,
            "published_at": self.published_at,
            "title": self.title,
            "description": self.description,
            "content": self.content,
            "source_name": self.source_name,
            "url": self.url,
            "author": self.author,
            "language": self.language,
            "country": self.country,
            "raw_json": self.raw_json,
            "event_ingested_at": _now_utc_iso(),
        }
        payload["event_id"] = hashlib.sha256(
            f"{payload['provider']}|{payload['url']}|{payload['published_at']}".encode("utf-8")
        ).hexdigest()
        return payload


class SourceClient:
    def fetch(self) -> Iterable[Article]:
        raise NotImplementedError


class NewsApiClient(SourceClient):
    def __init__(self, api_key: str, query: str = "stock market OR earnings"):
        self.api_key = api_key
        self.query = query

    def fetch(self) -> Iterable[Article]:
        now = datetime.now(timezone.utc)
        from_dt = (now - timedelta(minutes=15)).isoformat(timespec="seconds")
        query = quote_plus(self.query)
        url = (
            "https://newsapi.org/v2/everything"
            f"?q={query}&from={from_dt}&sortBy=publishedAt&pageSize=100&language=en"
        )
        req = Request(url, headers={"X-Api-Key": self.api_key})
        with urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))

        for idx, row in enumerate(payload.get("articles", [])):
            article_url = row.get("url")
            if not article_url:
                continue
            yield Article(
                provider="newsapi",
                provider_article_id=f"newsapi-{idx}-{row.get('publishedAt', '')}",
                published_at=row.get("publishedAt") or _now_utc_iso(),
                title=row.get("title") or "",
                description=row.get("description") or "",
                content=row.get("content") or "",
                source_name=(row.get("source") or {}).get("name", "unknown"),
                url=article_url,
                author=row.get("author") or "",
                language="en",
                country="",
                raw_json=row,
            )


class RssClient(SourceClient):
    def __init__(self, feed_urls: List[str], provider: str = "rss"):
        self.feed_urls = feed_urls
        self.provider = provider

    def _parse_item(self, item: Dict, fallback_source: str) -> Optional[Article]:
        link = item.get("link")
        if not link:
            return None

        published = item.get("published") or item.get("pubDate")
        published_at = _now_utc_iso()
        if published:
            try:
                published_at = parsedate_to_datetime(published).astimezone(timezone.utc).isoformat()
            except Exception:
                pass

        provider_id = item.get("id") or item.get("guid") or hashlib.sha256(link.encode("utf-8")).hexdigest()
        return Article(
            provider=self.provider,
            provider_article_id=str(provider_id),
            published_at=published_at,
            title=item.get("title") or "",
            description=item.get("summary") or item.get("description") or "",
            content=item.get("summary") or item.get("description") or "",
            source_name=item.get("source", {}).get("title") if isinstance(item.get("source"), dict) else fallback_source,
            url=link,
            author=item.get("author") or "",
            language="en",
            country="",
            raw_json=item,
        )

    def fetch(self) -> Iterable[Article]:
        import feedparser

        for feed_url in self.feed_urls:
            parsed = feedparser.parse(feed_url)
            fallback_source = (parsed.feed or {}).get("title", self.provider)
            for item in parsed.entries:
                article = self._parse_item(item, fallback_source)
                if article:
                    yield article


class GdeltApiClient(SourceClient):
    def __init__(
        self,
        query: str,
        max_records: int = 25,
        lookback_minutes: int = 60,
        max_attempts: int = 6,
        backoff_base_sec: float = 5.0,
        backoff_cap_sec: float = 120.0,
        failure_cooldown_base_sec: float = 60.0,
        failure_cooldown_cap_sec: float = 900.0,
    ):
        self.query = query
        self.max_records = max_records
        self.lookback_minutes = lookback_minutes
        self.max_attempts = max(1, max_attempts)
        self.backoff_base_sec = max(0.1, backoff_base_sec)
        self.backoff_cap_sec = max(self.backoff_base_sec, backoff_cap_sec)
        self.failure_cooldown_base_sec = max(1.0, failure_cooldown_base_sec)
        self.failure_cooldown_cap_sec = max(self.failure_cooldown_base_sec, failure_cooldown_cap_sec)
        self.consecutive_fetch_failures = 0

    def _build_url(self) -> str:
        encoded_query = quote_plus(self.query)
        return (
            "https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={encoded_query}&mode=ArtList&format=json"
            f"&maxrecords={self.max_records}&sort=datedesc&timespan={self.lookback_minutes}min"
        )

    def _compute_backoff_sleep(self, attempt: int, retry_after: Optional[float]) -> float:
        if retry_after is not None and retry_after > 0:
            base_sleep = min(self.backoff_cap_sec, retry_after)
        else:
            base_sleep = min(self.backoff_cap_sec, self.backoff_base_sec * (2 ** max(0, attempt - 1)))
        jitter = random.uniform(0, min(1.0, max(0.5, base_sleep * 0.2)))
        return min(self.backoff_cap_sec, base_sleep + jitter)

    def _compute_failure_cooldown(self) -> float:
        if self.consecutive_fetch_failures <= 0:
            return 0.0
        base = min(
            self.failure_cooldown_cap_sec,
            self.failure_cooldown_base_sec * (2 ** max(0, self.consecutive_fetch_failures - 1)),
        )
        jitter = random.uniform(0, min(10.0, max(1.0, base * 0.25)))
        return min(self.failure_cooldown_cap_sec, base + jitter)

    def _parse_retry_after(self, header_value: Optional[str]) -> Optional[float]:
        if not header_value:
            return None
        value = header_value.strip()
        if not value:
            return None
        try:
            return max(0.0, float(int(value)))
        except ValueError:
            pass
        try:
            retry_dt = parsedate_to_datetime(value)
            if retry_dt.tzinfo is None:
                retry_dt = retry_dt.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:  # noqa: BLE001
            return None

    def _request_payload(self) -> Dict:
        req = Request(
            self._build_url(),
            headers={
                "User-Agent": "cse5114-final-project-news-producer/1.0 (+https://github.com/KennyRao/cse5114_final_project)",
                "Accept": "application/json,text/plain,*/*",
            },
        )
        with urlopen(req, timeout=30) as response:
            body_bytes = response.read()

        if not body_bytes:
            raise ValueError("GDELT returned empty response body")

        body = body_bytes.decode("utf-8", errors="replace").strip()
        if not body:
            raise ValueError("GDELT returned blank response body")

        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError(f"GDELT returned unexpected JSON payload type: {type(payload).__name__}")

        articles = payload.get("articles")
        if articles is None:
            payload["articles"] = []
            return payload
        if not isinstance(articles, list):
            raise ValueError("GDELT payload field 'articles' is not a list")
        return payload

    def fetch(self) -> Iterable[Article]:
        payload: Optional[Dict] = None
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            retry_after: Optional[float] = None
            should_retry = False
            reason = "unknown"
            try:
                payload = self._request_payload()
                break
            except HTTPError as exc:
                last_error = exc
                retry_after = self._parse_retry_after(exc.headers.get("Retry-After")) if exc.headers else None
                should_retry = exc.code == 429 or 500 <= exc.code < 600
                if not should_retry:
                    raise
                reason = f"HTTP {exc.code}"
            except URLError as exc:
                last_error = exc
                should_retry = True
                reason = f"URLError ({exc.reason})"
            except (JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                last_error = exc
                should_retry = True
                reason = str(exc)

            if not should_retry or attempt >= self.max_attempts:
                break

            sleep_sec = self._compute_backoff_sleep(attempt, retry_after=retry_after)
            LOGGER.warning(
                "GDELT fetch retry attempt=%s/%s reason=%s retry_after=%s sleep_sec=%.2f",
                attempt,
                self.max_attempts,
                reason,
                retry_after,
                sleep_sec,
            )
            time.sleep(sleep_sec)

        if payload is None:
            self.consecutive_fetch_failures += 1
            cooldown = self._compute_failure_cooldown()
            LOGGER.warning(
                "GDELT fetch exhausted retries; consecutive_failures=%s cooldown_sec=%.2f",
                self.consecutive_fetch_failures,
                cooldown,
            )
            if cooldown > 0:
                time.sleep(cooldown)
            raise RuntimeError(f"GDELT fetch failed after {self.max_attempts} attempts: {last_error}") from last_error

        if self.consecutive_fetch_failures:
            LOGGER.info("GDELT fetch recovered after consecutive_failures=%s", self.consecutive_fetch_failures)
        self.consecutive_fetch_failures = 0

        for row in payload.get("articles", []):
            article_url = row.get("url")
            if not article_url:
                continue
            seen_date = row.get("seendate", "")
            title = row.get("title") or ""
            desc = row.get("socialimage") or row.get("excerpt") or ""
            source_name = row.get("domain") or row.get("sourcecountry") or "gdelt"
            yield Article(
                provider="gdelt",
                provider_article_id=hashlib.sha256(f"{article_url}|{seen_date}".encode("utf-8")).hexdigest(),
                published_at=_parse_gdelt_dt(seen_date),
                title=title,
                description=desc,
                content=desc,
                source_name=source_name,
                url=article_url,
                author="",
                language=row.get("language") or "en",
                country=row.get("sourcecountry") or "",
                raw_json=row,
            )


class DedupState:
    """In-memory dedup guard to avoid re-publishing the same URL repeatedly.

    For production, replace with Redis or compacted Kafka topic checkpoint state.
    """

    def __init__(self, max_keys: int = 200_000):
        self.max_keys = max_keys
        self._seen: Dict[str, float] = {}

    def is_new(self, article: Article) -> bool:
        key = article.stable_key
        if key in self._seen:
            return False
        self._seen[key] = time.time()
        if len(self._seen) > self.max_keys:
            sorted_keys = sorted(self._seen.items(), key=lambda kv: kv[1])
            for old_key, _ in sorted_keys[: max(1, self.max_keys // 10)]:
                self._seen.pop(old_key, None)
        return True


class ProducerApp:
    def __init__(self, bootstrap_servers: str, topic: str, source_client: SourceClient):
        self.topic = topic
        self.source_client = source_client
        self.producer = KafkaProducer(
            bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",")],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            linger_ms=50,
            retries=10,
            compression_type="gzip",
        )
        self.state = DedupState()

    def _run_once(self) -> None:
        fetched = 0
        dedup_dropped = 0
        published = 0
        for article in self.source_client.fetch():
            fetched += 1
            if not self.state.is_new(article):
                dedup_dropped += 1
                continue
            event = article.as_event()
            self.producer.send(self.topic, key=article.stable_key, value=event)
            published += 1
        self.producer.flush()
        LOGGER.info(
            "Producer iteration complete fetched=%s dedup_dropped=%s published=%s",
            fetched,
            dedup_dropped,
            published,
        )

    def run_forever(self, poll_seconds: int) -> None:
        LOGGER.info("Starting producer loop with poll interval=%ss", poll_seconds)
        while True:
            try:
                self._run_once()
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Producer iteration failed: %s", exc)
            time.sleep(poll_seconds)

    def run_once(self) -> None:
        self._run_once()


def build_source_from_env(source: str) -> SourceClient:
    if source == "newsapi":
        api_key = os.environ.get("NEWSAPI_API_KEY")
        if not api_key:
            raise RuntimeError("NEWSAPI_API_KEY is required when --source newsapi is selected")
        query = os.environ.get("NEWS_QUERY", "stock OR shares OR earnings")
        return NewsApiClient(api_key=api_key, query=query)

    if source == "rss":
        urls = os.environ.get(
            "RSS_FEED_URLS",
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml,https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        )
        return RssClient(feed_urls=[part.strip() for part in urls.split(",") if part.strip()], provider="rss")

    if source == "gdelt":
        query = os.environ.get(
            "GDELT_QUERY",
            '("stock market" OR earnings OR inflation OR "Federal Reserve" OR Apple OR Microsoft OR NVIDIA)',
        )
        max_records = int(os.environ.get("GDELT_MAX_RECORDS_PER_POLL", "25"))
        lookback_minutes = int(os.environ.get("GDELT_LOOKBACK_MINUTES", "60"))
        max_attempts = int(os.environ.get("GDELT_MAX_ATTEMPTS", "6"))
        backoff_base_sec = float(os.environ.get("GDELT_BACKOFF_BASE_SEC", "5"))
        backoff_cap_sec = float(os.environ.get("GDELT_BACKOFF_CAP_SEC", "120"))
        failure_cooldown_base_sec = float(os.environ.get("GDELT_FAILURE_COOLDOWN_BASE_SEC", "60"))
        failure_cooldown_cap_sec = float(os.environ.get("GDELT_FAILURE_COOLDOWN_CAP_SEC", "900"))
        gdelt_mode = os.environ.get("GDELT_MODE", "docapi").lower()
        if gdelt_mode == "rssarchive":
            urls = os.environ.get("GDELT_RSS_FEED_URLS", "").strip()
            if not urls:
                raise RuntimeError("Set GDELT_RSS_FEED_URLS when GDELT_MODE=rssarchive")
            return RssClient(feed_urls=[part.strip() for part in urls.split(",") if part.strip()], provider="gdelt")
        return GdeltApiClient(
            query=query,
            max_records=max_records,
            lookback_minutes=lookback_minutes,
            max_attempts=max_attempts,
            backoff_base_sec=backoff_base_sec,
            backoff_cap_sec=backoff_cap_sec,
            failure_cooldown_base_sec=failure_cooldown_base_sec,
            failure_cooldown_cap_sec=failure_cooldown_cap_sec,
        )

    raise ValueError(f"Unsupported source: {source}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Near-real-time news producer -> Kafka")
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("RAW_ARTICLES_TOPIC", "raw_news_articles"))
    parser.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    parser.add_argument("--source", choices=["newsapi", "rss", "gdelt"], default=os.environ.get("SOURCE_TYPE", "gdelt"))
    parser.add_argument("--run-once", action="store_true", help="Run one poll iteration and exit")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    source_client = build_source_from_env(args.source)
    app = ProducerApp(
        bootstrap_servers=args.kafka_bootstrap,
        topic=args.topic,
        source_client=source_client,
    )
    if args.run_once:
        app.run_once()
        return
    app.run_forever(poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    main()
