"""Long-running GDELT historical backfill producer.

Fetches historical articles from GDELT in non-overlapping windows and publishes
normalized events to Kafka. Includes safe rate limiting, retry/backoff, and
resumable local cursor checkpoints for LinuxLab sessions.
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
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
from json import JSONDecodeError

from kafka import KafkaProducer


LOGGER = logging.getLogger("gdelt_backfill")


class GdeltBackfillError(RuntimeError):
    """Base class for backfill fetch failures."""


class GdeltNonRetryableError(GdeltBackfillError):
    """Failure that should not be retried (e.g. hard 4xx)."""


class GdeltRetryExhaustedError(GdeltBackfillError):
    """Failure after max retry attempts."""


@dataclass
class WindowResult:
    articles: list[dict]
    attempts: int


def dt_to_gdelt(value: datetime) -> str:
    return value.strftime("%Y%m%d%H%M%S")


def gdelt_to_iso(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return datetime.now(timezone.utc).isoformat()


def build_event(article: dict) -> dict | None:
    url = article.get("url")
    if not url:
        return None

    seen_date = article.get("seendate", "")
    payload = {
        "provider": "gdelt",
        "provider_article_id": hashlib.sha256(f"{url}|{seen_date}".encode("utf-8")).hexdigest(),
        "published_at": gdelt_to_iso(seen_date),
        "title": article.get("title") or "",
        "description": article.get("socialimage") or article.get("excerpt") or "",
        "content": article.get("socialimage") or article.get("excerpt") or "",
        "source_name": article.get("domain") or article.get("sourcecountry") or "gdelt",
        "url": url,
        "author": "",
        "language": article.get("language") or "en",
        "country": article.get("sourcecountry") or "",
        "raw_json": article,
        "event_ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    payload["event_id"] = hashlib.sha256(
        f"{payload['provider']}|{payload['url']}|{payload['published_at']}".encode("utf-8")
    ).hexdigest()
    return payload


def _parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None

    value = header_value.strip()
    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _compute_sleep_s(
    attempt: int,
    backoff_base_sec: float,
    backoff_cap_sec: float,
    retry_after: Optional[float] = None,
) -> float:
    if retry_after is not None:
        return min(backoff_cap_sec, retry_after + random.uniform(0, 0.75))
    return min(backoff_cap_sec, backoff_base_sec * (2 ** (attempt - 1)) + random.uniform(0, 0.75))


def _parse_articles_payload(body_bytes: bytes) -> list[dict]:
    if not body_bytes:
        raise ValueError("Empty response body from GDELT")

    body_text = body_bytes.decode("utf-8", errors="replace").strip()
    if not body_text:
        raise ValueError("Blank response body from GDELT")

    payload = json.loads(body_text)

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected non-object JSON payload type: {type(payload).__name__}")

    articles = payload.get("articles", [])
    if articles is None:
        return []
    if not isinstance(articles, list):
        raise ValueError(f"Unexpected 'articles' type: {type(articles).__name__}")

    return articles


def fetch_window_with_retry(
    query: str,
    start_utc: datetime,
    end_utc: datetime,
    max_records: int,
    max_attempts: int,
    backoff_base_sec: float,
    backoff_cap_sec: float,
) -> WindowResult:
    encoded = quote_plus(query)
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={encoded}&mode=ArtList&format=json&maxrecords={max_records}&sort=datedesc"
        f"&startdatetime={dt_to_gdelt(start_utc)}&enddatetime={dt_to_gdelt(end_utc)}"
    )

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": "cse5114-final-project-gdelt-backfill/1.0",
                    "Accept": "application/json,text/plain,*/*",
                },
            )

            with urlopen(req, timeout=45) as response:
                body_bytes = response.read()

            return WindowResult(
                articles=_parse_articles_payload(body_bytes),
                attempts=attempt,
            )

        except HTTPError as exc:
            last_error = exc
            if not (exc.code == 429 or 500 <= exc.code < 600):
                raise GdeltNonRetryableError(f"HTTP error {exc.code} for GDELT window") from exc

            retry_after = _parse_retry_after(exc.headers.get("Retry-After")) if exc.headers else None
            sleep_s = _compute_sleep_s(
                attempt,
                backoff_base_sec,
                backoff_cap_sec,
                retry_after=retry_after if exc.code == 429 else None,
            )

            if attempt >= max_attempts:
                break

            LOGGER.warning(
                "HTTP error for window %s..%s (attempt %s/%s code=%s retry_after=%s), sleeping %.2fs",
                start_utc.isoformat(),
                end_utc.isoformat(),
                attempt,
                max_attempts,
                exc.code,
                retry_after,
                sleep_s,
            )
            time.sleep(sleep_s)

        except URLError as exc:
            last_error = exc
            sleep_s = _compute_sleep_s(attempt, backoff_base_sec, backoff_cap_sec)

            if attempt >= max_attempts:
                break

            LOGGER.warning(
                "Network error for window %s..%s (attempt %s/%s err=%s), sleeping %.2fs",
                start_utc.isoformat(),
                end_utc.isoformat(),
                attempt,
                max_attempts,
                exc,
                sleep_s,
            )
            time.sleep(sleep_s)

        except (JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            last_error = exc
            sleep_s = _compute_sleep_s(attempt, backoff_base_sec, backoff_cap_sec)

            if attempt >= max_attempts:
                break

            LOGGER.warning(
                "Malformed/empty API response for window %s..%s (attempt %s/%s err=%s), sleeping %.2fs",
                start_utc.isoformat(),
                end_utc.isoformat(),
                attempt,
                max_attempts,
                exc,
                sleep_s,
            )
            time.sleep(sleep_s)

    raise GdeltRetryExhaustedError(
        f"Exhausted retries for GDELT window {start_utc.isoformat()}..{end_utc.isoformat()}: {last_error}"
    ) from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GDELT backfill producer -> Kafka")
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("BACKFILL_TOPIC", os.environ.get("RAW_ARTICLES_TOPIC", "raw_news_articles")))
    parser.add_argument("--query", default=os.environ.get("GDELT_QUERY", '(Apple OR Microsoft OR NVIDIA OR Tesla OR Amazon)'))
    parser.add_argument("--days-back", type=int, default=int(os.environ.get("BACKFILL_DAYS", "7")))
    parser.add_argument("--window-hours", type=int, default=int(os.environ.get("BACKFILL_WINDOW_HOURS", "6")))
    parser.add_argument("--max-records", type=int, default=int(os.environ.get("BACKFILL_MAX_RECORDS", "250")))
    parser.add_argument("--max-events", type=int, default=int(os.environ.get("BACKFILL_MAX_EVENTS", "5000")))
    parser.add_argument("--min-request-interval-sec", type=float, default=float(os.environ.get("BACKFILL_MIN_REQUEST_INTERVAL_SEC", "2.5")))
    parser.add_argument("--max-attempts", type=int, default=int(os.environ.get("BACKFILL_MAX_ATTEMPTS", "8")))
    parser.add_argument("--backoff-base-sec", type=float, default=float(os.environ.get("BACKFILL_BACKOFF_BASE_SEC", "3")))
    parser.add_argument("--backoff-cap-sec", type=float, default=float(os.environ.get("BACKFILL_BACKOFF_CAP_SEC", "90")))
    parser.add_argument("--skip-failed-windows", action="store_true", default=os.environ.get("BACKFILL_SKIP_FAILED_WINDOWS", "1") == "1")
    parser.add_argument("--max-skipped-windows", type=int, default=int(os.environ.get("BACKFILL_MAX_SKIPPED_WINDOWS", "48")))
    parser.add_argument("--max-consecutive-failed-windows", type=int, default=int(os.environ.get("BACKFILL_MAX_CONSECUTIVE_FAILED_WINDOWS", "12")))
    parser.add_argument(
        "--failed-windows-file",
        default=os.environ.get("BACKFILL_FAILED_WINDOWS_FILE", str(Path.home() / ".cache" / "gdelt_backfill_failed_windows.jsonl")),
    )
    parser.add_argument("--cursor-file", default=os.environ.get("BACKFILL_CURSOR_FILE", str(Path.home() / ".cache" / "gdelt_backfill_cursor.json")))
    parser.add_argument("--resume", action="store_true", default=os.environ.get("BACKFILL_RESUME", "1") == "1")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def _load_cursor(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        value = payload.get("next_cursor")
        if not value:
            return None
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _write_cursor(path: Path, cursor: datetime, now_utc: datetime, sent: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "next_cursor": cursor.isoformat(),
        "updated_at": now_utc.isoformat(),
        "sent_events": sent,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


def _record_failed_window(
    path: Path,
    start_utc: datetime,
    end_utc: datetime,
    *,
    reason: str,
    error_type: str,
    skipped: bool,
    attempts: int,
    skipped_count: int,
    consecutive_failures: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "window_start": start_utc.isoformat(),
        "window_end": end_utc.isoformat(),
        "attempts": attempts,
        "error_type": error_type,
        "error": reason,
        "skipped": skipped,
        "skipped_count": skipped_count,
        "consecutive_failures": consecutive_failures,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    producer = KafkaProducer(
        bootstrap_servers=[part.strip() for part in args.kafka_bootstrap.split(",")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=10,
    )

    now_utc = datetime.now(timezone.utc)
    default_start = now_utc - timedelta(days=args.days_back)
    cursor_path = Path(args.cursor_file)
    failed_windows_path = Path(args.failed_windows_file)
    checkpoint_cursor = _load_cursor(cursor_path) if args.resume else None

    if checkpoint_cursor and checkpoint_cursor < now_utc:
        start_utc = checkpoint_cursor
        LOGGER.info("Resuming backfill from cursor %s", start_utc.isoformat())
    else:
        start_utc = default_start
        LOGGER.info("Starting backfill from days-back window %s", start_utc.isoformat())

    cursor = start_utc
    sent = 0
    skipped_windows = 0
    consecutive_failed_windows = 0
    seen_urls: set[str] = set()

    while cursor < now_utc and sent < args.max_events:
        chunk_end = min(cursor + timedelta(hours=args.window_hours), now_utc)
        try:
            window = fetch_window_with_retry(
                args.query,
                cursor,
                chunk_end,
                args.max_records,
                max_attempts=args.max_attempts,
                backoff_base_sec=args.backoff_base_sec,
                backoff_cap_sec=args.backoff_cap_sec,
            )
            articles = window.articles
            consecutive_failed_windows = 0
            LOGGER.info(
                "window=%s..%s fetched=%s attempts=%s",
                cursor.isoformat(),
                chunk_end.isoformat(),
                len(articles),
                window.attempts,
            )
        except GdeltBackfillError as exc:
            consecutive_failed_windows += 1
            exhausted = isinstance(exc, GdeltRetryExhaustedError)
            can_skip = args.skip_failed_windows and exhausted and skipped_windows < args.max_skipped_windows

            _record_failed_window(
                failed_windows_path,
                cursor,
                chunk_end,
                reason=str(exc),
                error_type=type(exc).__name__,
                skipped=can_skip,
                attempts=args.max_attempts if exhausted else 1,
                skipped_count=skipped_windows + (1 if can_skip else 0),
                consecutive_failures=consecutive_failed_windows,
            )

            if not can_skip:
                LOGGER.exception(
                    "Failed to fetch window %s..%s (consecutive_failures=%s skipped_windows=%s). Stopping.",
                    cursor.isoformat(),
                    chunk_end.isoformat(),
                    consecutive_failed_windows,
                    skipped_windows,
                )
                break

            skipped_windows += 1
            next_cursor = chunk_end + timedelta(seconds=1)
            _write_cursor(cursor_path, next_cursor, datetime.now(timezone.utc), sent)
            LOGGER.warning(
                "Skipping failed window %s..%s and advancing cursor to %s "
                "(skipped_windows=%s/%s consecutive_failures=%s sent=%s failed_windows_file=%s)",
                cursor.isoformat(),
                chunk_end.isoformat(),
                next_cursor.isoformat(),
                skipped_windows,
                args.max_skipped_windows,
                consecutive_failed_windows,
                sent,
                failed_windows_path,
            )
            cursor = next_cursor

            if consecutive_failed_windows >= args.max_consecutive_failed_windows:
                LOGGER.error(
                    "Reached max consecutive failed windows (%s). Stopping to avoid useless spinning.",
                    args.max_consecutive_failed_windows,
                )
                break

            time.sleep(max(0.0, args.min_request_interval_sec))
            continue

        for article in articles:
            event = build_event(article)
            if not event:
                continue
            url = event["url"].strip().lower()
            if url in seen_urls:
                continue
            seen_urls.add(url)

            if not args.dry_run:
                event_key = hashlib.sha256(event["url"].encode("utf-8")).hexdigest()
                producer.send(args.topic, key=event_key, value=event)
            sent += 1
            if sent >= args.max_events:
                break

        if not args.dry_run:
            producer.flush()

        next_cursor = chunk_end + timedelta(seconds=1)
        _write_cursor(cursor_path, next_cursor, datetime.now(timezone.utc), sent)
        LOGGER.info(
            "progress sent=%s next_cursor=%s skipped_windows=%s consecutive_failures=%s cursor_file=%s",
            sent,
            next_cursor.isoformat(),
            skipped_windows,
            consecutive_failed_windows,
            cursor_path,
        )

        cursor = next_cursor
        time.sleep(max(0.0, args.min_request_interval_sec))

    LOGGER.info(
        "Backfill complete sent=%s skipped_windows=%s dry_run=%s topic=%s failed_windows_file=%s",
        sent,
        skipped_windows,
        args.dry_run,
        args.topic,
        failed_windows_path,
    )


if __name__ == "__main__":
    main()
