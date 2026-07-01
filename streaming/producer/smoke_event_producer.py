"""One-shot Kafka smoke test producer.

Publishes exactly one synthetic event using the same schema as the streaming producer.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event() -> dict:
    published_at = now_utc_iso()
    url = f"https://demo.local/smoke/{int(datetime.now(timezone.utc).timestamp())}"
    payload = {
        "provider": "smoke",
        "provider_article_id": f"smoke-{int(datetime.now(timezone.utc).timestamp())}",
        "published_at": published_at,
        "title": "Smoke test: Apple earnings beat expectations",
        "description": "Synthetic smoke event for Kafka to Spark to Snowflake validation.",
        "content": "Apple and Microsoft shares rally after earnings beat. Profit growth expected.",
        "source_name": "smoke-test",
        "url": url,
        "author": "pipeline-smoke",
        "language": "en",
        "country": "US",
        "raw_json": {"synthetic": True, "purpose": "smoke_test"},
        "event_ingested_at": now_utc_iso(),
    }
    payload["event_id"] = hashlib.sha256(
        f"{payload['provider']}|{payload['url']}|{payload['published_at']}".encode("utf-8")
    ).hexdigest()
    return payload


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("RAW_ARTICLES_TOPIC", "raw_news_articles")

    event = build_event()
    event_key = hashlib.sha256(event["url"].encode("utf-8")).hexdigest()

    producer = KafkaProducer(
        bootstrap_servers=[part.strip() for part in bootstrap.split(",")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=10,
    )
    producer.send(topic, key=event_key, value=event)
    producer.flush()
    print(f"Published exactly 1 smoke event to topic={topic} key={event_key}")


if __name__ == "__main__":
    main()
