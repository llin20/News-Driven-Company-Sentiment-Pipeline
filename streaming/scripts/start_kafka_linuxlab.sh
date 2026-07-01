#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
KAFKA_CONFIG="${KAFKA_CONFIG:-$HOME/kafka-server.properties}"
KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-$HOME/kafka-local-logs}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
RAW_TOPIC="${RAW_ARTICLES_TOPIC:-raw_news_articles}"
TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
METADATA_RETRIES="${KAFKA_METADATA_RETRIES:-10}"
METADATA_SLEEP_SECONDS="${KAFKA_METADATA_SLEEP_SECONDS:-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$KAFKA_LOG_DIR"

check_log="/tmp/kafka_check_$$.log"
create_out="/tmp/kafka_topic_create_$$.out"
create_err="/tmp/kafka_topic_create_$$.err"
describe_out="/tmp/kafka_topic_describe_$$.out"

cleanup() {
  rm -f "$check_log" "$create_out" "$create_err" "$describe_out"
}
trap cleanup EXIT

kafka_metadata_ready() {
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1
}

wait_for_metadata() {
  local attempt
  for attempt in $(seq 1 "$METADATA_RETRIES"); do
    if kafka_metadata_ready; then
      return 0
    fi
    sleep "$METADATA_SLEEP_SECONDS"
  done
  return 1
}

ensure_topic_exists() {
  local attempt
  for attempt in $(seq 1 "$METADATA_RETRIES"); do
    if "$KAFKA_HOME/bin/kafka-topics.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --if-not-exists \
      --topic "$RAW_TOPIC" \
      --partitions "$TOPIC_PARTITIONS" \
      --replication-factor "$TOPIC_REPLICATION_FACTOR" \
      >"$create_out" 2>"$create_err"; then
      return 0
    fi
    sleep "$METADATA_SLEEP_SECONDS"
  done
  return 1
}

kafka_started="0"
if bash "$SCRIPT_DIR/check_kafka_linuxlab.sh" >"$check_log" 2>&1; then
  echo "Kafka appears healthy at ${BOOTSTRAP}."
  cat "$check_log"
else
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" > "$HOME/kafka-server.out" 2>&1 &
  kafka_started="1"
  echo "Kafka start requested at ${BOOTSTRAP}. Waiting for metadata readiness..."
fi

if ! wait_for_metadata; then
  echo "Kafka did not become metadata-ready after ${METADATA_RETRIES} attempts. Check: $HOME/kafka-server.out"
  exit 1
fi

if ! ensure_topic_exists; then
  echo "Failed to ensure topic '${RAW_TOPIC}' after ${METADATA_RETRIES} attempts."
  if [ -s "$create_err" ]; then
    cat "$create_err"
  fi
  exit 1
fi

if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$RAW_TOPIC" >"$describe_out" 2>/dev/null; then
  echo "Topic ready: ${RAW_TOPIC} (partitions=${TOPIC_PARTITIONS}, replication-factor=${TOPIC_REPLICATION_FACTOR})."
  cat "$describe_out"
else
  echo "Topic '${RAW_TOPIC}' ensured on ${BOOTSTRAP}."
fi

if [ "$kafka_started" = "1" ]; then
  echo "Kafka is ready. Log file: $HOME/kafka-server.out"
fi
