#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
PORT="${KAFKA_PORT:-9092}"

listener_check() {
  if command -v ss >/dev/null 2>&1; then
    ss -lnt | awk '{print $4}' | grep -q ":${PORT}$"
    return $?
  fi

  if command -v netstat >/dev/null 2>&1; then
    netstat -lnt 2>/dev/null | awk '{print $4}' | grep -q ":${PORT}$"
    return $?
  fi

  return 2
}

metadata_check() {
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/tmp/kafka_topics_$$.out 2>/tmp/kafka_topics_$$.err
}

socket_check() {
  python3 - <<'PY'
import os
import socket

bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
host, port = bootstrap.split(":", 1)
port = int(port)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1.0)
try:
    sock.connect((host, port))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()

raise SystemExit(0)
PY
}

echo "Checking Kafka health for bootstrap=${BOOTSTRAP}"

if listener_check; then
  echo "Kafka listener detected on :${PORT} (ss/netstat)."
elif [ $? -eq 2 ]; then
  echo "Neither ss nor netstat is available; skipping local listener probe."
else
  echo "No listener detected on :${PORT} from ss/netstat probe."
fi

if metadata_check; then
  echo "Kafka metadata check succeeded via kafka-topics.sh."
  echo "Topics visible from ${BOOTSTRAP}:"
  cat /tmp/kafka_topics_$$.out
  rm -f /tmp/kafka_topics_$$.out /tmp/kafka_topics_$$.err
  exit 0
fi

rm -f /tmp/kafka_topics_$$.out /tmp/kafka_topics_$$.err

if socket_check; then
  echo "TCP socket connectivity to ${BOOTSTRAP} succeeded (port open), but metadata query failed."
  exit 0
fi

echo "Kafka health check failed: no metadata response and no socket connectivity to ${BOOTSTRAP}."
exit 1
