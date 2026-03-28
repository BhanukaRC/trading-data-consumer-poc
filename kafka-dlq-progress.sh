#!/usr/bin/env bash
# DLQ topics are not consumed by a group; use topic describe + end offsets (not consumer-groups --describe).
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-trading-data-consumer-poc-kafka}"

docker exec "$CONTAINER" bash -lc '
for t in market-dlq reconciliation-dlq; do
  echo "========================================"
  echo "Topic: $t"
  echo "========================================"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$t" 2>/dev/null \
    || echo "(topic missing — first DLQ publish or auto-create will create it)"
  echo "--- Latest end offsets (per partition) ---"
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t" --time latest 2>/dev/null \
    || echo "(no offsets yet)"
  echo
done
'
