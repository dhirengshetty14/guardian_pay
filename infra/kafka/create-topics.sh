#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BROKERS:-kafka:9092}"

create_topic() {
  local name="$1"
  local partitions="$2"
  local replication="$3"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${replication}"
}

create_topic "tx.raw" 12 1
create_topic "tx.feature" 12 1
create_topic "tx.graph" 12 1
create_topic "tx.decision" 12 1
create_topic "tx.deadletter" 3 1
create_topic "tx.replay.report" 1 1

echo "Kafka topics ensured"
