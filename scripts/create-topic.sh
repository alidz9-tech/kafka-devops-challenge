#!/bin/bash

set -e

BOOTSTRAP="kafka-1:9092"
TOPIC="test-topic"
PARTITIONS=3
REPLICATION_FACTOR=2

echo "================================="
echo " Kafka Topic Setup"
echo "================================="

echo
echo "[1/3] Checking Kafka..."

docker exec kafka-1 \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" >/dev/null

echo "[PASS] Kafka is reachable"

echo
echo "[2/3] Checking topic..."

if docker exec kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list | grep -qx "$TOPIC"; then

    echo "[PASS] Topic already exists: $TOPIC"

else

    docker exec kafka-1 \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR"

    echo "[PASS] Topic created: $TOPIC"
fi

echo
echo "[3/3] Validating topic..."

docker exec kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"

echo
echo "================================="
echo " RESULT: TOPIC READY"
echo "================================="
