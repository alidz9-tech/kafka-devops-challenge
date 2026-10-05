#!/bin/bash

set -e

BOOTSTRAP="kafka-1:9092"
TOPIC="test-topic"

echo "================================="
echo " Kafka DevOps Challenge Test"
echo "================================="

echo "[1/5] Checking Kafka..."

if ! docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "[FAIL] Kafka is not reachable"
    exit 1
fi

echo "[PASS] Kafka is reachable"

echo
echo "[2/5] Checking topic..."

if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list | grep -qx "$TOPIC"; then

    echo "[PASS] Topic $TOPIC exists"

else

    docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions 3 \
        --replication-factor 2

    echo "[PASS] Topic $TOPIC created"
fi

echo
echo "[3/5] Topic configuration..."

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"

echo
echo "[4/5] Sending test message..."

MESSAGE="kafka-devops-test-$(date +%s)"

echo "$MESSAGE" | docker exec -i kafka-1 \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC"

echo "[PASS] Message sent: $MESSAGE"

echo
echo "[5/5] Reading message..."

if docker exec kafka-1 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | grep -q "$MESSAGE"; then

    echo "[PASS] Message received"

else

    echo "[FAIL] Message was not received"
    exit 1
fi

echo
echo "================================="
echo " RESULT: PASS"
echo "================================="
