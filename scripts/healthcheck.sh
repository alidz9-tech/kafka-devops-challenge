#!/bin/bash

set -e

BOOTSTRAP="kafka-1:9092"
TOPIC="test-topic"

echo "================================="
echo " Kafka Health Check"
echo "================================="

echo
echo "[1/6] Checking containers..."

for container in kafka-1 kafka-2; do
    if docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
        echo "[PASS] $container is running"
    else
        echo "[FAIL] $container is not running"
        exit 1
    fi
done

echo
echo "[2/6] Checking Kafka broker..."

if docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "[PASS] Kafka broker is reachable"
else
    echo "[FAIL] Kafka broker is not reachable"
    exit 1
fi

echo
echo "[3/6] Checking KRaft quorum..."

QUORUM_OUTPUT=$(docker exec kafka-1 \
    /opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server "$BOOTSTRAP" \
    describe --status 2>/dev/null)

echo "$QUORUM_OUTPUT"

if echo "$QUORUM_OUTPUT" | grep -q "CurrentVoters:"; then
    echo "[PASS] KRaft quorum is available"
else
    echo "[FAIL] KRaft quorum is unavailable"
    exit 1
fi

echo
echo "[4/6] Checking topic..."

if docker exec kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list | grep -qx "$TOPIC"; then

    echo "[PASS] Topic $TOPIC exists"

else

    echo "[FAIL] Topic $TOPIC does not exist"
    exit 1
fi

echo
echo "[5/6] Checking replication and ISR..."

TOPIC_OUTPUT=$(docker exec kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC")

echo "$TOPIC_OUTPUT"

PARTITION_COUNT=$(echo "$TOPIC_OUTPUT" | grep -c "Partition:")

if [ "$PARTITION_COUNT" -eq 3 ]; then
    echo "[PASS] Partition count: $PARTITION_COUNT"
else
    echo "[FAIL] Expected 3 partitions, found $PARTITION_COUNT"
    exit 1
fi

if echo "$TOPIC_OUTPUT" | grep -q "ReplicationFactor: 2"; then
    echo "[PASS] Replication factor: 2"
fi

BAD_ISR=$(echo "$TOPIC_OUTPUT" | grep "Partition:" | awk '
{
    for (i=1; i<=NF; i++) {
        if ($i == "Replicas:") replicas=$(i+1)
        if ($i == "Isr:") isr=$(i+1)
    }

    split(replicas,r,",")
    split(isr,s,",")

    if (length(r) != length(s))
        print
}')

if [ -z "$BAD_ISR" ]; then
    echo "[PASS] All replicas are in ISR"
else
    echo "[FAIL] Replica/ISR mismatch detected"
    echo "$BAD_ISR"
    exit 1
fi

echo
echo "[6/6] Checking message flow..."

MESSAGE="healthcheck-$(date +%s)"

echo "$MESSAGE" | docker exec -i kafka-1 \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" >/dev/null

if docker exec kafka-1 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | grep -q "$MESSAGE"; then

    echo "[PASS] Producer -> Kafka -> Consumer"

else

    echo "[FAIL] Message flow failed"
    exit 1
fi

echo
echo "================================="
echo " RESULT: HEALTHY"
echo "================================="
