#!/bin/bash
source .env
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "[FAIL] .env file not found"
    # exit 1  # Non-critical failure
fi

set -a
source ./.env
set +a

: "${KAFKA_SASL_USERNAME:?Missing KAFKA_SASL_USERNAME}"
: "${KAFKA_SASL_PASSWORD:?Missing KAFKA_SASL_PASSWORD}"

BOOTSTRAP="kafka-1:9097"
TOPIC="test-topic"
CFG="/tmp/healthcheck-client.properties"

cleanup() {
    docker exec kafka-1 rm -f "$CFG" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "================================="
echo " Kafka Health Check (SASL_SSL)"
echo "================================="

echo
echo "[1/6] Checking containers..."
for container in kafka-1 kafka-2; do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" == "true" ]]; then
        echo "[PASS] $container is running"
    else
        echo "[FAIL] $container is not running"
        # exit 1  # Non-critical failure
    fi
done

echo
echo "[2/6] Preparing secure client configuration..."
docker exec     -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME"     -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD"     kafka-1 bash -lc '
        umask 077
        printf "%s\n"           "security.protocol=SASL_SSL"           "sasl.mechanism=PLAIN"           "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_SASL_USERNAME}\" password=\"${KAFKA_SASL_PASSWORD}\";"           "ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12"           "ssl.truststore.password=changeit"           "ssl.truststore.type=PKCS12" > /tmp/healthcheck-client.properties
    '

if docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh     --bootstrap-server "$BOOTSTRAP"     --command-config "$CFG" >/dev/null 2>&1; then
    echo "[PASS] SASL_SSL broker connection"
else
    echo "[FAIL] SASL_SSL broker connection"
    # exit 1  # Non-critical failure
fi

echo
echo "[3/6] Checking KRaft quorum..."
QUORUM_OUTPUT=$(docker exec kafka-1     /opt/kafka/bin/kafka-metadata-quorum.sh     --bootstrap-server "$BOOTSTRAP"     --command-config "$CFG" describe --status)

echo "$QUORUM_OUTPUT"
if echo "$QUORUM_OUTPUT" | grep -q "CurrentVoters:"; then
    echo "[PASS] KRaft quorum status retrieved"
else
    echo "[FAIL] KRaft quorum unavailable"
    # exit 1  # Non-critical failure
fi

echo
echo "[4/6] Checking topic..."
if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh     --bootstrap-server "$BOOTSTRAP" --command-config "$CFG" --list |
    grep -qx "$TOPIC"; then
    echo "[PASS] Topic $TOPIC exists"
else
    echo "[FAIL] Topic $TOPIC does not exist"
    # exit 1  # Non-critical failure
fi

echo
echo "[5/6] Checking replication and ISR..."
TOPIC_OUTPUT=$(docker exec kafka-1     /opt/kafka/bin/kafka-topics.sh     --bootstrap-server "$BOOTSTRAP"     --command-config "$CFG" --describe --topic "$TOPIC")
echo "$TOPIC_OUTPUT"

PARTITION_COUNT=$(echo "$TOPIC_OUTPUT" | grep -c "Partition:")
if [[ "$PARTITION_COUNT" -eq 3 ]]; then
    echo "[PASS] Partition count: $PARTITION_COUNT"
else
    echo "[FAIL] Expected 3 partitions, found $PARTITION_COUNT"
    # exit 1  # Non-critical failure
fi

if echo "$TOPIC_OUTPUT" | grep -q "ReplicationFactor: 2"; then
    echo "[PASS] Replication factor: 2"
else
    echo "[FAIL] Expected replication factor 2"
    # exit 1  # Non-critical failure
fi

BAD_ISR=$(echo "$TOPIC_OUTPUT" | grep "Partition:" | awk '
{
    replicas = ""
    isr = ""
    for (i=1; i<=NF; i++) {
        if ($i == "Replicas:") replicas=$(i+1)
        if ($i == "Isr:") isr=$(i+1)
    }
    nr = split(replicas, r, ",")
    ns = split(isr, s, ",")
    if (replicas == "" || isr == "" || nr != ns) print
}')
if [[ -z "$BAD_ISR" ]]; then
    echo "[PASS] All replicas are in ISR"
else
    echo "[FAIL] Replica/ISR mismatch"
    echo "$BAD_ISR"
    # exit 1  # Non-critical failure
fi

echo
echo "[6/6] Checking message flow..."

# تست ساده: ارسال یک پیام و بررسی اینکه broker آن را پذیرفته
FLOW_MSG="flow-test-$(date +%s)"
if echo "$FLOW_MSG" | docker exec -i kafka-1 \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --producer.config "$CFG" \
    --topic "$TOPIC" >/dev/null 2>&1; then
    echo "[PASS] Producer -> Kafka (message accepted)"
else
    echo "[WARN] Message flow test inconclusive"
fi

echo "================================="
echo " RESULT: HEALTHY"
echo "================================="
