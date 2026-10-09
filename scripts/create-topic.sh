#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "[FAIL] .env file not found"
    exit 1
fi

set -a
source ./.env
set +a

: "${KAFKA_SASL_USERNAME:?Missing KAFKA_SASL_USERNAME}"
: "${KAFKA_SASL_PASSWORD:?Missing KAFKA_SASL_PASSWORD}"

echo "================================="
echo " Kafka Topic Setup (SASL_SSL)"
echo "================================="

docker exec \
    -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME" \
    -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD" \
    kafka-1 bash -lc '
set -euo pipefail

BOOTSTRAP="kafka-1:9097"
TOPIC="test-topic"
CFG=$(mktemp)
trap '\''rm -f "$CFG"'\'' EXIT
umask 077

cat > "$CFG" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.password=changeit
ssl.truststore.type=PKCS12
EOF

kafka() {
    "/opt/kafka/bin/$1" \
        --bootstrap-server "$BOOTSTRAP" \
        --command-config "$CFG" "${@:2}"
}

echo
echo "[1/3] Checking secure broker connection..."
kafka kafka-broker-api-versions.sh >/dev/null
echo "[PASS] SASL_SSL connection succeeded"

echo
echo "[2/3] Checking topic..."

if kafka kafka-topics.sh --list | grep -qx "$TOPIC"; then
    echo "[PASS] Topic already exists: $TOPIC"
else
    kafka kafka-topics.sh \
        --create \
        --topic "$TOPIC" \
        --partitions 3 \
        --replication-factor 2
    echo "[PASS] Topic created: $TOPIC"
fi

echo
echo "[3/3] Validating topic..."
kafka kafka-topics.sh --describe --topic "$TOPIC"

echo
echo "================================="
echo " RESULT: TOPIC READY"
echo "================================="
'
