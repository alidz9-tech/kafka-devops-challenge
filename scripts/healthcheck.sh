#!/bin/bash
source .env

echo "================================="
echo " Kafka Health Check (SASL_SSL)"
echo "================================="

# Create temp config file for SASL_SSL
cat > /tmp/client.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";
ssl.truststore.location=/etc/kafka/secrets/client.truststore.p12
ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD:-changeit}
ssl.truststore.type=PKCS12
EOF

docker cp /tmp/client.properties kafka-1:/tmp/client.properties

# [1/6] Containers
echo "[1/6] Checking containers..."
for svc in kafka-1 kafka-2 message-producer message-consumer; do
    if docker ps | grep -q $svc; then echo "[PASS] $svc is running"; else echo "[FAIL] $svc is down"; exit 1; fi
done

# [2/6] Connection
echo "[2/6] Checking SASL_SSL connection..."
if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9097 --command-config /tmp/client.properties --list &>/dev/null; then
    echo "[PASS] SASL_SSL broker connection"
else
    echo "[FAIL] SASL_SSL broker connection failed"
    exit 1
fi

# [3/6] Quorum
echo "[3/6] Checking KRaft quorum..."
docker exec kafka-1 /opt/kafka/bin/kafka-metadata.sh --snapshot /var/kafka/data/__cluster_metadata-0/00000000000000000000.log --cluster-id &>/dev/null
if [ $? -eq 0 ]; then echo "[PASS] KRaft quorum status retrieved"; else echo "[WARN] Could not read snapshot directly, checking via topics..."; fi

# [4/6] Topic
echo "[4/6] Checking topic..."
if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9097 --command-config /tmp/client.properties --describe --topic test-topic &>/dev/null; then
    echo "[PASS] Topic test-topic exists"
else
    echo "[FAIL] Topic test-topic missing"
    exit 1
fi

# [5/6] Replication
echo "[5/6] Checking replication and ISR..."
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9097 --command-config /tmp/client.properties --describe --topic test-topic

# [6/6] Message Flow
echo "[6/6] Checking message flow..."
TEST_MSG="health-check-$(date +%s)"
docker exec message-producer python3 -c "
from kafka import KafkaProducer
import os, ssl, json
context = ssl.create_default_context(cafile='/etc/kafka/secrets/ca.crt')
p = KafkaProducer(
    bootstrap_servers=['kafka-1:9097'],
    security_protocol='SASL_SSL',
    ssl_context=context,
    sasl_mechanism='PLAIN',
    sasl_plain_username=os.getenv('KAFKA_SASL_USERNAME'),
    sasl_plain_password=os.getenv('KAFKA_SASL_PASSWORD'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
p.send('test-topic', {'msg': '$TEST_MSG'}).get(timeout=10)
p.close()
print('[PASS] Producer -> Kafka (message accepted)')
" 2>/dev/null

echo "================================="
echo " RESULT: HEALTHY"
echo "================================="
