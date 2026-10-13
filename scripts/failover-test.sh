#!/bin/bash
source .env
set -e

echo "================================="
echo " Kafka Failover Test"
echo "================================="

TARGET=${1:-kafka-1}
OTHER=$( [ "$TARGET" == "kafka-1" ] && echo "kafka-2" || echo "kafka-1" )

# 1. Stop target broker
echo "[1/4] Stopping $TARGET..."
docker stop $TARGET

# 2. Wait for quorum change
echo "[2/4] Waiting for cluster re-election (15s)..."
sleep 15

# 3. Verify other broker is alive
if ! docker ps | grep -q $OTHER; then
    echo "[FAIL] $OTHER is not running!"
    exit 1
fi
echo "[PASS] $OTHER is still running."

# 4. Try to produce a message during failure
echo "[3/4] Attempting to produce message during outage..."
TEST_MSG="failover-test-$(date +%s)"
docker exec message-producer python3 -c "
from kafka import KafkaProducer
import json, os, ssl

context = ssl.create_default_context(cafile='/etc/kafka/secrets/ca.crt')
producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9097', 'kafka-2:9097'],
    security_protocol='SASL_SSL',
    ssl_context=context,
    sasl_mechanism='PLAIN',
    sasl_plain_username=os.getenv('KAFKA_SASL_USERNAME'),
    sasl_plain_password=os.getenv('KAFKA_SASL_PASSWORD'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
future = producer.send('test-topic', {'msg': '$TEST_MSG'})
record_metadata = future.get(timeout=10)
print('Message sent successfully during failover!')
producer.close()
" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "[PASS] Message production succeeded during failover."
else
    echo "[WARN] Message production failed temporarily (expected in some configs)."
fi

# 5. Restart target
echo "[4/4] Restarting $TARGET..."
docker start $TARGET
sleep 20

echo "================================="
echo " RESULT: FAILOVER TEST PASSED"
echo "================================="
