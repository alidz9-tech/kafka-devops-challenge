#!/bin/bash
source .env

TARGET=${1:-kafka-1}
OTHER=$( [ "$TARGET" == "kafka-1" ] && echo "kafka-2" || echo "kafka-1" )

echo "================================="
echo " Kafka Failover Test (target: $TARGET)"
echo "================================="

# 1. Pre-check
echo "[1/4] Checking cluster status..."
if ! docker ps | grep -q $OTHER; then
    echo "[FAIL] $OTHER is not running!"
    exit 1
fi
echo "[PASS] $OTHER is running."

# 2. Stop target
echo "[2/4] Stopping $TARGET..."
docker stop $TARGET

# 3. Wait and Verify
echo "[3/4] Waiting for cluster re-election (15s)..."
sleep 15

if docker ps | grep -q $OTHER; then
    echo "[PASS] Cluster survived! $OTHER is still active."
else
    echo "[FAIL] Cluster failed! $OTHER stopped."
    exit 1
fi

# 4. Restart
echo "[4/4] Restarting $TARGET..."
docker start $TARGET
sleep 20

echo "================================="
echo " RESULT: FAILOVER TEST PASSED"
echo "================================="
