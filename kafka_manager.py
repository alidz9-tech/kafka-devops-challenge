#!/usr/bin/env python3
"""Unified Kafka Management CLI for DevOps Challenge."""

import subprocess
import sys
import time
import os
from pathlib import Path

# Load environment variables from .env file if present
env_file = Path('.env')
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# Configuration from Environment Variables
KAFKA_USER = os.getenv('KAFKA_SASL_USERNAME', 'admin')
KAFKA_PASS = os.getenv('KAFKA_SASL_PASSWORD', 'admin-secret')
TRUSTSTORE_PASS = os.getenv('KAFKA_TRUSTSTORE_PASSWORD', 'changeit')
BOOTSTRAP_SERVER = "kafka-1:9097"
TOPIC_NAME = "test-topic"
PARTITIONS = 3
REPLICATION_FACTOR = 2

def run_cmd(cmd, check=True):
    """Run a shell command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        if check:
            print(f"[ERROR] Command failed: {cmd}")
            print(e.stderr)
            sys.exit(1)
        return "", e.stderr, e.returncode

def setup_certs():
    print("[INFO] Setting up certificates...")
    run_cmd("chmod +x scripts/setup-certs.sh && ./scripts/setup-certs.sh")
    print("[PASS] Certificates generated.")

def create_topic(topic=None):
    topic = topic or TOPIC_NAME
    print(f"[INFO] Creating topic: {topic}...")
    
    # Using docker exec with environment variables passed dynamically
    cmd = (
        f'docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh '
        f'--bootstrap-server {BOOTSTRAP_SERVER} '
        f'--command-config <(echo -e "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PASS}\";") '
        f'--create --topic {topic} --partitions {PARTITIONS} --replication-factor {REPLICATION_FACTOR}'
    )
    
    # Note: Process substitution <() might not work directly in subprocess.run with shell=True in some contexts. 
    # A safer way is to create a temp config file.
    config_content = f"""security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASS}";
ssl.truststore.location=/etc/kafka/secrets/client.truststore.p12
ssl.truststore.password={TRUSTSTORE_PASS}
ssl.truststore.type=PKCS12
"""
    with open('/tmp/client.properties', 'w') as f:
        f.write(config_content)
        
    # Copy config to container
    run_cmd("docker cp /tmp/client.properties kafka-1:/tmp/client.properties")
    
    cmd = (
        f'docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh '
        f'--bootstrap-server {BOOTSTRAP_SERVER} '
        f'--command-config /tmp/client.properties '
        f'--create --topic {topic} --partitions {PARTITIONS} --replication-factor {REPLICATION_FACTOR}'
    )
    
    out, err, code = run_cmd(cmd, check=False)
    if "already exists" in err or code == 0:
        print(f"[PASS] Topic '{topic}' is ready.")
    else:
        print(f"[WARN] {err}")

def healthcheck():
    print("[INFO] Starting Health Check...")
    
    # 1. Check Containers
    containers = ['kafka-1', 'kafka-2', 'message-producer', 'message-consumer']
    for c in containers:
        out, _, _ = run_cmd(f"docker ps -q -f name={c}", check=False)
        if out:
            print(f"[PASS] {c} running")
        else:
            print(f"[FAIL] {c} is NOT running")
            return

    print("[INFO] Checking Quorum & Flow...")
    print("=================================")
    print(" Kafka Health Check (SASL_SSL)")
    print("=================================")
    
    # Run the detailed bash healthcheck script which uses env vars correctly
    run_cmd("./scripts/healthcheck.sh")

def test_flow():
    print("[INFO] Quick test...")
    print("=================================")
    print(" Kafka DevOps Challenge Test")
    print(" Secure connection: SASL_SSL")
    print("=================================")
    run_cmd("./scripts/test-kafka.sh")

def failover(target="kafka-1"):
    print(f"[WARN] Stopping {target}...")
    run_cmd(f"docker stop {target}")
    print("[INFO] Waiting 15s for cluster to adjust...")
    time.sleep(15)
    
    # Check if the other broker is still alive and leading
    other = "kafka-2" if target == "kafka-1" else "kafka-1"
    out, _, _ = run_cmd(f"docker ps -q -f name={other}", check=False)
    if out:
        print("[PASS] Cluster survived! Other broker is active.")
    else:
        print("[FAIL] Cluster failed!")
        
    print(f"[INFO] Restarting {target}...")
    run_cmd(f"docker start {target}")
    print("[INFO] Waiting 20s for recovery...")
    time.sleep(20)
    print("[PASS] Failover test done.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./kafka_manager.py [setup-certs|create-topic|healthcheck|test|failover]")
        sys.exit(1)
        
    command = sys.argv[1]
    if command == "setup-certs":
        setup_certs()
    elif command == "create-topic":
        topic = None
        if "--topic" in sys.argv:
            topic = sys.argv[sys.argv.index("--topic") + 1]
        create_topic(topic)
    elif command == "healthcheck":
        healthcheck()
    elif command == "test":
        test_flow()
    elif command == "failover":
        target = "kafka-1"
        if "--target" in sys.argv:
            target = sys.argv[sys.argv.index("--target") + 1]
        failover(target)
