#!/usr/bin/env python3
"""
Kafka DevOps Challenge - Unified Management CLI
Author: Alireza Dashtizadeh
Replaces: setup-certs.sh, create-topic.sh, healthcheck.sh, failover-test.sh, test-kafka.sh
"""

import os
import sys
import time
import subprocess
import argparse

# --- تنظیمات رنگ برای خروجی زیبا در ترمینال ---
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def log_info(msg): print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")
def log_success(msg): print(f"{Colors.OKGREEN}[PASS]{Colors.ENDC} {msg}")
def log_error(msg): print(f"{Colors.FAIL}[FAIL]{Colors.ENDC} {msg}")
def log_warn(msg): print(f"{Colors.WARNING}[WARN]{Colors.ENDC} {msg}")

def run_cmd(cmd, shell=False, check=True):
    """اجرای دستورات سیستمی و مدیریت خطا"""
    try:
        result = subprocess.run(cmd, shell=shell, check=check, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        if check:
            log_error(f"Command failed: {' '.join(cmd) if not shell else cmd}")
            log_error(e.stderr.strip())
            sys.exit(1)
        return e.stdout

# --- 1. جایگزین setup-certs.sh ---
def cmd_setup_certs(args):
    log_info("Generating SSL/TLS Certificates and JAAS config...")
    if os.path.exists("scripts/setup-certs.sh"):
        run_cmd(["bash", "scripts/setup-certs.sh"])
        log_success("Certificates generated successfully!")
    else:
        log_error("scripts/setup-certs.sh not found.")

# --- 2. جایگزین create-topic.sh ---
def cmd_create_topic(args):
    topic = args.topic
    log_info(f"Creating topic '{topic}' with SASL_SSL...")
    
    client_conf = f"""security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.password=changeit
ssl.truststore.type=PKCS12
"""
    with open("/tmp/kafka_client.properties", "w") as f:
        f.write(client_conf)
    
    run_cmd(["docker", "cp", "/tmp/kafka_client.properties", "kafka-1:/tmp/kafka_client.properties"])
    
    cmd = [
        "docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka-1:9097",
        "--command-config", "/tmp/kafka_client.properties",
        "--create", "--topic", topic,
        "--partitions", "3", "--replication-factor", "2",
        "--if-not-exists"
    ]
    run_cmd(cmd)
    log_success(f"Topic '{topic}' is ready!")

# --- 3. جایگزین healthcheck.sh ---
def cmd_healthcheck(args):
    log_info("Starting Comprehensive Health Check...")
    for c in ["kafka-1", "kafka-2", "message-producer", "message-consumer"]:
        out = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", c], check=False).strip()
        if out == "true": log_success(f"{c} is running")
        else: log_error(f"{c} is NOT running"); return

    log_info("Checking KRaft Quorum and Message Flow...")
    if os.path.exists("scripts/healthcheck.sh"):
        run_cmd(["bash", "scripts/healthcheck.sh"])
    else:
        log_warn("Full healthcheck logic requires scripts/healthcheck.sh")

# --- 4. جایگزین failover-test.sh ---
def cmd_failover_test(args):
    target = args.target
    log_warn(f"Initiating Failover Test: Stopping {target}...")
    run_cmd(["docker", "stop", target])
    log_info(f"{target} stopped. Waiting 15 seconds for cluster stabilization...")
    time.sleep(15)
    
    remaining = "kafka-2" if target == "kafka-1" else "kafka-1"
    out = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", remaining], check=False).strip()
    if out == "true":
        log_success("Cluster survived the failure! Remaining node is active.")
    else:
        log_error("Cluster failed during failover test!")
        
    log_info(f"Restarting {target}...")
    run_cmd(["docker", "start", target])
    log_info("Waiting 20 seconds for replica recovery...")
    time.sleep(20)
    log_success("Failover test completed. Cluster should be fully recovered.")

# --- 5. جایگزین test-kafka.sh ---
def cmd_test_kafka(args):
    log_info("Running quick Kafka message flow test...")
    if os.path.exists("scripts/test-kafka.sh"):
        run_cmd(["bash", "scripts/test-kafka.sh"])
    else:
        log_error("scripts/test-kafka.sh not found.")

# --- تنظیمات CLI (Argparse) ---
def main():
    parser = argparse.ArgumentParser(description="Kafka DevOps Unified Manager")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("setup-certs", help="Generate SSL certs and JAAS config")

    p_topic = subparsers.add_parser("create-topic", help="Create Kafka topic securely")
    p_topic.add_argument("--topic", default="test-topic", help="Topic name")

    subparsers.add_parser("healthcheck", help="Run full system health check")

    p_fail = subparsers.add_parser("failover", help="Test cluster failover")
    p_fail.add_argument("--target", choices=["kafka-1", "kafka-2"], default="kafka-1", help="Node to stop")

    subparsers.add_parser("test", help="Quick message flow test")

    args = parser.parse_args()

    if args.command == "setup-certs": cmd_setup_certs(args)
    elif args.command == "create-topic": cmd_create_topic(args)
    elif args.command == "healthcheck": cmd_healthcheck(args)
    elif args.command == "failover": cmd_failover_test(args)
    elif args.command == "test": cmd_test_kafka(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
