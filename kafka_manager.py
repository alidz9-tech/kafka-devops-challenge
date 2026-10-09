#!/usr/bin/env python3
import os, sys, time, subprocess, argparse

class C:
    B='\033[94m'; G='\033[92m'; W='\033[93m'; F='\033[91m'; E='\033[0m'

def log_i(m): print(f"{C.B}[INFO]{C.E} {m}")
def log_s(m): print(f"{C.G}[PASS]{C.E} {m}")
def log_e(m): print(f"{C.F}[FAIL]{C.E} {m}")
def log_w(m): print(f"{C.W}[WARN]{C.E} {m}")

def run_cap(cmd, check=True):
    try: return subprocess.run(cmd, check=check, capture_output=True, text=True).stdout.strip()
    except Exception as e: return ""

def run_str(cmd, check=True):
    try: subprocess.run(cmd, check=check, text=True)
    except Exception as e: 
        if check: log_e(f"Failed: {cmd}"); sys.exit(1)

def cmd_setup(args):
    log_i("Generating certs...")
    if os.path.exists("scripts/setup-certs.sh"): run_str(["bash", "scripts/setup-certs.sh"]); log_s("Done!")
    else: log_e("setup-certs.sh not found")

def cmd_topic(args):
    log_i(f"Creating topic {args.topic}...")
    conf = 'security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";\nssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12\nssl.truststore.password=changeit\nssl.truststore.type=PKCS12'
    with open("/tmp/kc.properties", "w") as f: f.write(conf)
    run_str(["docker", "cp", "/tmp/kc.properties", "kafka-1:/tmp/kc.properties"])
    run_str(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka-1:9097", "--command-config", "/tmp/kc.properties", "--create", "--topic", args.topic, "--partitions", "3", "--replication-factor", "2", "--if-not-exists"])
    log_s(f"Topic {args.topic} ready!")

def cmd_health(args):
    log_i("Starting Health Check...")
    for c in ["kafka-1", "kafka-2", "message-producer", "message-consumer"]:
        if run_cap(["docker", "inspect", "-f", "{{.State.Running}}", c]) == "true": log_s(f"{c} running")
        else: log_e(f"{c} NOT running"); return
    log_i("Checking Quorum & Flow...")
    if os.path.exists("scripts/healthcheck.sh"): run_str(["bash", "scripts/healthcheck.sh"])
    else: log_w("healthcheck.sh missing")

def cmd_fail(args):
    log_w(f"Stopping {args.target}...")
    run_str(["docker", "stop", args.target])
    log_i("Waiting 15s..."); time.sleep(15)
    rem = "kafka-2" if args.target == "kafka-1" else "kafka-1"
    if run_cap(["docker", "inspect", "-f", "{{.State.Running}}", rem]) == "true": log_s("Cluster survived!")
    else: log_e("Cluster failed!")
    log_i(f"Restarting {args.target}..."); run_str(["docker", "start", args.target])
    log_i("Waiting 20s..."); time.sleep(20); log_s("Failover test done.")

def cmd_test(args):
    log_i("Quick test...")
    if os.path.exists("scripts/test-kafka.sh"): run_str(["bash", "scripts/test-kafka.sh"])
    else: log_e("test-kafka.sh missing")

def main():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd")
    sp.add_parser("setup-certs")
    pt = sp.add_parser("create-topic"); pt.add_argument("--topic", default="test-topic")
    sp.add_parser("healthcheck")
    pf = sp.add_parser("failover"); pf.add_argument("--target", choices=["kafka-1", "kafka-2"], default="kafka-1")
    sp.add_parser("test")
    a = p.parse_args()
    if a.cmd == "setup-certs": cmd_setup(a)
    elif a.cmd == "create-topic": cmd_topic(a)
    elif a.cmd == "healthcheck": cmd_health(a)
    elif a.cmd == "failover": cmd_fail(a)
    elif a.cmd == "test": cmd_test(a)
    else: p.print_help()

if __name__ == "__main__": main()
