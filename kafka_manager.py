#!/usr/bin/env python3
"""Unified management CLI for the Kafka DevOps challenge.

A thin wrapper around the scripts in ./scripts. Output is streamed live and the
exit code of the underlying script is returned, so a failing check fails the command.
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"


def run_script(name, *args):
    script = SCRIPTS / name
    if not script.is_file():
        print(f"[FAIL] {script} not found", file=sys.stderr)
        return 1
    try:
        return subprocess.run(["bash", str(script), *args], cwd=ROOT).returncode
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted")
        return 130


def main():
    parser = argparse.ArgumentParser(
        description="Manage the Kafka DevOps challenge environment.",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    sub.add_parser("setup-certs", help="generate CA, broker certificates, keystores and JAAS file")

    topic = sub.add_parser("create-topic", help="create the topic over SASL_SSL (3 partitions, RF 2)")
    topic.add_argument("--topic", default="test-topic", help="topic name (default: test-topic)")

    sub.add_parser("healthcheck", help="check containers, SASL_SSL, KRaft quorum, topic, ISR and message flow")
    sub.add_parser("test", help="send a message and read it back with a temporary consumer")

    failover = sub.add_parser("failover", help="stop one broker, verify recovery, check message flow")
    failover.add_argument("--target", choices=["kafka-1", "kafka-2"], default="kafka-1",
                          help="broker to stop (default: kafka-1)")

    args = parser.parse_args()

    if args.command == "setup-certs":
        return run_script("setup-certs.sh")
    if args.command == "create-topic":
        return run_script("create-topic.sh", args.topic)
    if args.command == "healthcheck":
        return run_script("healthcheck.sh")
    if args.command == "test":
        return run_script("test-kafka.sh")
    if args.command == "failover":
        return run_script("failover-test.sh", args.target)
    return 2


if __name__ == "__main__":
    sys.exit(main())
