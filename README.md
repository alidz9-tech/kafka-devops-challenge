# Kafka DevOps Challenge

A hands-on Kafka DevOps project built with Docker Compose and Apache Kafka 4.0.1 in KRaft mode.

The project demonstrates Kafka cluster deployment, KRaft quorum management, topic partitioning and replication, TLS encryption, SASL/PLAIN authentication, Python Producer/Consumer applications, health checks, persistence, and failover testing.

---

## Project Goals

This project was designed to demonstrate practical DevOps and Linux administration skills around Apache Kafka, including:

- Kafka cluster deployment
- KRaft mode configuration
- Multi-broker Kafka architecture
- Topic partitioning
- Replication
- ISR monitoring
- TLS encryption for client connections
- SASL/PLAIN authentication
- Python Producer/Consumer applications
- Docker Compose orchestration
- Health checks
- Failover and recovery testing
- Persistent Kafka storage
- Basic operational troubleshooting
- Git-based project management

---

## Architecture

```text
                         Kafka DevOps Challenge

             +-------------------------------+
             |       Python Producer        |
             |       kafka-producer          |
             +---------------+---------------+
                             |
                             | SASL/SSL
                             | PLAIN + TLS
                             v
                  +-----------------------+
                  |     Kafka Cluster     |
                  |       KRaft Mode      |
                  |                       |
                  |  +-----------------+  |
                  |  |    kafka-1      |  |
                  |  | Broker + Ctrl   |  |
                  |  +--------+--------+  |
                  |           |             |
                  |           | KRaft       |
                  |           | quorum      |
                  |           |             |
                  |  +--------+--------+  |
                  |  |    kafka-2      |  |
                  |  | Broker + Ctrl   |  |
                  |  +-----------------+  |
                  +-----------+-----------+
                              |
                              | SASL/SSL
                              | PLAIN + TLS
                              v
             +-------------------------------+
             |       Python Consumer        |
             |       kafka-consumer          |
             +-------------------------------+

                         test-topic
                    3 Partitions / RF=2
