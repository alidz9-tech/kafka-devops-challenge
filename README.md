# Kafka DevOps Challenge

## Overview

A hands-on Kafka DevOps project using Docker Compose and Apache Kafka 4.0.1 in KRaft mode.

The project demonstrates:

- Kafka cluster deployment
- KRaft controller quorum
- Topic creation and validation
- Partitioning and replication
- Producer/Consumer message flow
- Kafka health checks
- Failover and recovery testing
- Docker-based infrastructure
- Python Producer/Consumer applications

## Architecture

```text
                    +----------------+
                    |    Producer    |
                    +-------+--------+
                            |
                            v
                  +-------------------+
                  |     Kafka Topic    |
                  |     test-topic     |
                  |   3 Partitions     |
                  +---------+---------+
                            |
                            v
                    +---------------+
                    |    Consumer   |
                    +---------------+

              +---------------------------+
              |       Kafka Cluster       |
              |                           |
              |  kafka-1 <----> kafka-2  |
              |       KRaft Mode          |
              +---------------------------+
