# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with:

- 2 Kafka brokers
- KRaft mode
- 3 partitions
- Replication Factor = 2
- SASL/PLAIN authentication
- TLS encryption
- Python Producer and Consumer
- Failover and recovery testing
- Automated health checks

## Architecture

```text
                    +----------------------+
                    |   message-producer   |
                    |   Python Client      |
                    +----------+-----------+
                               |
                               | SASL/PLAIN
                               | TLS
                               v
                  +------------+-------------+
                  |       Kafka Cluster      |
                  |          KRaft           |
                  +-------------+------------+
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
          +-------------+               +-------------+
          |   kafka-1   |               |   kafka-2   |
          |   Broker 1  |               |   Broker 2  |
          +------+------+               +------+------+
                 |                             |
                 +---------- Replication ------+
                                |
                                v
                    +-----------+-----------+
                    |   message-consumer    |
                    |    Python Client      |
                    +-----------------------+
