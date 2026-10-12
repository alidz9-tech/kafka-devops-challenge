# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with a **Unified Python CLI** for seamless operations.

> 💡 **Note for Evaluators:**  
> While the underlying Bash scripts in the `scripts/` directory handle low-level system tasks, **you do not need to run them manually**.  
> All operations are fully automated through `kafka_manager.py`.

## 🚀 Quick Start (Recommended)

**1. Clone and Configure:**
```bash
git clone https://github.com/alidz9-tech/kafka-devops-challenge.git
cd kafka-devops-challenge
cp .env.example .env
```

**2. Setup and Launch:**
```bash
./kafka_manager.py setup-certs
docker compose up --build -d
./kafka_manager.py create-topic --topic test-topic
./kafka_manager.py healthcheck
```

**3. Test High Availability:**
```bash
./kafka_manager.py failover --target kafka-1
```

---

## Architecture

## Architecture

```mermaid
graph TB
    subgraph Client["Client Layer"]
        P[Producer Application]
        C[Consumer Application]
        CLI[kafka_manager.py Unified CLI]
    end

    subgraph Security["Security Layer"]
        TLS[TLS 1.3 Encryption<br/>Port 9097]
        SASL[SASL/PLAIN Authentication<br/>Username: admin]
        CERTS[SSL/TLS Certificates<br/>CA + Broker Keys]
    end

    subgraph KafkaCluster["Kafka Cluster - KRaft Mode"]
        subgraph Broker1["kafka-1<br/>Controller + Broker"]
            B1[Broker Process]
            Q1[Quorum Member]
        end
        
        subgraph Broker2["kafka-2<br/>Controller + Broker"]
            B2[Broker Process]
            Q2[Quorum Member]
        end
        
        B1 <-->|Replication Factor: 2| B2
        Q1 <-->|KRaft Consensus Protocol| Q2
    end

    subgraph Infrastructure["Docker Infrastructure"]
        DC[Docker Compose Orchestration]
        NET[Docker Network]
        VOL[Persistent Volumes]
    end

    %% Message Flow
    P -->|Produce Messages via SSL| TLS
    C -->|Consume Messages via SSL| TLS
    CLI -->|Manage Topics & Operations| SASL
    
    %% Security to Kafka
    TLS -->|Encrypted Traffic| B1
    TLS -->|Encrypted Traffic| B2
    SASL -->|Authenticate Requests| B1
    SASL -->|Authenticate Requests| B2
    
    %% Certificate Management
    CERTS -->|Enable Secure Communication| TLS
    CERTS -->|JAAS Configuration| SASL
    
    %% Data Persistence
    B1 -->|Store Messages| VOL
    B2 -->|Store Messages| VOL
    
    %% Docker Orchestration
    DC -->|Container Management| Broker1
    DC -->|Container Management| Broker2
    NET -->|Network Isolation| KafkaCluster

    %% Styling
    classDef client fill:#e3f2fd,stroke:#1976d2,stroke-width:3px,color:#0d47a1
    classDef security fill:#fff3e0,stroke:#f57c00,stroke-width:3px,color:#e65100
    classDef kafka fill:#e8f5e9,stroke:#388e3c,stroke-width:3px,color:#1b5e20
    classDef infra fill:#f3e5f5,stroke:#7b1fa2,stroke-width:3px,color:#4a148c
    
    class P,C,CLI client
    class TLS,SASL,CERTS security
    class B1,B2,Q1,Q2,Broker1,Broker2,KafkaCluster kafka
    class DC,NET,VOL,Infrastructure infra

```text
Producer --> Kafka Cluster (KRaft + SASL_SSL) --> Consumer
           [kafka-1] <--> [kafka-2]
```

## Unified Management CLI Reference

| Command | Description |
| :--- | :--- |
| `setup-certs` | Generates SSL/TLS certificates and JAAS config. |
| `create-topic` | Creates Kafka topic securely via SASL_SSL. |
| `healthcheck` | Runs comprehensive 6-step system validation. |
| `failover` | Simulates broker failure and verifies recovery. |
| `test` | Quick end-to-end message flow test. |

## Security & Validation

* **TLS 1.3** encryption on port 9097.
* **SASL/PLAIN** authentication via `.env`.
* **KRaft mode** (no Zookeeper), 2-node quorum.
* **Replication Factor = 2** for high availability.

## Troubleshooting

Reset cluster completely:
```bash
docker compose down -v
./kafka_manager.py setup-certs
docker compose up --build -d
```
