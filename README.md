# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with a **Unified Python CLI** for seamless operations.

> 💡 **Note for Evaluators:**  
> While the underlying Bash scripts in the `scripts/` directory handle low-level system tasks, **you do not need to run them manually**.  
> All operations are fully automated through `kafka_manager.py`.


graph TB
    subgraph ClientLayer["Client Layer"]
        direction LR
        P[Producer App]
        C[Consumer App]
        CLI[kafka_manager.py]
    end

    subgraph SecurityLayer["Security Gateway"]
        TLS((TLS 1.3 Termination<br/>Port 9097))
        SASL{SASL/PLAIN Auth}
    end

    subgraph KafkaCluster["Kafka Cluster (KRaft Mode)"]
        direction TB
        
        subgraph Node1["Node 1 (Controller + Broker)"]
            B1[Broker Process]
            Q1[Quorum Manager]
            V1[(Volume Data)]
        end

        subgraph Node2["Node 2 (Controller + Broker)"]
            B2[Broker Process]
            Q2[Quorum Manager]
            V2[(Volume Data)]
        end

        B1 <-->|Data Replication RF=2| B2
        Q1 <-->|KRaft Consensus Port 9093| Q2
        B1 --- V1
        B2 --- V2
    end

    %% Connections
    P -->|Produce via SSL| TLS
    C -->|Consume via SSL| TLS
    CLI -->|Admin Commands| SASL
    
    TLS -->|Encrypted Traffic| B1
    TLS -->|Encrypted Traffic| B2
    SASL -.->|Validate Credentials| B1
    SASL -.->|Validate Credentials| B2

    %% Styling
    classDef client fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef security fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
    classDef node fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef storage fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,stroke-dasharray: 5 5

    class P,C,CLI client
    class TLS,SASL security
    class B1,B2,Q1,Q2,Node1,Node2 node
    class V1,V2 storage

## 🚀 Quick Start (Recommended)

**1. Clone and Configure:**
```bash
git clone https://github.com/alidz9-tech/kafka-devops-challenge.git
cd kafka-devops-challenge
cp .env.example .env
