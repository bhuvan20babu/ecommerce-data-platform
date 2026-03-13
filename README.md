# Ecommerce Data Platform

Production-style **CDC → Medallion → Incremental SCD2 Data Platform**

Built using:

- Apache Spark
- Apache Airflow
- PostgreSQL
- Docker

Implements modern data engineering patterns:

- CDC ingestion
- Medallion architecture
- Incremental SCD2
- Late arriving event handling
- Data Quality validation
- Dead Letter Queue
- Pipeline Observability
- Data Freshness SLA monitoring

---

# Infrastructure Architecture

```mermaid
flowchart LR

SRC[CDC Source / CSV Feed]

subgraph Docker Environment
AIRFLOW[Airflow Scheduler + DAG]
SPARK[Spark Processing Engine]
POSTGRES[(PostgreSQL Warehouse)]
end

SRC --> AIRFLOW
AIRFLOW --> SPARK
SPARK --> POSTGRES

POSTGRES --> BI[Analytics / BI Tools]
POSTGRES --> MONITOR[Monitoring Tables]

style SRC fill:#ffcccc
style AIRFLOW fill:#cce5ff
style SPARK fill:#d6f5d6
style POSTGRES fill:#ffe680
style BI fill:#e6ccff
style MONITOR fill:#b3e6ff
```

---

# Medallion Architecture

```mermaid
flowchart LR

RAW[Raw CDC Events] --> BRONZE
BRONZE --> SILVER
SILVER --> GOLD

style RAW fill:#ffcccc
style BRONZE fill:#cce5ff
style SILVER fill:#d6f5d6
style GOLD fill:#ffe680
```

| Layer | Description |
|------|-------------|
Bronze | Raw CDC ingestion |
Silver | Cleaned + deduplicated data |
Gold | Curated analytical tables |

---

# End-to-End Data Flow

```mermaid
flowchart LR

CDC[CDC Events]

CDC --> BRONZE[(bronze_customer_events)]

BRONZE --> SILVER[(silver_customer_events)]

SILVER --> DQ[Data Quality Layer]

DQ -->|Valid| GOLD[(dim_customer)]
DQ -->|Invalid| DLQ[(dq_failed_events)]

GOLD --> ANALYTICS
GOLD --> SLA_MONITOR

style CDC fill:#ffcccc
style BRONZE fill:#cce5ff
style SILVER fill:#d6f5d6
style DQ fill:#ffd9b3
style GOLD fill:#ffe680
style DLQ fill:#ffb3b3
style SLA_MONITOR fill:#b3e6ff
```

---

# Airflow Pipeline

```mermaid
flowchart TD

A[Load Bronze Data] --> B[Build Silver Layer]

B --> C[Run Data Quality Checks]

C --> D[Load Gold SCD2 Dimension]

D --> E[Update Pipeline Metrics]

E --> F[Freshness SLA Monitoring]

style A fill:#cce5ff
style B fill:#d6f5d6
style C fill:#ffd9b3
style D fill:#ffe680
style F fill:#b3e6ff
```

---

# Incremental SCD2 Logic

```mermaid
flowchart TD

EVENT[Incoming Customer Event]

EVENT --> HASH[Generate Hash Diff]

HASH --> COMPARE{Compare With Current Record}

COMPARE -->|No Change| IGNORE[Skip]

COMPARE -->|Change Detected| CLOSE[Close Current Record]

CLOSE --> INSERT[Insert New Version]

style EVENT fill:#cce5ff
style HASH fill:#d6f5d6
style CLOSE fill:#ffb3b3
style INSERT fill:#ffe680
```

---

# Late Arriving Event Handling

```mermaid
flowchart LR

EVENT[Incoming Event]

EVENT --> CHECK{event_time < latest?}

CHECK -->|No| NORMAL[Normal SCD2 Processing]

CHECK -->|Yes| LATE[Late Event Detected]

LATE --> REBUILD[Rebuild Customer Timeline]

REBUILD --> UPDATE[Update Dimension History]

style EVENT fill:#cce5ff
style CHECK fill:#ffd9b3
style LATE fill:#ffb3b3
style REBUILD fill:#ffe680
```

---

# Data Quality Layer

```mermaid
flowchart TD

SILVER_DATA --> NULL_CHECK[Check Null Keys]

SILVER_DATA --> DUPLICATE_CHECK[Check Duplicate Events]

SILVER_DATA --> FUTURE_CHECK[Check Future Timestamps]

NULL_CHECK --> VALID
DUPLICATE_CHECK --> VALID
FUTURE_CHECK --> VALID

NULL_CHECK --> INVALID
DUPLICATE_CHECK --> INVALID
FUTURE_CHECK --> INVALID

VALID --> GOLD_LAYER

INVALID --> DLQ

style VALID fill:#ffe680
style INVALID fill:#ffb3b3
style DLQ fill:#ff6666
```

---

# Dead Letter Queue

```mermaid
flowchart LR

DQ_LAYER -->|Invalid Records| DLQ_TABLE

DLQ_TABLE --> INVESTIGATION

style DLQ_TABLE fill:#ffb3b3
style INVESTIGATION fill:#ffd9b3
```

---

# Data Freshness Monitoring

```mermaid
flowchart TD

GOLD_TABLE --> MAX_EVENT[Get Latest Event Timestamp]

MAX_EVENT --> CALC_DELAY[Calculate Freshness Delay]

CALC_DELAY --> SLA_CHECK{Within SLA?}

SLA_CHECK -->|Yes| OK[Status OK]

SLA_CHECK -->|No| BREACH[SLA Breach]

style GOLD_TABLE fill:#ffe680
style CALC_DELAY fill:#b3e6ff
style BREACH fill:#ffb3b3
```

---

# Observability Architecture

```mermaid
flowchart LR

PIPELINE_RUN --> METRICS[(pipeline_metrics)]

PIPELINE_RUN --> SLA_TABLE[(pipeline_sla_monitor)]

METRICS --> DASHBOARD

SLA_TABLE --> ALERTS

style METRICS fill:#b3e6ff
style SLA_TABLE fill:#b3e6ff
style ALERTS fill:#ffb3b3
```

---

# Project Structure

```
ecommerce-data-platform

├── dags
│   └── ecommerce_pipeline.py
│
├── spark_jobs
│   └── load_dimensions.py
│
├── sql
│   └── create_tables.sql
│
├── data
│   ├── raw_customers.csv
│   ├── raw_products.csv
│   └── raw_orders.csv
│
├── docker-compose.yml
│
└── README.md
```

---

# Technology Stack

| Component | Technology |
|----------|-------------|
Orchestration | Airflow |
Processing | Apache Spark |
Warehouse | PostgreSQL |
Containerization | Docker |
Language | Python |

---

# Key Concepts Demonstrated

- CDC ingestion
- Medallion architecture
- Incremental SCD2
- Late arriving event handling
- Data Quality validation
- Dead letter queue
- Observability metrics
- Data freshness SLA monitoring
- Airflow orchestration
- Spark transformations

---

# Final Architecture

```mermaid
flowchart LR

CDC[CDC Feed]

CDC --> BRONZE

BRONZE --> SILVER

SILVER --> DQ

DQ -->|Valid| GOLD

DQ -->|Invalid| DLQ

GOLD --> BI

GOLD --> SLA

SLA --> MONITORING

style CDC fill:#ffcccc
style BRONZE fill:#cce5ff
style SILVER fill:#d6f5d6
style DQ fill:#ffd9b3
style GOLD fill:#ffe680
style DLQ fill:#ffb3b3
style SLA fill:#b3e6ff
style MONITORING fill:#e6ccff
```

---

# Learning Outcomes

This project demonstrates real-world data engineering concepts:

- CDC ingestion
- Medallion architecture
- Incremental SCD2
- Late arriving event handling
- Data quality frameworks
- Dead letter queues
- Pipeline observability
- Data freshness monitoring
- Airflow orchestration
- Spark-based transformations