# HAADE

### Kubernetes-Based Real-Time Data Lakehouse

HAADE is a **data engineering and real-time lakehouse project** designed to ingest streaming events from Apache Kafka, process them using Apache Spark Structured Streaming, and persist them into Apache Iceberg tables backed by S3-compatible object storage.

The platform is designed around a containerized and Kubernetes-native architecture, with infrastructure and workloads managed through Kubernetes.

The core streaming pipeline is:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ▼
Apache Iceberg
  │
  ▼
MinIO / S3-compatible Storage
```

The project provides a foundation for building scalable data ingestion, storage, processing, analytics, and observability pipelines.

---

# 🚀 Project Overview

Modern applications continuously generate large volumes of events.

HAADE explores how these events can be collected and transformed into a reliable analytical data platform using open-source technologies.

The system separates the major stages of the data lifecycle:

```text
                 ┌──────────────────┐
                 │   Data Sources   │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      Kafka       │
                 │ Event Streaming  │
                 └────────┬─────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │ Spark Structured       │
              │ Streaming              │
              │                        │
              │ Parse / Transform      │
              │ Enrich / Process       │
              └───────────┬────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ Apache Iceberg   │
                 │   Lakehouse      │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      MinIO       │
                 │ S3-Compatible    │
                 │    Storage       │
                 └──────────────────┘
```

---

# ✨ Key Features

* Real-time event ingestion using **Apache Kafka**
* Stream processing using **Apache Spark Structured Streaming**
* Open table format using **Apache Iceberg**
* S3-compatible object storage using **MinIO**
* Kubernetes-native deployment
* Spark workloads managed using **Spark Operator**
* Containerized Spark jobs
* Kafka running inside Kubernetes
* Persistent streaming checkpoints
* Bronze-layer data ingestion
* JSON event parsing and enrichment
* Prometheus-based observability
* Shared logging and metrics components
* Development workflow using Skaffold
* GitOps-oriented deployment with Argo CD
* Designed for extensible Bronze/Silver/analytics layers

---

# 🏗️ Architecture

HAADE uses Kubernetes as the underlying orchestration platform.

```text
┌─────────────────────────────────────────────────────────────┐
│                         Kubernetes                           │
│                                                             │
│   ┌─────────────────┐                                      │
│   │ Kafka Cluster   │                                      │
│   │                 │                                      │
│   │ fintech.debt.raw│                                      │
│   └────────┬────────┘                                      │
│            │                                                │
│            ▼                                                │
│   ┌──────────────────────────┐                              │
│   │ Spark Operator           │                              │
│   │                          │                              │
│   │ Spark Structured         │                              │
│   │ Streaming Application    │                              │
│   └────────────┬─────────────┘                              │
│                │                                            │
│                ▼                                            │
│       ┌─────────────────┐                                  │
│       │ Apache Iceberg  │                                  │
│       │                 │                                  │
│       │ Bronze Tables   │                                  │
│       └────────┬────────┘                                  │
│                │                                            │
│                ▼                                            │
│       ┌─────────────────┐                                  │
│       │     MinIO       │                                  │
│       │                 │                                  │
│       │ S3-Compatible   │                                  │
│       │ Object Storage  │                                  │
│       └─────────────────┘                                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘

             │
             ▼
      Observability
      Prometheus / Metrics
```

---

# 📡 Streaming Pipeline

The primary HAADE pipeline consumes events from Kafka and writes them into an Iceberg table.

```text
Kafka Topic
    │
    │ JSON events
    ▼
Spark Structured Streaming
    │
    ├── Parse JSON
    ├── Extract fields
    ├── Add ingestion timestamp
    │
    ▼
Iceberg Bronze Table
    │
    ▼
MinIO Object Storage
```

---

# 📨 Kafka

Kafka is used as the event streaming platform.

The project uses the Kafka topic:

```text
fintech.debt.raw
```

This topic represents the raw incoming event stream.

A typical event contains fields such as:

```json
{
  "source": "example-source",
  "title": "Example event",
  "timestamp": "2026-05-19T10:00:00Z"
}
```

The streaming application consumes these events continuously.

---

# ⚡ Spark Structured Streaming

Apache Spark is responsible for processing the Kafka stream.

The project uses:

```text
Apache Spark 3.5.0
```

The Spark application:

1. Connects to Kafka
2. Reads the event stream
3. Parses JSON messages
4. Extracts relevant fields
5. Adds an ingestion timestamp
6. Writes records into Iceberg
7. Maintains a streaming checkpoint

The main streaming application is:

```text
/opt/spark/jobs/streaming/kafka_to_iceberg.py
```

---

# 🔄 Kafka → Spark → Iceberg

The core processing logic can be represented as:

```text
Kafka
  │
  │ fintech.debt.raw
  ▼
Spark Structured Streaming
  │
  ├── source
  ├── title
  ├── timestamp
  │
  └── ingested_at
          │
          ▼
    Apache Iceberg
          │
          ▼
      MinIO/S3
```

The `ingested_at` field records when the event entered the data lake pipeline.

---

# 🧊 Apache Iceberg

Apache Iceberg provides the table layer for the lakehouse.

Instead of writing raw files directly to object storage without table management, HAADE uses Iceberg to provide a structured table abstraction over the underlying data.

The initial Bronze table is:

```text
local.bronze_events
```

The Bronze layer represents data close to its original source format, with only the transformations necessary for ingestion and basic normalization.

---

# 🥉 Bronze Layer

The current streaming pipeline writes incoming events to:

```text
local.bronze_events
```

The Bronze layer contains fields extracted from the incoming Kafka messages along with ingestion metadata.

Conceptually:

```text
Bronze
┌──────────────────────────────┐
│ source                       │
│ title                        │
│ timestamp                    │
│ ingested_at                  │
└──────────────────────────────┘
```

The architecture can later be extended with additional processing layers.

```text
Raw Events
    │
    ▼
 Bronze
    │
    ▼
 Silver
    │
    ▼
 Gold / Analytics
```

---

# 💾 MinIO

MinIO provides S3-compatible object storage for the lakehouse.

The Kubernetes deployment runs MinIO in the:

```text
lakehouse
```

namespace.

The MinIO service is:

```text
minio
```

and uses port:

```text
9000
```

The project uses a bucket named:

```text
warehouse
```

The general storage architecture is:

```text
Iceberg
   │
   ▼
S3 API
   │
   ▼
MinIO
   │
   ▼
warehouse/
```

This allows the local development environment to behave similarly to an S3-backed data lake.

---

# 📍 Streaming Checkpoints

Spark Structured Streaming maintains checkpoints so that the streaming job can track its progress.

The Bronze pipeline uses:

```text
s3a://warehouse/checkpoints/bronze
```

Checkpointing is important because it allows Spark to maintain streaming state and recover from failures.

```text
Kafka
  │
  ▼
Spark
  │
  ├── Process events
  │
  └── Checkpoint
          │
          ▼
        MinIO
```

---

# ☸️ Kubernetes

HAADE is designed to run natively on Kubernetes.

The Kubernetes layer manages:

* Kafka
* Spark workloads
* MinIO
* Supporting services
* Networking
* Persistent resources
* Application lifecycle

The project separates infrastructure and workloads into Kubernetes namespaces where appropriate.

The primary lakehouse namespace is:

```text
lakehouse
```

---

# 🔥 Kafka on Kubernetes

Kafka is deployed inside Kubernetes.

The Spark application connects to the Kafka bootstrap service through the Kubernetes network.

The Spark application uses the Kafka bootstrap endpoint:

```text
recovery-kafka-kafka-bootstrap.kafka.svc.cluster.local
```

This allows Spark to communicate with Kafka using Kubernetes service discovery instead of hardcoded external addresses.

---

# 🧑‍🚀 Spark Operator

Spark applications are deployed through the Kubernetes Spark Operator.

The streaming job is represented as a Kubernetes `SparkApplication`.

Conceptually:

```text
Kubernetes
    │
    ▼
SparkApplication
    │
    ▼
Spark Operator
    │
    ▼
Spark Driver
    │
    ▼
Spark Executors
```

The application runs in:

```text
cluster mode
```

using Python.

---

# 🐍 PySpark Application

The main streaming job is implemented in Python.

```text
services/
    ...
```

The Spark job is responsible for the Kafka-to-Iceberg ingestion pipeline.

The deployed Spark application references:

```text
/opt/spark/jobs/streaming/kafka_to_iceberg.py
```

---

# 📦 Docker

Spark workloads are containerized using Docker.

A Spark image is built on top of:

```text
apache/spark
```

The image contains the dependencies required by the streaming application.

Important dependencies include:

* Spark SQL Kafka connector
* Apache Iceberg Spark runtime
* Hadoop AWS/S3A support

These dependencies allow Spark to communicate with both Kafka and S3-compatible storage.

---

# 🔗 Spark Dependencies

The streaming application requires integration between several systems:

```text
Spark
 │
 ├── Kafka Connector
 │
 ├── Iceberg Runtime
 │
 └── Hadoop AWS / S3A
          │
          ▼
        MinIO
```

This enables Spark to consume Kafka messages and persist Iceberg data to MinIO.

---

# 🛠️ Development Workflow

HAADE uses **Skaffold** to simplify Kubernetes development.

Skaffold can be used to:

* Build container images
* Deploy Kubernetes resources
* Iterate quickly during development
* Manage the development lifecycle

Typical commands include:

```bash
skaffold run
```

and:

```bash
skaffold dev
```

---

# 🔄 Skaffold Development Flow

```text
Source Code
    │
    ▼
Skaffold
    │
    ├── Build Docker Image
    │
    ├── Deploy Kubernetes Resources
    │
    └── Monitor Changes
            │
            ▼
        Kubernetes
```

---

# 🚀 GitOps with Argo CD

The project is designed to support GitOps-style Kubernetes deployment using **Argo CD**.

The desired deployment model is:

```text
Git Repository
      │
      ▼
   Argo CD
      │
      ▼
 Kubernetes Cluster
      │
      ├── Kafka
      ├── Spark
      ├── Iceberg
      └── MinIO
```

Changes committed to the repository can therefore become the source of truth for Kubernetes deployment configuration.

---

# 📊 Observability

The project includes metrics and observability components for monitoring services.

Prometheus is used to collect application metrics.

The ingestion service exposes metrics that can be monitored by Prometheus.

The architecture is:

```text
Application
    │
    ▼
Metrics Endpoint
    │
    ▼
Prometheus
    │
    ▼
Monitoring Dashboard
```

Kubernetes `ServiceMonitor` resources can be used to automatically discover metric endpoints.

---

# 📁 Project Structure

The project follows a service-oriented structure.

A simplified structure is:

```text
HAADE/
│
├── services/
│   │
│   ├── ingestion/
│   │   └── app/
│   │       ├── ...
│   │       └── run.py
│   │
│   ├── consumers/
│   │   └── trend_consumer/
│   │       └── app/
│   │           ├── ...
│   │           └── processor.py
│   │
│   └── shared/
│       ├── metrics/
│       ├── kafka/
│       └── logger/
│
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       └── streaming/
│           └── kafka_to_iceberg.py
│
├── k8s/
│   ├── kafka/
│   ├── spark/
│   ├── iceberg/
│   ├── minio/
│   └── ...
│
├── skaffold.yaml
│
└── README.md
```

The exact directory structure may evolve as additional lakehouse layers and services are added.

---

# 🧩 Shared Services

Common functionality is separated into shared modules.

```text
services/shared/
├── metrics/
├── kafka/
└── logger/
```

This avoids duplicating common infrastructure logic across services.

---

# 📈 Data Flow

The complete current data flow is:

```text
                  DATA PRODUCER
                       │
                       ▼
              ┌─────────────────┐
              │      Kafka      │
              │                 │
              │ fintech.debt.raw│
              └────────┬────────┘
                       │
                       ▼
          ┌──────────────────────────┐
          │ Spark Structured         │
          │ Streaming                │
          │                          │
          │ JSON Parsing             │
          │ Field Extraction         │
          │ Timestamp Enrichment     │
          └────────────┬─────────────┘
                       │
                       ▼
                ┌─────────────┐
                │   Iceberg   │
                │             │
                │ bronze_     │
                │ events      │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │    MinIO    │
                │             │
                │  warehouse  │
                └─────────────┘
```

---

# 🧪 Example Event Processing

Suppose Kafka receives:

```json
{
  "source": "news",
  "title": "Debt market update",
  "timestamp": "2026-05-19T10:00:00Z"
}
```

Spark reads and transforms the event.

The resulting record contains:

```json
{
  "source": "news",
  "title": "Debt market update",
  "timestamp": "2026-05-19T10:00:00Z",
  "ingested_at": "2026-05-19T10:00:05Z"
}
```

The record is then appended to:

```text
local.bronze_events
```

with the associated Iceberg data stored in the MinIO warehouse.

---

# 🗂️ Lakehouse Architecture

The current implementation focuses on the Bronze ingestion layer.

The architecture is designed to evolve toward:

```text
                 Kafka
                   │
                   ▼
               Spark
                   │
                   ▼
             ┌───────────┐
             │  Bronze   │
             │  Iceberg  │
             └─────┬─────┘
                   │
                   ▼
             ┌───────────┐
             │  Silver   │
             │  Iceberg  │
             └─────┬─────┘
                   │
                   ▼
             ┌───────────┐
             │   Gold    │
             │ Analytics │
             └─────┬─────┘
                   │
                   ▼
          Query / Visualization
```

This separation allows raw ingestion to remain independent from downstream transformations.

---

# 🔍 Analytics Layer

The project architecture is designed to support analytical querying over Iceberg data.

A future/current analytical layer can use tools such as:

* Trino
* SQL-based querying
* Apache Superset

Conceptually:

```text
Iceberg
   │
   ▼
 Trino
   │
   ▼
Superset
   │
   ▼
Dashboards
```

This allows the lakehouse storage layer to be separated from the query and visualization layers.

---

# 🧱 Technology Stack

| Category           | Technology         |
| ------------------ | ------------------ |
| Language           | Python             |
| Stream Processing  | Apache Spark 3.5.0 |
| Streaming Platform | Apache Kafka       |
| Table Format       | Apache Iceberg     |
| Object Storage     | MinIO              |
| Storage Protocol   | S3 / S3A           |
| Orchestration      | Kubernetes         |
| Spark Deployment   | Spark Operator     |
| Containerization   | Docker             |
| Development        | Skaffold           |
| GitOps             | Argo CD            |
| Metrics            | Prometheus         |
| Query Engine       | Trino              |
| Visualization      | Apache Superset    |

---

# 🎯 Engineering Concepts Demonstrated

HAADE demonstrates several important data engineering and infrastructure concepts.

### Real-Time Data Ingestion

Kafka provides a durable event-streaming layer between producers and processing workloads.

### Stream Processing

Spark Structured Streaming continuously processes incoming Kafka events.

### Data Lakehouse

Apache Iceberg provides a structured table abstraction over object storage.

### Object Storage

MinIO provides S3-compatible storage for local/on-premite workflow for accepting video-processing jobs from external applications, managing them through a controlled asynchronous queue, encoding videos with FFmpeg, storing results in S3-compatible storage, tracking job status, notifying external systems through callbacks, and providing administrators with tools for managing users, encoding profiles, jobs, monitoring, and audit trails.se lakehouse deployments.

### Kubernetes-Native Processing

Spark jobs are deployed and managed as Kubernetes workloads.

### Infrastructure as Code / Declarative Deployment

Kubernetes manifests define infrastructure and application resources declaratively.

### GitOps

Argo CD provides a path toward Git-based Kubernetes deployment management.

### Observability

Prometheus metrics provide visibility into application behavior and service health.

---

# 🏃 Running the Project

## Prerequisites

The development environment requires:

* Docker
* Kubernetes
* kubectl
* Helm
* Skaffold
* Apache Spark Operator
* Kafka
* MinIO

Depending on the deployment configuration, additional Kubernetes components may be required.

---

# ☸️ Kubernetes

Verify the Kubernetes cluster:

```bash
kubectl cluster-info
```

Check nodes:

```bash
kubectl get nodes
```

---

# 📦 Check Namespaces

For the lakehouse components:

```bash
kubectl get namespaces
```

The project uses:

```text
lakehouse
```

for lakehouse infrastructure.

---

# 🪣 Verify MinIO

Check the MinIO service:

```bash
kubectl get svc -n lakehouse
```

The MinIO service is:

```text
minio
```

on port:

```text
9000
```

---

# 📡 Verify Kafka

Check Kafka resources:

```bash
kubectl get pods -n kafka
```

Verify the Kafka topic:

```text
fintech.debt.raw
```

---

# ⚡ Deploy Spark Application

The Kafka-to-Iceberg workload is deployed as a Kubernetes `SparkApplication`.

The Spark job runs in cluster mode using Spark 3.5.0.

The main application is:

```text
/opt/spark/jobs/streaming/kafka_to_iceberg.py
```

---

# 🔎 Monitor Spark

Check Spark applications:

```bash
kubectl get sparkapplications -A
```

Check the driver pod:

```bash
kubectl get pods
```

View driver logs:

```bash
kubectl logs <spark-driver-pod>
```

---

# 📊 Monitoring

Prometheus can be queried to verify application metrics.

For example, Kubernetes monitoring resources can be inspected using:

```bash
kubectl get servicemonitor -A
```

---

# 🐛 Troubleshooting

## Spark cannot find Iceberg

Make sure the Spark application includes the appropriate Iceberg Spark runtime dependency and configures the Iceberg Spark session extensions.

The Spark session needs Iceberg integration for operations such as:

```text
Spark
  │
  ▼
IcebergSparkSessionExtensions
  │
  ▼
Iceberg Tables
```

---

## Spark cannot read Kafka

Make sure the Spark deployment includes the Kafka SQL connector:

```text
spark-sql-kafka-0-10
```

The Kafka bootstrap server must also point to the Kubernetes Kafka service.

---

## Spark cannot access MinIO

Verify that S3A/Hadoop AWS dependencies are available.

The Spark environment needs S3A support to communicate with MinIO.

Check:

```text
hadoop-aws
```

and the corresponding S3 filesystem configuration.

---

# 🔐 Data Storage Configuration

The lakehouse uses S3-compatible storage.

Conceptually:

```text
s3a://warehouse/
```

is the storage location.

Streaming checkpoints are stored at:

```text
s3a://warehouse/checkpoints/bronze
```

while Iceberg table data is stored in the warehouse.

---

# 🛣️ Roadmap

Potential future development includes:

* [ ] Complete Silver layer
* [ ] Gold analytical layer
* [ ] Data quality validation
* [ ] Schema evolution workflows
* [ ] Iceberg partitioning optimization
* [ ] Trino integration
* [ ] Superset dashboards
* [ ] Kafka schema management
* [ ] Advanced Spark transformations
* [ ] Data lineage
* [ ] Data quality monitoring
* [ ] Improved Prometheus dashboards
* [ ] Automated GitOps deployment
* [ ] Production-grade Kafka configuration
* [ ] Production lakehouse security

---

# 🧠 Why This Project?

HAADE was built to explore how a modern data platform can be constructed entirely from open-source technologies.

Instead of treating Kafka, Spark, object storage, and analytical databases as isolated components, the project connects them into a complete data pipeline:

```text
                    Streaming
                       │
                       ▼
                    Kafka
                       │
                       ▼
                 Spark Streaming
                       │
                       ▼
                 Apache Iceberg
                       │
                       ▼
                     MinIO
                       │
                       ▼
              Analytical Query Layer
                       │
                       ▼
                  Visualization
```

The project also explores deploying these components in a **Kubernetes-native environment**, providing a foundation for scalable and reproducible data infrastructure.

---

# 👨‍💻 Author

**Sanchar Panthi**

Backend / Data Engineering

Areas of interest:

* Data Engineering
* Distributed Systems
* Python
* Kafka
* Apache Spark
* Apache Iceberg
* Kubernetes
* Cloud-Native Infrastructure
* Real-Time Data Processing
* Data Lakehouse Architecture

---





---

# ⭐ Project Summary

**HAADE** is a Kubernetes-based real-time data lakehouse that demonstrates:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Apache Iceberg
  ↓
MinIO
  ↓
Analytics
```

with the surrounding infrastructure provided by:

```text
Kubernetes
Docker
Spark Operator
Skaffold
Argo CD
Prometheus
Trino
Superset
```

The project focuses on building a **reproducible, containerized, Kubernetes-native data engineering pipeline** capable of receiving streaming events, processing them continuously, storing them in an open table format, and preparing the resulting data for downstream analytics.
