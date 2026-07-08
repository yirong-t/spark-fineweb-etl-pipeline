# Pipeline Project: FineWeb-Edu

This is a Medallion Architecture data platform engineered to ingest, transform, optimize, and audit massive web-scraped corpora for Large Language Model (LLM) pre-training.


---

## 🏗️ System Architecture & Data Flow

The platform implements a unified 5-stage automated assembly line governed by Apache Airflow:

```
[HuggingFace API] ──(Ingestion)──> [GCS Bronze Layer] (.parquet)
                                           │
                                    (Quality Gates) ──(Failures)──> [GCS DLQ Bucket]
                                           │
                                           ▼
[GCS BigQuery Logs] <──(Auditing)── [GCS Silver Layer] (Delta Lake ACID)
                                           │
                                     (Compaction) ──> [Delta OPTIMIZE / Layout Packing]
                                           │
                                           ▼
[Google BigQuery] <──(Circuit Breaker)── [GCS Gold Layer] (Pandas SDK Aggregation)

```

1. **Bronze Layer (Ingestion)**: Distributed shard ingestion streaming raw `.parquet` extracts from HuggingFace repositories into a Google Cloud Storage (GCS) object store landing zone.
2. **Silver Layer (Defensive Cleaning)**: A high-throughput PySpark engine evaluates strict programmatic quality gates, executing text regex normalizations, isolating corrupted text blocks into a side-channel Dead Letter Queue (DLQ), and logging operational metrics.
3. **Storage Compaction (FinOps)**: Executes Delta Lake transactional optimization mechanisms to collapse highly fragmented, kilobyte-sized files into dense 128MB uniform chunks, permanently eliminating the "Small File Problem".
4. **Gold Layer (Warehouse Aggregation)**: Compiles highly curated analytical matrices and domain quality profiles, streaming structural assets directly into Google BigQuery.
5. **Observability Gate (Data Diffing)**: Runs post-aggregation macro-statistical evaluation loops against historical baselines, executing a fail-fast circuit breaker if upstream data drift or structural dataset poisoning is detected.

---

## 🛠️ Tech Stack & Core Infrastructure

| Component | Technology | Operational Role |
| --- | --- | --- |
| **Control Plane** | Apache Airflow (v2.x) | Workload Orchestration & Multi-Tier DAG Management |
| **Virtualization** | Docker / Docker Compose | Isolated Cluster Environment & Resource Caps |
| **Compute Engine** | PySpark (v3.x) | Distributed Processing Core with Shaded GCS Overrides |
| **Storage Engine** | Delta Lake (v3.x) | ACID Transaction Management & Storage Layout Controls |
| **Cloud Data Lake** | Google Cloud Storage (GCS) | Low-Cost Compute-Separated Object Store Storage |
| **Data Warehouse** | Google BigQuery | High-Performance Downstream Data Warehousing & Observability |

---

## 📁 Repository Directory Structure

```text
├── dags/
│   └── fineweb_lakehouse_pipeline.py    # Master Airflow Directed Acyclic Graph
├── src/
│   ├── utils/
│   │   └── ingestion.py                 # Shard extractor and cloud storage staging
│   ├── processing/
│   │   ├── silver_clean.py              # PySpark Quality Gates & DLQ分流 mechanism
│   │   ├── lakehouse_optimize.py        # Delta layout compaction & storage tuning
│   │   └── gold_load.py                 # Token aggregation & BigQuery analytical loader
│   └── monitoring/
│       └── data_diff.py                 # Macro-statistical drift circuit breaker
├── config/                              # Local structural configuration assets
├── data/                                # Local volume mount fallback for DEV testing
└── docker-compose.yaml                  # Unified multi-container cluster topology definition

```

---

## 🛡️ Key Engineering Configurations & Implementation Highlights

### 1. Programmatic Quality Gates & Dead Letter Queue (DLQ)

Rather than allowing corrupted rows or unexpected schema drift to break cluster execution, the Silver pipeline utilizes a split-channel boolean condition array. Pristine records flow forward, while anomalous files are appended with structural context (`rejection_reason`, `rejected_at`) and routed to an isolated directory.

### 2. High-Throughput Cloud I/O Customizations

To combat the local-to-cloud network upload bottleneck during hybrid execution, the PySpark engine explicitly overrides standard Hadoop file system drivers, forcing direct-memory buffers and inflating TCP pipeline payloads:

* `spark.hadoop.fs.gs.output.buffer.type`: `BYTEBUFFER`
* `spark.hadoop.fs.gs.write.chunk.size`: `67108864` (64MB streaming parts)

### 3. Automated Telemetry & Drift Observability

The platform treats operational visibility as a primary invariant. The `silver_clean.py` engine ships real-time metric arrays (`records_processed_clean`, `records_rejected_dlq`) directly to a cloud monitoring warehouse catalog (`pipeline_audit_logs`). Concurrently, the final block operates a macro-level **Data Drift Circuit Breaker**, computing text length and quality distributions against strict historical thresholds to halt downstream poisoning.

---

## 🚀 Deployment & Operational Lifecycle

### Prerequisites

* Docker & Docker Compose installed locally.
* Google Cloud Platform (GCP) Service Account with `Storage Object Admin` and `BigQuery Data Editor` access.
* Local Google Application Default Credentials (ADC) initialized via `gcloud auth application-default login`.

### 1. Environment Configuration

Environment settings are fully decoupled and governed inside the centralized `docker-compose.yaml` system-level anchor variables:

```yaml
# Set to 'DEV' to run exclusively on local disk volumes
# Set to 'PROD' to route processing directly across GCS & BigQuery
ENV_MODE: PROD
GCS_BRONZE_BUCKET: your-global-gcs-bucket-name
GCP_PROJECT_ID: your-gcp-project-id
AIRFLOW__WEBSERVER__SECRET_KEY: 'your-synchronized-cluster-hex-key'

```

### 2. Ignition & Bootstrapping

Spin up the coordinated multi-container engine stack (PostgreSQL back-end database, Airflow Scheduler, and Airflow Webserver server nodes) in detached execution mode:

```bash
docker compose down && docker compose up -d

```

### 3. Executing Workloads

1. Access the orchestration control center by navigating to `http://localhost:8080` (Credentials: `airflow`/`airflow`).
2. Locate the `fineweb_lakehouse_pipeline` DAG. The UI dashboard will dynamically display a `PROD` or `DEV` environment tag according to your configuration setup.
3. Unpause the pipeline toggle and click the **Trigger DAG** play icon to launch the automated workflow.

---

## ⚖️ Architectural Trade-offs & Operational Guardrails

* **Fail-Fast Operations over Maximum Uptime**: In traditional data engineering, maintaining pipeline uptime is critical. However, within LLM pre-training environments, content degradation represents a severe risk. The system prioritizes a **Fail-Fast Circuit Breaker strategy**—meaning it deliberately shuts down via automated exceptions if semantic quality parameters skew, controlling the blast radius before toxic rows pollute training data.
* **Staging Compute Localization via FinOps**: To avoid expensive cloud computing infrastructure costs during development, this platform runs containerized Spark jobs on local hardware while mapping inputs directly to remote cloud object stores. The resultant broadband upload throughput limitations are mitigated by deploying explicit `DEV-BATCH` data limits (`.limit(10000)`) to quickly test system integrations without processing terabytes of data locally.