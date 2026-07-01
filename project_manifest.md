# Project Manifest: SparkFinewebProject

## System State
- Python: 3.10 via `uv`
- Spark Engine: PySpark 3.5.3 + Delta-Spark 3.3.2
- Target Data: FineWeb-Edu (Sample-10B Parquet)
- Core Infrastructure: Docker Local Orchestration + GCP Storage/BigQuery Free Tier

## Finished Components
- **Phase 1, Step 1.1**: Local directory blueprinted and safe Git boundaries (`.gitignore`) established. Python virtual environment locked using `uv`.
- **Phase 1, Step 1.2**: Resource-constrained multi-container sandbox deployed via Docker Compose (Airflow 2.8.1 with LocalExecutor, PostgreSQL metadata database, and embedded OpenJDK-17). Bi-directional host-to-container volume mappings completed.
- **Phase 1, Step 1.3**: Core architecture validated under the separation of Control Plane (Airflow & Metadata DB) and Data Plane (GCS & BigQuery) design pattern. The local scheduling control center is fully operational.
- **Phase 2, Step 2.1**: Adopted Infrastructure as Code (IaC). Provisioned the Bronze Data Lake (GCS) and Gold Data Warehouse (BigQuery) via Terraform utilizing ADC passthrough. Implemented 3-day FinOps auto-deletion lifecycle rules on raw storage.
- **Phase 2, Step 2.1b**: Updated safe Git boundaries by appending `.terraform/` and system state locks to the local `.gitignore` matrix.
- **Phase 2, Step 2.2**: Constructed modular, environment-aware Ingestion Engine (`src/utils/ingestion.py`). Implemented decoupled routing logic separating local DEV storage paths from PROD GCS cloud storage upload layers.
- **Phase 2, Step 2.3**: Orchestrated ingestion engine via Apache Airflow (`dags/fineweb_ingestion_dag.py`). Utilized decoupled BashOperator executing isolated subprocess models with environment configurations passed dynamically at execution runtime.
- **Phase 3, Step 3.1**: Engineered Delta-Enabled Spark Session core configuration (`src/processing/silver_clean.py`). Integrated Java extension catalog mappings and constructed a distributed quality-filtering pipeline writing to local transactional Delta Lake tables.