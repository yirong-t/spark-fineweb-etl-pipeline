import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments applied to all tasks in this pipeline
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id="fineweb_data_ingestion",
    default_args=default_args,
    description="Automated pipeline to ingest FineWeb-Edu Parquet shards from Hugging Face",
    schedule_interval=None,  # Trigger manually for development
    catchup=False,
    tags=["control_plane", "ingestion", "bronze"],
) as dag:

    # Task 1: Execute Ingestion Engine via Bash
    ingest_raw_data = BashOperator(
        task_id="ingest_hf_to_bronze",
        bash_command="python /opt/airflow/src/utils/ingestion.py",
        env={
            **os.environ,  # Inherit existing system environment variables (like Google ADC keys)
            "ENV_MODE": os.getenv("ENV_MODE", "DEV"),  # Passes active environment mode downstream
            "GCS_BRONZE_BUCKET": os.getenv("GCS_BRONZE_BUCKET", "fineweb-bronze-storage-bucket"),
        },
    )

    ingest_raw_data