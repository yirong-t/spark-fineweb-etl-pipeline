import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fineweb_lakehouse_pipeline",  # Renamed to reflect unified pipeline
    default_args=default_args,
    description="End-to-end FineWeb-Edu pipeline: Ingestion (Bronze) and Spark Cleaning (Silver)",
    schedule_interval=None,
    catchup=False,
    tags=["control_plane", "spark", "delta", "lakehouse"],
) as dag:

    # Task 1: Ingest Raw Parquet Shards (HuggingFace -> Local Disk / GCS)
    ingest_raw_data = BashOperator(
        task_id="ingest_hf_to_bronze",
        bash_command="python /opt/airflow/src/utils/ingestion.py",
        env={
            **os.environ,
            "ENV_MODE": os.getenv("ENV_MODE", "DEV"),
            "GCS_BRONZE_BUCKET": os.getenv("GCS_BRONZE_BUCKET", "YOUR_GCS_BUCKET_NAME"),
        },
    )

    # Task 2: Distributed Spark Cleaning & Delta Lake Packaging
    clean_and_transform_silver = BashOperator(
        task_id="process_bronze_to_silver_spark",
        bash_command="python /opt/airflow/src/processing/silver_clean.py",
        env={
            **os.environ,
            "ENV_MODE": os.getenv("ENV_MODE", "DEV"),
            "GCS_BRONZE_BUCKET": os.getenv("GCS_BRONZE_BUCKET", "YOUR_GCS_BUCKET_NAME"),
        },
    )
    # Task 3: Token Estimation & Domain Profiling (Gold Layer)
    load_gold_metrics = BashOperator(
        task_id="generate_gold_profile_warehouse",
        bash_command="python /opt/airflow/src/processing/gold_load.py",
        env={
            **os.environ,
            "ENV_MODE": os.getenv("ENV_MODE", "DEV"),
            "GCS_BRONZE_BUCKET": os.getenv("GCS_BRONZE_BUCKET", "YOUR_GCS_BUCKET_NAME"),
        },
    )

    # Set Up Streamflow Dependency Matrix
    ingest_raw_data >> clean_and_transform_silver >> load_gold_metrics