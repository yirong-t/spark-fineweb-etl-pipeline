import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


current_env = os.getenv("ENV_MODE", "DEV").upper()

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
    dag_id="fineweb_lakehouse_pipeline",
    default_args=default_args,
    description=f"End-to-end FineWeb Pipeline running in [{current_env}] mode",
    schedule_interval=None,
    catchup=False,

    tags=["spark", "delta", "lakehouse", current_env],
) as dag:

    # Task 1: Ingest Raw Parquet Shards 
    ingest_raw_data = BashOperator(
        task_id="ingest_hf_to_bronze",
        bash_command="python /opt/airflow/src/utils/ingestion.py",
    
    )

    # Task 2: Distributed Spark Cleaning & Delta Lake Packaging
    clean_and_transform_silver = BashOperator(
        task_id="process_bronze_to_silver_spark",
        bash_command="python /opt/airflow/src/processing/silver_clean.py",
    )

    # Task 3: Token Estimation & Domain Profiling (Gold Layer)
    load_gold_metrics = BashOperator(
        task_id="generate_gold_profile_warehouse",
        bash_command="python /opt/airflow/src/processing/gold_load.py",
    )

    # task 4: Lakehouse optimization &  Compaction
    optimize_storage = BashOperator(
        task_id="optimize_lakehouse_storage_finops",
        bash_command="python /opt/airflow/src/processing/lakehouse_optimize.py",
    )

    # Set Up Streamflow Dependency Matrix
    ingest_raw_data >> clean_and_transform_silver >> optimize_storage >> load_gold_metrics