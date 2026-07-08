import os
import sys
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, length, current_timestamp, lit
from delta import configure_spark_with_delta_pip
from google.cloud import bigquery  # ─── Added for operational metadata auditing

def init_spark_session():
    """Initializes a local Spark Session with advanced high-throughput GCS tuning."""
    print("⚙️ Building Delta-enabled Spark Session with Shaded GCS Connector...")
    
    gcs_shaded_jar_url = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.14/gcs-connector-hadoop3-2.2.14-shaded.jar"
    
    builder = SparkSession.builder \
        .appName("FineWeb_Silver_Processing") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.driver.memory", "2560m") \
        .config("spark.executor.memory", "2048m") \
        .config("spark.jars", gcs_shaded_jar_url) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.abstract_filesystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT") \
        .config("spark.hadoop.fs.gs.output.buffer.type", "BYTEBUFFER") \
        .config("spark.hadoop.fs.gs.write.chunk.size", "67108864")
        
    print("✅ Spark Session successfully bound to Delta Lake Engine with 2.5G Heap!")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def process_bronze_to_silver():
    spark = init_spark_session()

    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    print(f"🎬 Processing data in [{env_mode}] mode")

    if env_mode == "PROD":
        bucket_name = os.getenv("GCS_BRONZE_BUCKET")
        input_path = f"gs://{bucket_name}/bronze/fineweb/*.parquet"
        output_path = f"gs://{bucket_name}/silver/fineweb_cleaned"
        dlq_path = f"gs://{bucket_name}/dlq/fineweb_errors"
    else:
        input_path = "/opt/airflow/data/bronze/sample/10BT/*.parquet"
        output_path = "/opt/airflow/data/silver/fineweb_cleaned"
        dlq_path = "/opt/airflow/data/dlq/fineweb_errors"

    # 1. Read Raw Bronze Data
    print(f"📖 Reading raw Parquet data from: {input_path}")
    try:
        df_raw = spark.read.parquet(input_path)
        if env_mode == "PROD":
            print("⚠️ DEV-BATCH: Limiting cloud input to 10,000 rows to bypass network latency.")
            df_raw = df_raw.limit(10000)
    except Exception as e:
        print(f"❌ Failed to read Bronze Data. Ensure file exists. Error: {str(e)}")
        sys.exit(1)

    print("📊 Source Schema Detected:")
    df_raw.printSchema()

    # 2. Execute LLM Text Cleaning Operations
    print("🧼 Executing text normalization and data processing...")
    df_transformed = df_raw \
        .withColumn("cleaned_text", trim(col("text"))) \
        .withColumn("cleaned_text", regexp_replace(col("cleaned_text"), r"\s+", " "))

    # 3. Define Quality Gate Invariants
    print("🛡️ Evaluating programmatic quality gates...")
    valid_record_condition = (
        col("id").isNotNull() & 
        col("cleaned_text").isNotNull() & 
        (length(col("cleaned_text")) > 200) & 
        (col("int_score") >= 0) & (col("int_score") <= 5)
    )

    # Conditional Branching
    df_silver_good = df_transformed.filter(valid_record_condition)
    df_silver_bad = df_transformed.filter(~valid_record_condition) \
        .withColumn("rejection_reason", lit("Failed Invariant Verification (Null values, short text, or out-of-bounds score)")) \
        .withColumn("rejected_at", current_timestamp())

    # 4. Write Atomic Transformed Records to Silver Delta Table
    print(f"💾 Writing hyper-pure records to Silver Delta Lake at: {output_path}")
    df_silver_good.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    # 5. Write Corrupted Records to Side-Channel DLQ
    print(f"🚨 Routing anomalies to Dead Letter Queue at: {dlq_path}")
    df_silver_bad.write \
        .format("parquet") \
        .mode("overwrite" if env_mode == "DEV" else "append") \
        .save(dlq_path)

    total_good = df_silver_good.count()
    total_bad = df_silver_bad.count()
    
    print(f"🎉 Silver Layer Processing Complete! Clean: {total_good} | Isolated: {total_bad}")

    # 6. METADATA AUDITING & BIGQUERY LOGGING (Step 6.3)
    if env_mode == "PROD":
        print("☁️ PROD Mode: Shipping operational audit metadata to BigQuery...")
        
        project_id = os.getenv("GCP_PROJECT_ID", "catalyst-chain-project")
        bq_client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.fineweb_gold_layer.pipeline_audit_logs"
        
        # Package metrics into a clean audit schema
        audit_entry = [{
            "job_name": "silver_clean_processing",
            "execution_timestamp": datetime.utcnow().isoformat(),
            "env_mode": env_mode,
            "records_processed_clean": int(total_good),
            "records_rejected_dlq": int(total_bad),
            "status": "SUCCESS"
        }]
        
        audit_df = pd.DataFrame(audit_entry)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        try:
            log_job = bq_client.load_table_from_dataframe(audit_df, table_id, job_config=job_config)
            log_job.result()
            print(f"📊 Audit metadata successfully appended to BigQuery logs: {table_id}")
        except Exception as e:
            print(f"⚠️ Audit logging telemetry failed to ship, but data job succeeded. Error: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()