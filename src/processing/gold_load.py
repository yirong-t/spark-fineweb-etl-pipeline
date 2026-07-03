import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, avg, count
from delta import configure_spark_with_delta_pip
from google.cloud import bigquery

def init_spark_session():
    """Initializes a local Spark Session explicitly configured for Delta Lake and Shaded GCS."""
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
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        
    print("✅ Spark Session successfully bound to Delta Lake Engine with 2.5G Heap!")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_gold_pipeline():
    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    print(f"🏆 Initializing Gold Layer Pipeline in [{env_mode}] Mode")

    spark = init_spark_session()

    # 1. Set Environment Paths
    if env_mode == "PROD":
        bucket_name = os.getenv("GCS_BRONZE_BUCKET")
        silver_path = f"gs://{bucket_name}/silver/fineweb_cleaned"
    else:
        silver_path = "/opt/airflow/data/silver/fineweb_cleaned"

    print(f"📖 Reading Silver Delta table from: {silver_path}")
    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as e:
        print(f"❌ Failed to read Silver Delta table. Error: {str(e)}")
        sys.exit(1)

    # 2. Compute Highly Curated Gold Metrics (Approximate token distributions by URL domain)
    print("📊 Compiling LLM dataset profile and computing token statistics...")
    df_gold_metrics = df_silver \
        .withColumn("domain", split(col("url"), "/")[2]) \
        .withColumn("approx_tokens", size(split(col("cleaned_text"), " "))) \
        .groupBy("domain") \
        .agg(
            count("id").alias("total_documents"),
            avg("approx_tokens").alias("avg_tokens_per_doc"),
            avg("int_score").alias("avg_educational_score")
        ) \
        .filter(col("total_documents") > 1) \
        .orderBy(col("total_documents").desc())

    # 3. Route and Load Data to Warehouse
    if env_mode == "PROD":
        print("☁️ PROD Mode: Shipping Gold Metrics directly to BigQuery...")
        # Convert the aggregated summary to Pandas for standard BigQuery SDK ingest
        pandas_df = df_gold_metrics.toPandas()

        project_id = os.getenv("GCP_PROJECT_ID", "catalyst-chain-project")
        bq_client = bigquery.Client(project=project_id)

        table_id = f"{project_id}.fineweb_gold_layer.domain_quality_profile"

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bq_client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
        job.result() # Wait for table creation to complete
        print(f"✅ Successfully loaded {len(pandas_df)} warehouse records to BigQuery: {table_id}")
    else:
        local_output = "/opt/airflow/data/gold/domain_quality_profile"
        print(f"🏠 DEV Mode: Saving Gold metrics locally to: {local_output}")
        df_gold_metrics.write.mode("overwrite").parquet(local_output)
        print("✅ Local Gold Parquet file successfully generated.")

    spark.stop()

if __name__ == "__main__":
    run_gold_pipeline()