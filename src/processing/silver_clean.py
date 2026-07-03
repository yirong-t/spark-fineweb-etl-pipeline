import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, length
from delta import configure_spark_with_delta_pip

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

def process_bronze_to_silver():
    spark = init_spark_session()

    # Define environment-aware file system paths
    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    print(f"🎬 Processing data in [{env_mode}] mode")

    if env_mode == "PROD":
        # In PROD, Spark would stream directly from the GCS bucket path
        bucket_name = os.getenv("GCS_BRONZE_BUCKET")
        input_path = f"gs://{bucket_name}/bronze/fineweb/*.parquet"
        output_path = f"gs://{bucket_name}/silver/fineweb_cleaned"
    else:
        # In DEV, Spark reads from our local volume mount
        input_path = "/opt/airflow/data/bronze/sample/10BT/*.parquet"
        output_path = "/opt/airflow/data/silver/fineweb_cleaned"

    # 1. Read Raw Bronze Data
    print(f"📖 Reading raw Parquet data from: {input_path}")
    try:
        df_raw = spark.read.parquet(input_path)
    except Exception as e:
        print(f"❌ Failed to read Bronze Data. Ensure file exists. Error: {str(e)}")
        sys.exit(1)

    # Print schema for tracking
    print("📊 Source Schema Detected:")
    df_raw.printSchema()

    # 2. Execute LLM Text Cleaning Operations
    print("🧼 Executing text normalization and quality filtering...")
    df_cleaned = df_raw \
        .filter(col("text").isNotNull()) \
        .withColumn("cleaned_text", trim(col("text"))) \
        .withColumn("cleaned_text", regexp_replace(col("cleaned_text"), r"\s+", " ")) \
        .filter(length(col("cleaned_text")) > 200) # Filter out short noise/error pages

    # 3. Write Atomic Transformed Records to Silver Delta Table
    print(f"💾 Writing cleaned data to Silver Delta Lake at: {output_path}")
    df_cleaned.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    print(f"🎉 Silver Layer successfully built! Total row count: {df_cleaned.count()}")
    # input("⏸️ Spark job is complete! Spark UI is available at localhost:4040. Press Enter to exit...")
    spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()