import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length
from delta import configure_spark_with_delta_pip

def init_spark_session():
    """Initializes a local Spark Session to read Delta logs for statistical analysis."""
    gcs_shaded_jar_url = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.14/gcs-connector-hadoop3-2.2.14-shaded.jar"
    builder = SparkSession.builder \
        .appName("FineWeb_Data_Drift_Diff") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", gcs_shaded_jar_url) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.abstract_filesystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def analyze_data_drift():
    print("📊 Initiating Data Observability & Statistical Diffing Suite...")
    spark = init_spark_session()

    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    if env_mode == "PROD":
        bucket_name = os.getenv("GCS_BRONZE_BUCKET")
        silver_path = f"gs://{bucket_name}/silver/fineweb_cleaned"
    else:
        silver_path = "/opt/airflow/data/silver/fineweb_cleaned"

    print(f"🧐 Auditing current dataset distributions at: {silver_path}")

    try:
        df_silver = spark.read.format("delta").load(silver_path)

        # ─── COMPUTE BATCH STATISTICAL PROFILE ────────────────────────────────
        stats = df_silver.select(
            avg("int_score").alias("avg_quality"),
            avg(length("cleaned_text")).alias("avg_char_length")
        ).collect()[0]

        current_avg_quality = stats["avg_quality"] if stats["avg_quality"] else 0.0
        current_avg_length = stats["avg_char_length"] if stats["avg_char_length"] else 0.0

        # ─── ENFORCE HISTORICAL EXPECTATION GATES ─────────────────────────────
        # Define historical bounds for FineWeb-Edu quality
        EXPECTED_MIN_QUALITY = 2.5
        EXPECTED_MIN_CHAR_LENGTH = 500.0

        print("\n🔍 ─── DATA DRIFT DIFF REPORT ───")
        print(f"📈 Current Batch Avg Educational Score: {current_avg_quality:.2f} (Expected >= {EXPECTED_MIN_QUALITY})")
        print(f"📈 Current Batch Avg Character Length: {current_avg_length:.2f} (Expected >= {EXPECTED_MIN_CHAR_LENGTH})")
        print("─────────────────────────────────\n")

        if current_avg_quality < EXPECTED_MIN_QUALITY:
            raise ValueError(f"🚨 CRITICAL DATA DRIFT: Batch quality dropped to {current_avg_quality:.2f}! Potential upstream scraping pollution.")

        if current_avg_length < EXPECTED_MIN_CHAR_LENGTH:
            raise ValueError(f"🚨 CRITICAL DATA DRIFT: Batch text length truncated to {current_avg_length:.2f} characters! Data integrity anomaly detected.")

        print("✅ Data Diffing Complete: Statistical variances fall within acceptable standard distributions. Production deployment is safe.")

    except Exception as e:
        print(f"❌ Observability Guardrail Triggered: {str(e)}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    analyze_data_drift()