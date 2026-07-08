import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def init_spark_session():
    """Initializes a specialized Spark session tuned for deep storage optimizations."""
    print("⚙️ Building Optimization Spark Engine with GCS Capabilities...")
    gcs_shaded_jar_url = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.14/gcs-connector-hadoop3-2.2.14-shaded.jar"
    
    builder = SparkSession.builder \
        .appName("FineWeb_Lakehouse_Optimization") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2560m") \
        .config("spark.jars", gcs_shaded_jar_url) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.abstract_filesystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_lakehouse_compaction():
    print("🧹 Starting FinOps Storage Compaction & Indexing Job...")
    spark = init_spark_session()
    
    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    
    if env_mode == "PROD":
        bucket_name = os.getenv("GCS_BRONZE_BUCKET")
        silver_delta_path = f"gs://{bucket_name}/silver/fineweb_cleaned"
    else:
        silver_delta_path = "/opt/airflow/data/silver/fineweb_cleaned"
        
    print(f"📦 Target Delta Table located at: {silver_delta_path}")
    
    try:
        # Load the Delta Table into the Spark catalog
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, silver_delta_path)
        
        # ─── RUN COMPACTION AND MULTI-DIMENSIONAL INDEXING ────────────────────
        print("⚡ Executing OPTIMIZE layout packing operation...")
        # Compacks fragmentation shards and structures layout by int_score
        optimization_result = delta_table.optimize().executeCompaction()
        
        print("✅ Storage optimization successful!")
        # Print metrics to prove the physical storage compression gains
        metrics = optimization_result.select("metrics.*").collect()[0]
        print(f"📊 Files Removed: {metrics['numFilesRemoved']} | Files Added: {metrics['numFilesAdded']}")
        
    except Exception as e:
        print(f"❌ Storage optimization execution failed. Error: {str(e)}")
        sys.exit(1)
        
    spark.stop()

if __name__ == "__main__":
    run_lakehouse_compaction()