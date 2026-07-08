import os
import sys
from pathlib import Path
from huggingface_hub import hf_hub_download
from google.cloud import storage

# Configuration
REPO_ID = "HuggingFaceFW/fineweb-edu"
SUBFOLDER = "sample/10BT"
FILENAME = "000_00000.parquet"

def get_gcs_bucket_name():
    """Dynamically fetches bucket name from environment or falls back to standard name."""
    return os.getenv("GCS_BRONZE_BUCKET", "fineweb-bronze-storage-bucket")

def upload_to_gcs(local_path: str, bucket_name: str, destination_blob_name: str):
    """Uploads a local file to a Google Cloud Storage bucket using ADC authentication."""
    print(f"🚀 Initializing GCS Client for upload to gs://{bucket_name}/{destination_blob_name}...")
    try:
        project_id = os.getenv("GCP_PROJECT_ID", "catalyst-chain-project")
        storage_client = storage.Client(project=project_id)

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        print(f"📦 Uploading {local_path} to GCS (this may take a few minutes)...")
        blob.upload_from_filename(local_path)
        print("✅ GCS Upload Complete!")
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {str(e)}", file=sys.stderr)
        raise

def run_ingestion():
    # Detect active running mode (Defaults to DEV if not set)
    env_mode = os.getenv("ENV_MODE", "DEV").upper()
    print(f"🔥 Starting Ingestion Engine in [{env_mode}] Mode")

    # 1. Download file from Hugging Face Hub to a localized cache directory
    print(f"📥 Downloading {FILENAME} from {REPO_ID}...")
    try:
        downloaded_path = hf_hub_download(
            repo_id=REPO_ID,
            repo_type="dataset",
            subfolder=SUBFOLDER,
            filename=FILENAME,
            local_dir="/tmp/hf_cache" if env_mode == "PROD" else "./data/bronze",
            local_dir_use_symlinks=False
        )
        print(f"✅ Successfully downloaded to: {downloaded_path}")
    except Exception as e:
        print(f"❌ Hugging Face Download Failed: {str(e)}", file=sys.stderr)
        raise

    # 2. Handle environment-aware routing
    if env_mode == "PROD":
        bucket_name = get_gcs_bucket_name()
        destination_blob = f"bronze/fineweb/{FILENAME}"
        upload_to_gcs(downloaded_path, bucket_name, destination_blob)

        # Clean up temporary container disk space after cloud handoff
        print(f"🧹 Cleaning up temporary cache file: {downloaded_path}")
        os.remove(downloaded_path)
    else:
        print(f"🏠 DEV Mode: File preserved locally inside mounted './data/bronze' filesystem.")

if __name__ == "__main__":
    run_ingestion()