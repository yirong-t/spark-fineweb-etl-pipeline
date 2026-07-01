variable "project" {
    description = "Project"
    # IMPORTANT: Replace this with your actual GCP Project ID
    default = "catalyst-chain-project"
}

variable "region" {
    description = "Region"
    default = "us-central1"
}

variable "location" {
    description = "Project location"
    # Using a single region (us-central1) instead of 'US' guarantees you stay within the free tier.
    default = "us-central1"
}

variable "bq_dataset_name" {
    description = "My BigQuery dataset name"
    default = "fineweb_gold_layer"
}

variable "gcs_bucket_name" {
    description = "My Storage bucket name"
    # Bucket names must be globally unique across all of GCP.
    default = "fineweb-bronze-storage-bucket"
}

variable "gcs_storage_class" {
    description = "Bucket storage class"
    default = "STANDARD"
}