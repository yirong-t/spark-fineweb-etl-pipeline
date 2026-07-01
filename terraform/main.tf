terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.36.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "fineweb_bronze_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
  storage_class = var.gcs_storage_class

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "fineweb_gold_dataset" {
  dataset_id  = var.bq_dataset_name
  location    = var.location
  delete_contents_on_destroy = true
}