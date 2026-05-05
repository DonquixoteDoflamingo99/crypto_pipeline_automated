/**
 * Crypto Pipeline Infrastructure - Main Configuration
 *
 * This is the root Terraform configuration that orchestrates
 * all infrastructure modules for the crypto data pipeline.
 */

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }

  # Backend configuration for state management
  # Uncomment and configure for production use
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "crypto-pipeline/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "dataproc.googleapis.com",
    "composer.googleapis.com",
    "storage.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# Networking Module
module "networking" {
  source = "./modules/networking"

  project_id   = var.project_id
  region       = var.region
  environment  = var.environment
  network_name = "${var.project_name}-network"

  depends_on = [google_project_service.apis]
}

# Pub/Sub Module
module "pubsub" {
  source = "./modules/pubsub"

  project_id        = var.project_id
  environment       = var.environment
  topic_name        = var.pubsub_topic_name
  subscription_name = var.pubsub_subscription_name
  dlq_topic_name    = "${var.pubsub_topic_name}-dlq"

  message_retention_duration = "604800s" # 7 days
  ack_deadline_seconds       = 60

  depends_on = [google_project_service.apis]
}

# BigQuery Module
module "bigquery" {
  source = "./modules/bigquery"

  project_id   = var.project_id
  region       = var.region
  environment  = var.environment
  dataset_id   = var.bigquery_dataset
  location     = var.bigquery_location

  tables = {
    transactions = {
      partition_field  = "trade_date"
      clustering_fields = ["symbol", "trade_hour"]
    }
    hourly_aggregates = {
      partition_field  = "hour_timestamp"
      clustering_fields = ["symbol"]
    }
    daily_aggregates = {
      partition_field  = "trade_date"
      clustering_fields = ["symbol"]
    }
  }

  depends_on = [google_project_service.apis]
}

# Dataproc Module
module "dataproc" {
  source = "./modules/dataproc"

  project_id      = var.project_id
  region          = var.region
  environment     = var.environment
  cluster_name    = var.dataproc_cluster_name
  network_id      = module.networking.network_id
  subnetwork_id   = module.networking.subnetwork_id
  staging_bucket  = google_storage_bucket.data_bucket.name

  master_config = {
    machine_type   = var.dataproc_master_machine_type
    boot_disk_size = 500
  }

  worker_config = {
    num_instances  = var.dataproc_num_workers
    machine_type   = var.dataproc_worker_machine_type
    boot_disk_size = 500
  }

  spark_properties = {
    "spark:spark.jars.packages" = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0"
    "spark:spark.streaming.backpressure.enabled" = "true"
  }

  depends_on = [
    google_project_service.apis,
    module.networking,
    google_storage_bucket.data_bucket
  ]
}

# Cloud Composer Module
module "composer" {
  source = "./modules/composer"

  project_id       = var.project_id
  region           = var.region
  environment      = var.environment
  environment_name = var.composer_environment_name
  network_id       = module.networking.network_id
  subnetwork_id    = module.networking.subnetwork_id

  airflow_config_overrides = {
    "core-dags_are_paused_at_creation" = "True"
    "webserver-expose_config"          = "False"
  }

  env_variables = {
    GCP_PROJECT_ID     = var.project_id
    BIGQUERY_DATASET   = var.bigquery_dataset
    PUBSUB_TOPIC       = var.pubsub_topic_name
    DATAPROC_CLUSTER   = var.dataproc_cluster_name
  }

  depends_on = [
    google_project_service.apis,
    module.networking
  ]
}

# Cloud Storage Buckets
resource "google_storage_bucket" "data_bucket" {
  name          = "${var.project_id}-${var.environment}-data"
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }

  labels = {
    environment = var.environment
    project     = var.project_name
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "checkpoint_bucket" {
  name          = "${var.project_id}-${var.environment}-checkpoints"
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  labels = {
    environment = var.environment
    project     = var.project_name
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "temp_bucket" {
  name          = "${var.project_id}-${var.environment}-temp"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }

  labels = {
    environment = var.environment
    project     = var.project_name
  }

  depends_on = [google_project_service.apis]
}

# Service Account for Pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "crypto-pipeline-sa"
  display_name = "Crypto Pipeline Service Account"
  project      = var.project_id
}

# IAM bindings for service account
resource "google_project_iam_member" "pipeline_sa_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/storage.objectAdmin",
    "roles/dataproc.worker",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
