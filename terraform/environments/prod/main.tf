/**
 * Production Environment Configuration
 */

terraform {
  required_version = ">= 1.5.0"

  # Remote state is required for production
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "crypto-pipeline/prod"
  # }
}

module "crypto_pipeline" {
  source = "../../"

  # Project Configuration
  project_id   = var.project_id
  project_name = "crypto-pipeline"
  region       = "us-central1"
  zone         = "us-central1-a"
  environment  = "prod"

  # Pub/Sub
  pubsub_topic_name        = "crypto-transactions"
  pubsub_subscription_name = "crypto-transactions-sub"

  # BigQuery
  bigquery_dataset  = "crypto_pipeline"
  bigquery_location = "US"

  # Dataproc - production scale for 50M+ daily transactions
  dataproc_cluster_name        = "crypto-spark-prod"
  dataproc_num_workers         = 5
  dataproc_master_machine_type = "n2-standard-8"
  dataproc_worker_machine_type = "n2-standard-8"

  # Composer - production scale
  composer_environment_name = "crypto-composer-prod"

  # Alerting
  alert_email = var.alert_email

  # Labels
  labels = {
    environment = "prod"
    team        = "data-engineering"
    cost-center = "production"
    compliance  = "pci-dss"
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "alert_email" {
  description = "Alert notification email"
  type        = string
  default     = "prod-alerts@example.com"
}

output "pipeline_info" {
  description = "Pipeline deployment information"
  value = {
    project_id           = module.crypto_pipeline.project_id
    environment          = module.crypto_pipeline.environment
    pubsub_topic         = module.crypto_pipeline.pubsub_topic_path
    bigquery_dataset     = module.crypto_pipeline.bigquery_dataset_path
    dataproc_cluster     = module.crypto_pipeline.dataproc_cluster_name
    composer_airflow_uri = module.crypto_pipeline.composer_airflow_uri
    service_account      = module.crypto_pipeline.pipeline_service_account
  }
}
