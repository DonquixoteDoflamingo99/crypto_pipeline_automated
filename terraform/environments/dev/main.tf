/**
 * Development Environment Configuration
 */

terraform {
  required_version = ">= 1.5.0"

  # Uncomment for remote state
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "crypto-pipeline/dev"
  # }
}

module "crypto_pipeline" {
  source = "../../"

  # Project Configuration
  project_id   = var.project_id
  project_name = "crypto-pipeline"
  region       = "us-central1"
  zone         = "us-central1-a"
  environment  = "dev"

  # Pub/Sub
  pubsub_topic_name        = "crypto-transactions-dev"
  pubsub_subscription_name = "crypto-transactions-dev-sub"

  # BigQuery
  bigquery_dataset  = "crypto_pipeline_dev"
  bigquery_location = "US"

  # Dataproc - smaller for dev
  dataproc_cluster_name        = "crypto-spark-dev"
  dataproc_num_workers         = 2
  dataproc_master_machine_type = "n2-standard-2"
  dataproc_worker_machine_type = "n2-standard-2"

  # Composer - smaller for dev
  composer_environment_name = "crypto-composer-dev"

  # Alerting
  alert_email = var.alert_email

  # Labels
  labels = {
    environment = "dev"
    team        = "data-engineering"
    cost-center = "development"
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "alert_email" {
  description = "Alert notification email"
  type        = string
  default     = "dev-alerts@example.com"
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
  }
}
