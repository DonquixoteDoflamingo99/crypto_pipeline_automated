/**
 * Terraform Outputs for Crypto Pipeline Infrastructure
 */

# Project Information
output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP Region"
  value       = var.region
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

# Networking
output "network_id" {
  description = "VPC Network ID"
  value       = module.networking.network_id
}

output "subnetwork_id" {
  description = "Subnetwork ID"
  value       = module.networking.subnetwork_id
}

# Pub/Sub
output "pubsub_topic_id" {
  description = "Pub/Sub topic ID"
  value       = module.pubsub.topic_id
}

output "pubsub_subscription_id" {
  description = "Pub/Sub subscription ID"
  value       = module.pubsub.subscription_id
}

output "pubsub_dlq_topic_id" {
  description = "Pub/Sub DLQ topic ID"
  value       = module.pubsub.dlq_topic_id
}

# BigQuery
output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = module.bigquery.dataset_id
}

output "bigquery_table_ids" {
  description = "BigQuery table IDs"
  value       = module.bigquery.table_ids
}

# Dataproc
output "dataproc_cluster_name" {
  description = "Dataproc cluster name"
  value       = module.dataproc.cluster_name
}

output "dataproc_master_instance" {
  description = "Dataproc master instance name"
  value       = module.dataproc.master_instance
}

# Cloud Composer
output "composer_environment_id" {
  description = "Cloud Composer environment ID"
  value       = module.composer.environment_id
}

output "composer_dag_gcs_bucket" {
  description = "Cloud Composer DAG GCS bucket"
  value       = module.composer.dag_gcs_bucket
}

output "composer_airflow_uri" {
  description = "Cloud Composer Airflow web UI URI"
  value       = module.composer.airflow_uri
}

# Storage Buckets
output "data_bucket_name" {
  description = "Data storage bucket name"
  value       = google_storage_bucket.data_bucket.name
}

output "checkpoint_bucket_name" {
  description = "Checkpoint storage bucket name"
  value       = google_storage_bucket.checkpoint_bucket.name
}

output "temp_bucket_name" {
  description = "Temporary storage bucket name"
  value       = google_storage_bucket.temp_bucket.name
}

# Service Account
output "pipeline_service_account" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

# Connection Strings / URIs
output "pubsub_topic_path" {
  description = "Full Pub/Sub topic path"
  value       = "projects/${var.project_id}/topics/${var.pubsub_topic_name}"
}

output "bigquery_dataset_path" {
  description = "Full BigQuery dataset path"
  value       = "${var.project_id}.${var.bigquery_dataset}"
}
