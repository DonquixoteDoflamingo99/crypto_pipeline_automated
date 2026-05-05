/**
 * Cloud Composer Module Outputs
 */

output "environment_id" {
  description = "Composer environment ID"
  value       = google_composer_environment.main.id
}

output "environment_name" {
  description = "Composer environment name"
  value       = google_composer_environment.main.name
}

output "dag_gcs_bucket" {
  description = "GCS bucket for DAGs"
  value       = google_composer_environment.main.config[0].dag_gcs_prefix
}

output "airflow_uri" {
  description = "Airflow web UI URI"
  value       = google_composer_environment.main.config[0].airflow_uri
}

output "gke_cluster" {
  description = "GKE cluster running Composer"
  value       = google_composer_environment.main.config[0].gke_cluster
}
