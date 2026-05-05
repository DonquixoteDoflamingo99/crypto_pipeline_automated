/**
 * BigQuery Module Outputs
 */

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.main.dataset_id
}

output "dataset_self_link" {
  description = "BigQuery dataset self link"
  value       = google_bigquery_dataset.main.self_link
}

output "table_ids" {
  description = "Map of table IDs"
  value = {
    transactions       = google_bigquery_table.transactions.table_id
    hourly_aggregates  = google_bigquery_table.hourly_aggregates.table_id
    daily_aggregates   = google_bigquery_table.daily_aggregates.table_id
    schema_history     = google_bigquery_table.schema_history.table_id
    schema_registry    = google_bigquery_table.schema_registry.table_id
    transactions_dlq   = google_bigquery_table.transactions_dlq.table_id
  }
}

output "full_table_paths" {
  description = "Full table paths"
  value = {
    transactions      = "${var.project_id}.${google_bigquery_dataset.main.dataset_id}.${google_bigquery_table.transactions.table_id}"
    hourly_aggregates = "${var.project_id}.${google_bigquery_dataset.main.dataset_id}.${google_bigquery_table.hourly_aggregates.table_id}"
    daily_aggregates  = "${var.project_id}.${google_bigquery_dataset.main.dataset_id}.${google_bigquery_table.daily_aggregates.table_id}"
  }
}
