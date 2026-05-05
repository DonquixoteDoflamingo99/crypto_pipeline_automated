/**
 * BigQuery Module - Creates datasets and tables for crypto data warehouse
 */

# BigQuery Dataset
resource "google_bigquery_dataset" "main" {
  dataset_id  = var.dataset_id
  project     = var.project_id
  location    = var.location
  description = "Crypto Pipeline Data Warehouse"

  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"

  labels = {
    environment = var.environment
    component   = "data-warehouse"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

# Transactions Table
resource "google_bigquery_table" "transactions" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "transactions"
  project             = var.project_id
  deletion_protection = var.environment == "prod"

  description = "Raw cryptocurrency transaction data"

  time_partitioning {
    type  = "DAY"
    field = "trade_date"
  }

  clustering = ["symbol", "trade_hour"]

  schema = jsonencode([
    { name = "event_type", type = "STRING", mode = "NULLABLE" },
    { name = "event_time", type = "INTEGER", mode = "NULLABLE" },
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "trade_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "quantity", type = "FLOAT64", mode = "REQUIRED" },
    { name = "buyer_order_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "seller_order_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "trade_time", type = "INTEGER", mode = "NULLABLE" },
    { name = "is_buyer_maker", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "ingestion_time", type = "STRING", mode = "NULLABLE" },
    { name = "source", type = "STRING", mode = "NULLABLE" },
    { name = "pubsub_publish_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "trade_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "trade_value_usd", type = "FLOAT64", mode = "NULLABLE" },
    { name = "trade_date", type = "DATE", mode = "NULLABLE" },
    { name = "trade_hour", type = "INTEGER", mode = "NULLABLE" },
    { name = "processing_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "pipeline_version", type = "STRING", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
    data_type   = "transactions"
  }
}

# Hourly Aggregates Table
resource "google_bigquery_table" "hourly_aggregates" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "hourly_aggregates"
  project             = var.project_id
  deletion_protection = var.environment == "prod"

  description = "Hourly OHLCV aggregates"

  time_partitioning {
    type  = "DAY"
    field = "hour_timestamp"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "hour_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "open_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "high_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "low_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "close_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "total_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "total_trades", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_value_usd", type = "FLOAT64", mode = "NULLABLE" },
    { name = "avg_trade_size", type = "FLOAT64", mode = "NULLABLE" },
    { name = "buy_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sell_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "vwap", type = "FLOAT64", mode = "NULLABLE" },
    { name = "aggregation_time", type = "TIMESTAMP", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
    data_type   = "aggregates"
  }
}

# Daily Aggregates Table
resource "google_bigquery_table" "daily_aggregates" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "daily_aggregates"
  project             = var.project_id
  deletion_protection = var.environment == "prod"

  description = "Daily OHLCV aggregates with statistics"

  time_partitioning {
    type  = "DAY"
    field = "trade_date"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "trade_date", type = "DATE", mode = "REQUIRED" },
    { name = "open_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "high_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "low_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "close_price", type = "FLOAT64", mode = "NULLABLE" },
    { name = "total_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "total_trades", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_value_usd", type = "FLOAT64", mode = "NULLABLE" },
    { name = "avg_hourly_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "max_hourly_volume", type = "FLOAT64", mode = "NULLABLE" },
    { name = "price_change", type = "FLOAT64", mode = "NULLABLE" },
    { name = "price_change_pct", type = "FLOAT64", mode = "NULLABLE" },
    { name = "aggregation_time", type = "TIMESTAMP", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
    data_type   = "aggregates"
  }
}

# Schema History Table
resource "google_bigquery_table" "schema_history" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "schema_history"
  project             = var.project_id
  deletion_protection = false

  description = "Schema evolution tracking"

  schema = jsonencode([
    { name = "table_id", type = "STRING", mode = "REQUIRED" },
    { name = "version_hash", type = "STRING", mode = "REQUIRED" },
    { name = "fields_added", type = "STRING", mode = "NULLABLE" },
    { name = "change_time", type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = {
    environment = var.environment
    data_type   = "metadata"
  }
}

# Schema Registry Table
resource "google_bigquery_table" "schema_registry" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "schema_registry"
  project             = var.project_id
  deletion_protection = false

  description = "Schema version registry"

  schema = jsonencode([
    { name = "subject", type = "STRING", mode = "REQUIRED" },
    { name = "version_id", type = "STRING", mode = "REQUIRED" },
    { name = "schema_hash", type = "STRING", mode = "REQUIRED" },
    { name = "schema_definition", type = "STRING", mode = "REQUIRED" },
    { name = "compatibility_mode", type = "STRING", mode = "REQUIRED" },
    { name = "is_active", type = "BOOLEAN", mode = "REQUIRED" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "created_by", type = "STRING", mode = "NULLABLE" },
    { name = "description", type = "STRING", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
    data_type   = "metadata"
  }
}

# DLQ Table for failed records
resource "google_bigquery_table" "transactions_dlq" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "transactions_dlq"
  project             = var.project_id
  deletion_protection = false

  description = "Dead letter queue for failed transaction records"

  time_partitioning {
    type  = "DAY"
    field = "processing_time"
  }

  schema = jsonencode([
    { name = "raw_data", type = "STRING", mode = "NULLABLE" },
    { name = "validation_error", type = "STRING", mode = "NULLABLE" },
    { name = "processing_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "source", type = "STRING", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
    data_type   = "dlq"
  }
}
