/**
 * Pub/Sub Module - Creates topics and subscriptions for crypto data ingestion
 */

# Main topic for crypto transactions
resource "google_pubsub_topic" "main" {
  name    = var.topic_name
  project = var.project_id

  message_retention_duration = var.message_retention_duration

  labels = {
    environment = var.environment
    component   = "ingestion"
  }
}

# Dead Letter Queue topic
resource "google_pubsub_topic" "dlq" {
  name    = var.dlq_topic_name
  project = var.project_id

  message_retention_duration = "604800s" # 7 days

  labels = {
    environment = var.environment
    component   = "dlq"
  }
}

# Main subscription with DLQ
resource "google_pubsub_subscription" "main" {
  name    = var.subscription_name
  topic   = google_pubsub_topic.main.name
  project = var.project_id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention_duration
  retain_acked_messages      = false

  expiration_policy {
    ttl = "" # Never expire
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  enable_exactly_once_delivery = true

  labels = {
    environment = var.environment
    component   = "streaming"
  }
}

# DLQ subscription for monitoring
resource "google_pubsub_subscription" "dlq" {
  name    = "${var.dlq_topic_name}-sub"
  topic   = google_pubsub_topic.dlq.name
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = {
    environment = var.environment
    component   = "dlq"
  }
}

# Schema for message validation (optional)
resource "google_pubsub_schema" "transaction" {
  name       = "crypto-transaction-schema"
  project    = var.project_id
  type       = "AVRO"
  definition = jsonencode({
    type = "record"
    name = "CryptoTransaction"
    fields = [
      { name = "event_type", type = "string" },
      { name = "event_time", type = "long" },
      { name = "symbol", type = "string" },
      { name = "trade_id", type = "long" },
      { name = "price", type = "double" },
      { name = "quantity", type = "double" },
      { name = "is_buyer_maker", type = "boolean" },
      { name = "source", type = "string" },
    ]
  })
}
