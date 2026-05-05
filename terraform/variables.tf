/**
 * Terraform Variables for Crypto Pipeline Infrastructure
 */

# Project Configuration
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "crypto-pipeline"
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for zonal resources"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# Pub/Sub Configuration
variable "pubsub_topic_name" {
  description = "Name of the main Pub/Sub topic"
  type        = string
  default     = "crypto-transactions"
}

variable "pubsub_subscription_name" {
  description = "Name of the Pub/Sub subscription"
  type        = string
  default     = "crypto-transactions-sub"
}

# BigQuery Configuration
variable "bigquery_dataset" {
  description = "BigQuery dataset name"
  type        = string
  default     = "crypto_pipeline"
}

variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

# Dataproc Configuration
variable "dataproc_cluster_name" {
  description = "Dataproc cluster name"
  type        = string
  default     = "crypto-spark-cluster"
}

variable "dataproc_num_workers" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 3
}

variable "dataproc_master_machine_type" {
  description = "Machine type for Dataproc master"
  type        = string
  default     = "n2-standard-4"
}

variable "dataproc_worker_machine_type" {
  description = "Machine type for Dataproc workers"
  type        = string
  default     = "n2-standard-4"
}

# Cloud Composer Configuration
variable "composer_environment_name" {
  description = "Cloud Composer environment name"
  type        = string
  default     = "crypto-pipeline-composer"
}

# Alerting Configuration
variable "alert_email" {
  description = "Email address for alerts"
  type        = string
  default     = "alerts@example.com"
}

# Labels
variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default     = {}
}
