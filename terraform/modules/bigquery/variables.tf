/**
 * BigQuery Module Variables
 */

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "tables" {
  description = "Table configurations"
  type = map(object({
    partition_field   = string
    clustering_fields = list(string)
  }))
  default = {}
}
