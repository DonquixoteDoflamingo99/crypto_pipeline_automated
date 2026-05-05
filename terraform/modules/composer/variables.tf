/**
 * Cloud Composer Module Variables
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

variable "environment_name" {
  description = "Composer environment name"
  type        = string
}

variable "network_id" {
  description = "VPC network ID"
  type        = string
}

variable "subnetwork_id" {
  description = "Subnetwork ID"
  type        = string
}

variable "service_account" {
  description = "Service account email"
  type        = string
  default     = null
}

variable "airflow_config_overrides" {
  description = "Airflow configuration overrides"
  type        = map(string)
  default     = {}
}

variable "env_variables" {
  description = "Environment variables for Airflow"
  type        = map(string)
  default     = {}
}
