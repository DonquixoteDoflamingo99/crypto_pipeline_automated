/**
 * Dataproc Module Variables
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

variable "cluster_name" {
  description = "Dataproc cluster name"
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

variable "staging_bucket" {
  description = "GCS bucket for staging"
  type        = string
  default     = null
}

variable "master_config" {
  description = "Master node configuration"
  type = object({
    machine_type   = string
    boot_disk_size = number
  })
  default = {
    machine_type   = "n2-standard-4"
    boot_disk_size = 500
  }
}

variable "worker_config" {
  description = "Worker node configuration"
  type = object({
    num_instances  = number
    machine_type   = string
    boot_disk_size = number
  })
  default = {
    num_instances  = 3
    machine_type   = "n2-standard-4"
    boot_disk_size = 500
  }
}

variable "spark_properties" {
  description = "Spark configuration properties"
  type        = map(string)
  default     = {}
}
