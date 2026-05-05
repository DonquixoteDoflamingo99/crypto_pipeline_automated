/**
 * Pub/Sub Module Variables
 */

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "topic_name" {
  description = "Main Pub/Sub topic name"
  type        = string
}

variable "subscription_name" {
  description = "Main subscription name"
  type        = string
}

variable "dlq_topic_name" {
  description = "Dead letter queue topic name"
  type        = string
}

variable "message_retention_duration" {
  description = "Message retention duration"
  type        = string
  default     = "604800s" # 7 days
}

variable "ack_deadline_seconds" {
  description = "Acknowledgement deadline in seconds"
  type        = number
  default     = 60
}
