/**
 * Dataproc Module Outputs
 */

output "cluster_name" {
  description = "Dataproc cluster name"
  value       = google_dataproc_cluster.cluster.name
}

output "cluster_id" {
  description = "Dataproc cluster ID"
  value       = google_dataproc_cluster.cluster.id
}

output "master_instance" {
  description = "Master instance name"
  value       = "${google_dataproc_cluster.cluster.name}-m"
}

output "cluster_config" {
  description = "Cluster configuration"
  value = {
    master_type      = var.master_config.machine_type
    worker_type      = var.worker_config.machine_type
    num_workers      = var.worker_config.num_instances
    image_version    = "2.1-debian11"
  }
}

output "web_interfaces" {
  description = "Web interface endpoints"
  value = {
    yarn_resource_manager = "https://${google_dataproc_cluster.cluster.name}-m:8088"
    spark_history_server  = "https://${google_dataproc_cluster.cluster.name}-m:18080"
  }
}

output "autoscaling_policy_id" {
  description = "Autoscaling policy ID"
  value       = google_dataproc_autoscaling_policy.policy.id
}
