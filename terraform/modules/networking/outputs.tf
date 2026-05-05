/**
 * Networking Module Outputs
 */

output "network_id" {
  description = "VPC network ID"
  value       = google_compute_network.main.id
}

output "network_name" {
  description = "VPC network name"
  value       = google_compute_network.main.name
}

output "network_self_link" {
  description = "VPC network self link"
  value       = google_compute_network.main.self_link
}

output "subnetwork_id" {
  description = "Subnetwork ID"
  value       = google_compute_subnetwork.main.id
}

output "subnetwork_name" {
  description = "Subnetwork name"
  value       = google_compute_subnetwork.main.name
}

output "subnetwork_self_link" {
  description = "Subnetwork self link"
  value       = google_compute_subnetwork.main.self_link
}

output "router_id" {
  description = "Cloud Router ID"
  value       = google_compute_router.main.id
}

output "nat_id" {
  description = "Cloud NAT ID"
  value       = google_compute_router_nat.main.id
}
