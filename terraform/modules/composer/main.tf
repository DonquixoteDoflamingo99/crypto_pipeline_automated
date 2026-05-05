/**
 * Cloud Composer Module - Creates managed Airflow environment
 */

# Cloud Composer Environment
resource "google_composer_environment" "main" {
  name    = var.environment_name
  region  = var.region
  project = var.project_id

  labels = {
    environment = var.environment
    component   = "orchestration"
  }

  config {
    software_config {
      image_version = "composer-2.5.0-airflow-2.6.3"

      airflow_config_overrides = var.airflow_config_overrides

      pypi_packages = {
        "google-cloud-bigquery"   = ">=3.13.0"
        "google-cloud-pubsub"     = ">=2.18.0"
        "google-cloud-dataproc"   = ">=5.7.0"
        "google-cloud-storage"    = ">=2.13.0"
        "google-cloud-monitoring" = ">=2.16.0"
        "structlog"               = ">=23.2.0"
      }

      env_variables = var.env_variables
    }

    workloads_config {
      scheduler {
        cpu        = 2
        memory_gb  = 4
        storage_gb = 5
        count      = 1
      }

      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 5
      }

      worker {
        cpu        = 2
        memory_gb  = 4
        storage_gb = 10
        min_count  = 1
        max_count  = 6
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      network         = var.network_id
      subnetwork      = var.subnetwork_id
      service_account = var.service_account
    }

    private_environment_config {
      enable_private_endpoint = false
    }

    maintenance_window {
      start_time = "2024-01-01T00:00:00Z"
      end_time   = "2024-01-01T04:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SU"
    }
  }

  timeouts {
    create = "60m"
    update = "60m"
    delete = "30m"
  }
}

# IAM binding for DAG bucket access
resource "google_storage_bucket_iam_member" "dag_bucket_access" {
  bucket = replace(
    google_composer_environment.main.config[0].dag_gcs_prefix,
    "gs://", ""
  )
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.service_account}"

  depends_on = [google_composer_environment.main]
}
