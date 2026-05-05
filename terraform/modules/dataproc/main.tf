/**
 * Dataproc Module - Creates Spark cluster for stream processing
 */

# Autoscaling policy for dynamic scaling
resource "google_dataproc_autoscaling_policy" "policy" {
  policy_id = "${var.cluster_name}-autoscaling"
  location  = var.region
  project   = var.project_id

  basic_algorithm {
    cooldown_period = "120s"

    yarn_config {
      graceful_decommission_timeout = "60s"
      scale_up_factor               = 1.0
      scale_down_factor             = 1.0
      scale_up_min_worker_fraction  = 0.5
      scale_down_min_worker_fraction = 0.0
    }
  }

  worker_config {
    min_instances = var.worker_config.num_instances
    max_instances = var.worker_config.num_instances * 3
    weight        = 1
  }

  secondary_worker_config {
    min_instances = 0
    max_instances = var.worker_config.num_instances * 2
    weight        = 1
  }
}

# Dataproc cluster
resource "google_dataproc_cluster" "cluster" {
  name    = var.cluster_name
  region  = var.region
  project = var.project_id

  labels = {
    environment = var.environment
    component   = "spark-streaming"
  }

  cluster_config {
    staging_bucket = var.staging_bucket

    master_config {
      num_instances = 1
      machine_type  = var.master_config.machine_type

      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.master_config.boot_disk_size
      }
    }

    worker_config {
      num_instances = var.worker_config.num_instances
      machine_type  = var.worker_config.machine_type

      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.worker_config.boot_disk_size
        num_local_ssds    = 1
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    software_config {
      image_version = "2.1-debian11"

      override_properties = var.spark_properties

      optional_components = [
        "JUPYTER",
        "DOCKER",
      ]
    }

    gce_cluster_config {
      network         = var.network_id
      subnetwork      = var.subnetwork_id
      internal_ip_only = false
      zone            = "${var.region}-a"

      tags = ["crypto-pipeline", "spark-cluster"]

      shielded_instance_config {
        enable_secure_boot          = true
        enable_vtpm                 = true
        enable_integrity_monitoring = true
      }

      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
      ]
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.policy.id
    }

    initialization_action {
      script      = "gs://goog-dataproc-initialization-actions-${var.region}/connectors/connectors.sh"
      timeout_sec = 300
    }

    endpoint_config {
      enable_http_port_access = true
    }
  }

  lifecycle {
    ignore_changes = [
      cluster_config[0].worker_config[0].num_instances,
      cluster_config[0].preemptible_worker_config[0].num_instances,
    ]
  }
}

# Workflow template for Spark jobs (only created if staging_bucket is provided)
resource "google_dataproc_workflow_template" "spark_streaming" {
  count    = var.staging_bucket != null ? 1 : 0
  name     = "${var.cluster_name}-streaming-workflow"
  location = var.region
  project  = var.project_id

  placement {
    managed_cluster {
      cluster_name = var.cluster_name

      config {
        master_config {
          num_instances = 1
          machine_type  = var.master_config.machine_type
        }

        worker_config {
          num_instances = var.worker_config.num_instances
          machine_type  = var.worker_config.machine_type
        }

        software_config {
          image_version = "2.1-debian11"
        }
      }
    }
  }

  jobs {
    step_id = "spark-streaming-job"

    pyspark_job {
      main_python_file_uri = "gs://${var.staging_bucket}/spark-jobs/spark_streaming.py"

      properties = {
        "spark.streaming.stopGracefullyOnShutdown" = "true"
        "spark.sql.streaming.metricsEnabled"       = "true"
      }
    }
  }
}
