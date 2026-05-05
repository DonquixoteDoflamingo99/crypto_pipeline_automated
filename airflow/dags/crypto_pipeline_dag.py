"""
Main Crypto Pipeline DAG for orchestrating the streaming pipeline.

This DAG manages:
1. Dataproc cluster lifecycle
2. Spark Streaming job submission
3. Health checks and alerting
4. Cluster scaling based on load
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# Configuration from Airflow Variables
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
REGION = Variable.get("gcp_region", default_var="us-central1")
CLUSTER_NAME = Variable.get("dataproc_cluster_name", default_var="crypto-spark-cluster")
BUCKET = Variable.get("gcs_bucket", default_var="crypto-pipeline-data")
PUBSUB_SUBSCRIPTION = Variable.get("pubsub_subscription", default_var="crypto-transactions-sub")


# Cluster configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 500,
        },
    },
    "worker_config": {
        "num_instances": 3,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 500,
        },
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.jars.packages": (
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0"
            ),
            "spark:spark.streaming.backpressure.enabled": "true",
        },
    },
    "autoscaling_config": {
        "policy_uri": f"projects/{PROJECT_ID}/regions/{REGION}/autoscalingPolicies/crypto-autoscaling",
    },
    "gce_cluster_config": {
        "internal_ip_only": False,
        "tags": ["crypto-pipeline"],
    },
}


# Spark job configuration
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/spark-jobs/spark_streaming.py",
        "python_file_uris": [
            f"gs://{BUCKET}/spark-jobs/src.zip",
        ],
        "args": [
            f"--project-id={PROJECT_ID}",
            f"--subscription={PUBSUB_SUBSCRIPTION}",
        ],
        "properties": {
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.checkpointLocation": f"gs://{BUCKET}/checkpoints/",
        },
    },
}


# Default DAG arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [Variable.get("alert_email", default_var="alerts@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def check_cluster_exists(**context) -> str:
    """Check if Dataproc cluster already exists."""
    from google.cloud import dataproc_v1

    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    try:
        client.get_cluster(
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
        )
        return "cluster_exists"
    except Exception:
        return "create_cluster"


def check_streaming_job_running(**context) -> str:
    """Check if streaming job is already running and branch accordingly."""
    from google.cloud import dataproc_v1

    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    # List recent jobs
    try:
        jobs = client.list_jobs(
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            job_state_matcher=dataproc_v1.ListJobsRequest.JobStateMatcher.ACTIVE,
        )

        for job in jobs:
            if "spark_streaming" in job.reference.job_id:
                context["task_instance"].xcom_push(key="active_job_id", value=job.reference.job_id)
                return "job_already_running"
    except Exception:
        pass  # Cluster may not exist yet, proceed to submit job

    return "submit_spark_streaming_job"


def log_pipeline_metrics(**context) -> None:
    """Log pipeline metrics to Cloud Monitoring."""
    from google.cloud import monitoring_v3
    import time

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"

    # Create custom metric for DAG run
    series = monitoring_v3.TimeSeries()
    series.metric.type = "custom.googleapis.com/crypto_pipeline/dag_run"
    series.resource.type = "global"
    series.resource.labels["project_id"] = PROJECT_ID

    now = time.time()
    interval = monitoring_v3.TimeInterval()
    interval.end_time.seconds = int(now)

    point = monitoring_v3.Point()
    point.interval = interval
    point.value.int64_value = 1
    series.points.append(point)

    client.create_time_series(name=project_name, time_series=[series])


# DAG Definition
with DAG(
    dag_id="crypto_streaming_pipeline",
    default_args=default_args,
    description="Orchestrates the crypto streaming pipeline on Dataproc",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "streaming", "dataproc"],
    max_active_runs=1,
) as dag:

    # Check if cluster exists
    check_cluster = BranchPythonOperator(
        task_id="check_cluster_exists",
        python_callable=check_cluster_exists,
    )

    # Create cluster if needed
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        use_if_exists=True,
    )

    # Dummy task for existing cluster
    cluster_exists = PythonOperator(
        task_id="cluster_exists",
        python_callable=lambda: print("Cluster already exists"),
    )

    # Check if streaming job is running (branches to skip or submit)
    check_job = BranchPythonOperator(
        task_id="check_streaming_job",
        python_callable=check_streaming_job_running,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Skip task when job is already running
    job_already_running = PythonOperator(
        task_id="job_already_running",
        python_callable=lambda: print("Streaming job already running, skipping submission"),
    )

    # Submit streaming job
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_streaming_job",
        job=SPARK_JOB,
        project_id=PROJECT_ID,
        region=REGION,
        asynchronous=True,
    )

    # Log metrics
    log_metrics = PythonOperator(
        task_id="log_pipeline_metrics",
        python_callable=log_pipeline_metrics,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Define task dependencies
    check_cluster >> [create_cluster, cluster_exists]
    [create_cluster, cluster_exists] >> check_job >> [submit_spark_job, job_already_running]
    [submit_spark_job, job_already_running] >> log_metrics
