"""
Maintenance DAG for pipeline housekeeping tasks.

This DAG runs daily to:
1. Clean up old checkpoints and temp files
2. Manage BigQuery partitions
3. Monitor resource usage
4. Generate operational reports
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)


# Configuration
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
DATASET = Variable.get("bigquery_dataset", default_var="crypto_pipeline")
CHECKPOINT_BUCKET = Variable.get("checkpoint_bucket", default_var="crypto-pipeline-checkpoints")
TEMP_BUCKET = Variable.get("temp_bucket", default_var="crypto-pipeline-temp")
RETENTION_DAYS = int(Variable.get("data_retention_days", default_var="90"))


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [Variable.get("alert_email", default_var="alerts@example.com")],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def cleanup_gcs_temp_files(**context) -> None:
    """Delete old temporary files from GCS."""
    from google.cloud import storage
    from datetime import timezone

    client = storage.Client(project=PROJECT_ID)

    buckets_to_clean = [TEMP_BUCKET]
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
    deleted_count = 0

    for bucket_name in buckets_to_clean:
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs()

        for blob in blobs:
            if blob.time_created < cutoff_date:
                blob.delete()
                deleted_count += 1

    print(f"Deleted {deleted_count} old temp files")
    context["task_instance"].xcom_push(key="deleted_temp_files", value=deleted_count)


def cleanup_old_checkpoints(**context) -> None:
    """Clean up old Spark checkpoint directories."""
    from google.cloud import storage
    from datetime import timezone

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(CHECKPOINT_BUCKET)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
    deleted_count = 0

    # List checkpoint directories
    blobs = bucket.list_blobs(prefix="spark-checkpoints/")

    for blob in blobs:
        if blob.time_created < cutoff_date:
            blob.delete()
            deleted_count += 1

    print(f"Deleted {deleted_count} old checkpoint files")
    context["task_instance"].xcom_push(key="deleted_checkpoints", value=deleted_count)


def check_bigquery_storage(**context) -> None:
    """Monitor BigQuery storage usage."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
        table_name,
        ROUND(total_rows / 1e6, 2) as rows_millions,
        ROUND(total_logical_bytes / POW(1024, 3), 2) as size_gb,
        ROUND(total_billable_bytes / POW(1024, 3), 2) as billable_gb
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLE_STORAGE`
    ORDER BY total_logical_bytes DESC
    """

    results = client.query(query).result()

    storage_info = []
    for row in results:
        storage_info.append({
            "table": row.table_name,
            "rows_millions": row.rows_millions,
            "size_gb": row.size_gb,
            "billable_gb": row.billable_gb,
        })
        print(f"Table {row.table_name}: {row.rows_millions}M rows, {row.size_gb}GB")

    context["task_instance"].xcom_push(key="storage_info", value=storage_info)


def generate_daily_report(**context) -> None:
    """Generate daily operational report."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    # Get yesterday's stats
    query = f"""
    SELECT
        COUNT(*) as total_transactions,
        COUNT(DISTINCT symbol) as unique_symbols,
        SUM(trade_value_usd) as total_value_usd,
        AVG(price) as avg_price,
        MIN(trade_timestamp) as first_trade,
        MAX(trade_timestamp) as last_trade
    FROM `{PROJECT_ID}.{DATASET}.transactions`
    WHERE trade_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """

    results = client.query(query).result()

    for row in results:
        report = {
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "total_transactions": row.total_transactions,
            "unique_symbols": row.unique_symbols,
            "total_value_usd": float(row.total_value_usd or 0),
        }
        print(f"Daily Report: {report}")
        context["task_instance"].xcom_push(key="daily_report", value=report)


# Partition cleanup SQL
PARTITION_CLEANUP_SQL = """
-- Delete partitions older than retention period
DECLARE cutoff_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL {retention_days} DAY);

DELETE FROM `{project}.{dataset}.transactions`
WHERE trade_date < cutoff_date;

DELETE FROM `{project}.{dataset}.hourly_aggregates`
WHERE DATE(hour_timestamp) < cutoff_date;
"""


# DAG Definition
with DAG(
    dag_id="crypto_pipeline_maintenance",
    default_args=default_args,
    description="Daily maintenance tasks for the crypto pipeline",
    schedule_interval="0 4 * * *",  # Run at 4 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "maintenance", "cleanup"],
    max_active_runs=1,
) as dag:

    # Clean up GCS temp files
    cleanup_temp = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_gcs_temp_files,
    )

    # Clean up old checkpoints
    cleanup_checkpoints = PythonOperator(
        task_id="cleanup_checkpoints",
        python_callable=cleanup_old_checkpoints,
    )

    # Clean up old BigQuery partitions
    cleanup_partitions = BigQueryInsertJobOperator(
        task_id="cleanup_old_partitions",
        configuration={
            "query": {
                "query": PARTITION_CLEANUP_SQL.format(
                    project=PROJECT_ID,
                    dataset=DATASET,
                    retention_days=RETENTION_DAYS,
                ),
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # Check BigQuery storage
    check_storage = PythonOperator(
        task_id="check_bigquery_storage",
        python_callable=check_bigquery_storage,
    )

    # Generate daily report
    daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
    )

    # Task dependencies - cleanup tasks run in parallel, then storage check and report
    [cleanup_temp, cleanup_checkpoints, cleanup_partitions] >> check_storage >> daily_report
