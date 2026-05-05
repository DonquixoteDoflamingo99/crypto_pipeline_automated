"""
Aggregation DAG for computing hourly and daily crypto analytics.

This DAG runs hourly to:
1. Compute hourly OHLCV aggregates
2. Compute daily aggregates (at midnight)
3. Update materialized views
4. Run data quality checks
"""

from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# Configuration
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
DATASET = Variable.get("bigquery_dataset", default_var="crypto_pipeline")
LOCATION = Variable.get("bigquery_location", default_var="US")


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [Variable.get("alert_email", default_var="alerts@example.com")],
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# SQL Queries
HOURLY_AGGREGATION_SQL = """
MERGE `{project}.{dataset}.hourly_aggregates` T
USING (
    SELECT
        symbol,
        TIMESTAMP_TRUNC(trade_timestamp, HOUR) as hour_timestamp,
        ARRAY_AGG(price ORDER BY trade_timestamp ASC LIMIT 1)[OFFSET(0)] as open_price,
        MAX(price) as high_price,
        MIN(price) as low_price,
        ARRAY_AGG(price ORDER BY trade_timestamp DESC LIMIT 1)[OFFSET(0)] as close_price,
        SUM(quantity) as total_volume,
        COUNT(*) as total_trades,
        SUM(trade_value_usd) as total_value_usd,
        AVG(quantity) as avg_trade_size,
        SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) as buy_volume,
        SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END) as sell_volume,
        SUM(trade_value_usd) / NULLIF(SUM(quantity), 0) as vwap
    FROM `{project}.{dataset}.transactions`
    WHERE trade_timestamp >= TIMESTAMP_SUB(@execution_time, INTERVAL 2 HOUR)
      AND trade_timestamp < @execution_time
    GROUP BY symbol, TIMESTAMP_TRUNC(trade_timestamp, HOUR)
) S
ON T.symbol = S.symbol AND T.hour_timestamp = S.hour_timestamp
WHEN MATCHED THEN UPDATE SET
    open_price = S.open_price,
    high_price = S.high_price,
    low_price = S.low_price,
    close_price = S.close_price,
    total_volume = S.total_volume,
    total_trades = S.total_trades,
    total_value_usd = S.total_value_usd,
    avg_trade_size = S.avg_trade_size,
    buy_volume = S.buy_volume,
    sell_volume = S.sell_volume,
    vwap = S.vwap,
    aggregation_time = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ROW
"""


DAILY_AGGREGATION_SQL = """
MERGE `{project}.{dataset}.daily_aggregates` T
USING (
    SELECT
        symbol,
        DATE(hour_timestamp) as trade_date,
        ARRAY_AGG(open_price ORDER BY hour_timestamp ASC LIMIT 1)[OFFSET(0)] as open_price,
        MAX(high_price) as high_price,
        MIN(low_price) as low_price,
        ARRAY_AGG(close_price ORDER BY hour_timestamp DESC LIMIT 1)[OFFSET(0)] as close_price,
        SUM(total_volume) as total_volume,
        SUM(total_trades) as total_trades,
        SUM(total_value_usd) as total_value_usd,
        AVG(total_volume) as avg_hourly_volume,
        MAX(total_volume) as max_hourly_volume
    FROM `{project}.{dataset}.hourly_aggregates`
    WHERE DATE(hour_timestamp) = DATE_SUB(@execution_date, INTERVAL 1 DAY)
    GROUP BY symbol, DATE(hour_timestamp)
) S
ON T.symbol = S.symbol AND T.trade_date = S.trade_date
WHEN MATCHED THEN UPDATE SET
    open_price = S.open_price,
    high_price = S.high_price,
    low_price = S.low_price,
    close_price = S.close_price,
    total_volume = S.total_volume,
    total_trades = S.total_trades,
    total_value_usd = S.total_value_usd,
    avg_hourly_volume = S.avg_hourly_volume,
    max_hourly_volume = S.max_hourly_volume,
    price_change = S.close_price - S.open_price,
    price_change_pct = (S.close_price - S.open_price) / NULLIF(S.open_price, 0) * 100,
    aggregation_time = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ROW
"""


DATA_QUALITY_CHECK_SQL = """
SELECT
    COUNT(*) as total_rows,
    COUNTIF(symbol IS NULL) as null_symbols,
    COUNTIF(price <= 0) as invalid_prices,
    COUNTIF(quantity <= 0) as invalid_quantities
FROM `{project}.{dataset}.transactions`
WHERE trade_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
"""


def should_run_daily_aggregation(**context) -> str:
    """Check if we should run daily aggregation (at midnight)."""
    execution_date = context["execution_date"]
    if execution_date.hour == 0:
        return "run_daily_aggregation"
    return "skip_daily_aggregation"


def run_data_quality_checks(**context) -> None:
    """Run data quality checks and alert on issues."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    query = DATA_QUALITY_CHECK_SQL.format(project=PROJECT_ID, dataset=DATASET)
    results = client.query(query).result()

    for row in results:
        if row.null_symbols > 0 or row.invalid_prices > 0 or row.invalid_quantities > 0:
            # Log warning - in production, send alert
            print(f"Data quality issues detected: {dict(row)}")
            context["task_instance"].xcom_push(key="dq_issues", value=dict(row))


def update_dashboard_cache(**context) -> None:
    """Update cached data for dashboards."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    # Refresh materialized view or cache table
    refresh_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.dashboard_cache` AS
    SELECT
        symbol,
        DATE(hour_timestamp) as trade_date,
        SUM(total_volume) as daily_volume,
        SUM(total_trades) as daily_trades,
        SUM(total_value_usd) as daily_value,
        MIN(low_price) as daily_low,
        MAX(high_price) as daily_high
    FROM `{PROJECT_ID}.{DATASET}.hourly_aggregates`
    WHERE hour_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY symbol, DATE(hour_timestamp)
    """

    client.query(refresh_query).result()
    print("Dashboard cache updated")


# DAG Definition
with DAG(
    dag_id="crypto_aggregation_pipeline",
    default_args=default_args,
    description="Computes hourly and daily crypto aggregates",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "aggregation", "bigquery"],
    max_active_runs=1,
) as dag:

    # Run hourly aggregation
    hourly_aggregation = BigQueryInsertJobOperator(
        task_id="hourly_aggregation",
        configuration={
            "query": {
                "query": HOURLY_AGGREGATION_SQL.format(
                    project=PROJECT_ID,
                    dataset=DATASET,
                ),
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "execution_time",
                        "parameterType": {"type": "TIMESTAMP"},
                        "parameterValue": {"value": "{{ ts }}"},
                    }
                ],
            }
        },
        location=LOCATION,
    )

    # Check if daily aggregation should run
    check_daily = BranchPythonOperator(
        task_id="check_daily_aggregation",
        python_callable=should_run_daily_aggregation,
    )

    # Run daily aggregation
    daily_aggregation = BigQueryInsertJobOperator(
        task_id="run_daily_aggregation",
        configuration={
            "query": {
                "query": DAILY_AGGREGATION_SQL.format(
                    project=PROJECT_ID,
                    dataset=DATASET,
                ),
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "execution_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": "{{ ds }}"},
                    }
                ],
            }
        },
        location=LOCATION,
    )

    # Skip daily task
    skip_daily = PythonOperator(
        task_id="skip_daily_aggregation",
        python_callable=lambda: print("Skipping daily aggregation"),
    )

    # Data quality checks
    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Update dashboard cache
    update_cache = PythonOperator(
        task_id="update_dashboard_cache",
        python_callable=update_dashboard_cache,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task dependencies
    hourly_aggregation >> check_daily >> [daily_aggregation, skip_daily]
    [daily_aggregation, skip_daily] >> data_quality >> update_cache
