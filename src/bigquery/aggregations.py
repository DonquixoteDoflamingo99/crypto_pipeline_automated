"""
Aggregation service for computing analytics on crypto data.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import structlog
from google.cloud import bigquery

from src.config import get_config
from .client import BigQueryClient

logger = structlog.get_logger(__name__)


class AggregationService:
    """
    Service for computing and storing aggregated metrics.

    Computes:
    - Hourly/Daily OHLCV (Open, High, Low, Close, Volume)
    - Moving averages
    - Trading statistics
    """

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the aggregation service.

        Args:
            project_id: GCP project ID
        """
        self.config = get_config()
        self.project_id = project_id or self.config.gcp.project_id
        self.bq_client = BigQueryClient(self.project_id)
        self.dataset = self.config.bigquery.dataset
        self.source_table = self.config.bigquery.table_transactions
        self.target_table = self.config.bigquery.table_aggregates

        logger.info(
            "AggregationService initialized",
            source=self.source_table,
            target=self.target_table,
        )

    def compute_hourly_aggregates(
        self,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        symbols: Optional[List[str]] = None,
    ) -> int:
        """
        Compute hourly OHLCV aggregates.

        Args:
            start_time: Start of aggregation window
            end_time: End of aggregation window (defaults to start + 1 hour)
            symbols: Optional list of symbols to aggregate

        Returns:
            Number of rows inserted
        """
        if end_time is None:
            end_time = start_time + timedelta(hours=1)

        symbol_filter = ""
        if symbols:
            symbol_list = ", ".join([f"'{s}'" for s in symbols])
            symbol_filter = f"AND symbol IN ({symbol_list})"

        query = f"""
            INSERT INTO `{self.project_id}.{self.dataset}.{self.target_table}`
            (
                symbol,
                hour_timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                total_volume,
                total_trades,
                total_value_usd,
                avg_trade_size,
                buy_volume,
                sell_volume,
                vwap,
                aggregation_time
            )
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
                SUM(trade_value_usd) / NULLIF(SUM(quantity), 0) as vwap,
                CURRENT_TIMESTAMP() as aggregation_time
            FROM `{self.project_id}.{self.dataset}.{self.source_table}`
            WHERE trade_timestamp >= @start_time
              AND trade_timestamp < @end_time
              {symbol_filter}
            GROUP BY symbol, hour_timestamp
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
            ]
        )

        job = self.bq_client.client.query(query, job_config=job_config)
        result = job.result()

        rows_inserted = job.num_dml_affected_rows or 0

        logger.info(
            "Hourly aggregates computed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
            rows_inserted=rows_inserted,
        )

        return rows_inserted

    def compute_daily_aggregates(
        self,
        date: datetime,
        symbols: Optional[List[str]] = None,
    ) -> int:
        """
        Compute daily aggregates from hourly data.

        Args:
            date: Date to aggregate
            symbols: Optional list of symbols

        Returns:
            Number of rows inserted
        """
        symbol_filter = ""
        if symbols:
            symbol_list = ", ".join([f"'{s}'" for s in symbols])
            symbol_filter = f"AND symbol IN ({symbol_list})"

        query = f"""
            INSERT INTO `{self.project_id}.{self.dataset}.daily_aggregates`
            (
                symbol,
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                total_volume,
                total_trades,
                total_value_usd,
                avg_hourly_volume,
                max_hourly_volume,
                price_change,
                price_change_pct,
                aggregation_time
            )
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
                MAX(total_volume) as max_hourly_volume,
                ARRAY_AGG(close_price ORDER BY hour_timestamp DESC LIMIT 1)[OFFSET(0)] -
                    ARRAY_AGG(open_price ORDER BY hour_timestamp ASC LIMIT 1)[OFFSET(0)] as price_change,
                (ARRAY_AGG(close_price ORDER BY hour_timestamp DESC LIMIT 1)[OFFSET(0)] -
                    ARRAY_AGG(open_price ORDER BY hour_timestamp ASC LIMIT 1)[OFFSET(0)]) /
                    NULLIF(ARRAY_AGG(open_price ORDER BY hour_timestamp ASC LIMIT 1)[OFFSET(0)], 0) * 100 as price_change_pct,
                CURRENT_TIMESTAMP() as aggregation_time
            FROM `{self.project_id}.{self.dataset}.{self.target_table}`
            WHERE DATE(hour_timestamp) = @trade_date
              {symbol_filter}
            GROUP BY symbol, trade_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("trade_date", "DATE", date.date()),
            ]
        )

        job = self.bq_client.client.query(query, job_config=job_config)
        result = job.result()

        rows_inserted = job.num_dml_affected_rows or 0

        logger.info(
            "Daily aggregates computed",
            date=date.strftime("%Y-%m-%d"),
            rows_inserted=rows_inserted,
        )

        return rows_inserted

    def get_latest_aggregates(
        self,
        symbol: str,
        hours: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Get latest hourly aggregates for a symbol.

        Args:
            symbol: Trading symbol
            hours: Number of hours to fetch

        Returns:
            List of aggregate records
        """
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset}.{self.target_table}`
            WHERE symbol = @symbol
              AND hour_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
            ORDER BY hour_timestamp DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
                bigquery.ScalarQueryParameter("hours", "INT64", hours),
            ]
        )

        results = self.bq_client.client.query(query, job_config=job_config).result()

        return [dict(row) for row in results]

    def get_volume_leaders(
        self,
        top_n: int = 10,
        hours: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Get top symbols by trading volume.

        Args:
            top_n: Number of top symbols
            hours: Time window in hours

        Returns:
            List of symbols with volume data
        """
        query = f"""
            SELECT
                symbol,
                SUM(total_volume) as total_volume,
                SUM(total_trades) as total_trades,
                SUM(total_value_usd) as total_value_usd,
                AVG(vwap) as avg_vwap
            FROM `{self.project_id}.{self.dataset}.{self.target_table}`
            WHERE hour_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
            GROUP BY symbol
            ORDER BY total_volume DESC
            LIMIT @top_n
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hours", "INT64", hours),
                bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
            ]
        )

        results = self.bq_client.client.query(query, job_config=job_config).result()

        return [dict(row) for row in results]
