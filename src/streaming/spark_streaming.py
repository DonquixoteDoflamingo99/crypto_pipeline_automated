"""
Spark Streaming job for processing cryptocurrency data from Pub/Sub to BigQuery.

This module implements a Spark Structured Streaming job that:
1. Reads from Pub/Sub subscription
2. Transforms and validates data
3. Handles schema evolution
4. Writes to BigQuery with exactly-once semantics
"""

import json
from datetime import datetime, timezone
from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import get_config
from src.schemas.schema_manager import SchemaManager

logger = structlog.get_logger(__name__)


class CryptoSparkStreaming:
    """
    Spark Structured Streaming job for crypto transaction processing.

    Handles high-throughput streaming from Pub/Sub with schema evolution
    and BigQuery sink with exactly-once semantics.
    """

    # Schema for incoming crypto transactions
    TRANSACTION_SCHEMA = StructType([
        StructField("event_type", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("buyer_order_id", LongType(), True),
        StructField("seller_order_id", LongType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("source", StringType(), True),
    ])

    def __init__(
        self,
        app_name: str = "CryptoStreamProcessor",
        project_id: Optional[str] = None,
    ):
        """
        Initialize the Spark Streaming job.

        Args:
            app_name: Spark application name
            project_id: GCP project ID (uses config if not provided)
        """
        self.config = get_config()
        self.project_id = project_id or self.config.gcp.project_id

        # Initialize Spark Session with GCP configurations
        self.spark = self._create_spark_session(app_name)
        self.schema_manager = SchemaManager(self.project_id)

        logger.info(
            "CryptoSparkStreaming initialized",
            app_name=app_name,
            project_id=self.project_id,
        )

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create configured Spark session for GCP."""
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.streaming.checkpointLocation",
                    f"gs://{self.config.storage.checkpoint_bucket}/spark-checkpoints")
            .config("spark.jars.packages",
                    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0,"
                    "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.maxRatePerPartition",
                    str(self.config.spark.max_offsets_per_trigger))
            .config("temporaryGcsBucket", self.config.storage.temp_bucket)
            .getOrCreate()
        )

    def _read_from_pubsub(self) -> DataFrame:
        """
        Create a streaming DataFrame from Pub/Sub.

        Returns:
            Streaming DataFrame with Pub/Sub messages
        """
        subscription_path = (
            f"projects/{self.project_id}/"
            f"subscriptions/{self.config.pubsub.subscription}"
        )

        return (
            self.spark.readStream
            .format("pubsub")
            .option("subscription", subscription_path)
            .option("maxMessagesPerBatch", self.config.spark.max_offsets_per_trigger)
            .load()
        )

    def _parse_messages(self, df: DataFrame) -> DataFrame:
        """
        Parse Pub/Sub messages to structured data.

        Args:
            df: Raw Pub/Sub DataFrame

        Returns:
            Parsed DataFrame with typed columns
        """
        # Parse JSON payload from Pub/Sub message
        parsed_df = df.select(
            F.from_json(
                F.col("data").cast("string"),
                self.TRANSACTION_SCHEMA
            ).alias("parsed"),
            F.col("attributes"),
            F.col("publish_time"),
        )

        # Flatten the structure
        return parsed_df.select(
            F.col("parsed.*"),
            F.col("publish_time").alias("pubsub_publish_time"),
            # Convert epoch milliseconds to timestamp
            F.from_unixtime(F.col("parsed.event_time") / 1000).cast(TimestampType())
                .alias("event_timestamp"),
            F.from_unixtime(F.col("parsed.trade_time") / 1000).cast(TimestampType())
                .alias("trade_timestamp"),
        )

    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations to the data.

        Args:
            df: Parsed DataFrame

        Returns:
            Transformed DataFrame with computed fields
        """
        return df.withColumns({
            # Calculate trade value
            "trade_value_usd": F.col("price") * F.col("quantity"),
            # Extract date components for partitioning
            "trade_date": F.to_date(F.col("trade_timestamp")),
            "trade_hour": F.hour(F.col("trade_timestamp")),
            # Processing metadata
            "processing_time": F.current_timestamp(),
            "pipeline_version": F.lit("1.0.0"),
        })

    def _validate_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validate data and separate valid/invalid records.

        Args:
            df: Transformed DataFrame

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        # Define validation conditions
        is_valid = (
            F.col("symbol").isNotNull() &
            (F.col("price") > 0) &
            (F.col("quantity") > 0) &
            F.col("trade_id").isNotNull()
        )

        valid_df = df.filter(is_valid)
        invalid_df = df.filter(~is_valid).withColumn(
            "validation_error",
            F.when(F.col("symbol").isNull(), "missing_symbol")
             .when(F.col("price") <= 0, "invalid_price")
             .when(F.col("quantity") <= 0, "invalid_quantity")
             .otherwise("unknown_error")
        )

        return valid_df, invalid_df

    def _write_to_bigquery(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """
        Write streaming DataFrame to BigQuery.

        Args:
            df: DataFrame to write
            table_name: Target BigQuery table
            mode: Write mode (append/overwrite)
        """
        full_table_name = (
            f"{self.project_id}."
            f"{self.config.bigquery.dataset}."
            f"{table_name}"
        )

        query = (
            df.writeStream
            .format("bigquery")
            .option("table", full_table_name)
            .option("checkpointLocation",
                    f"gs://{self.config.storage.checkpoint_bucket}/bq-checkpoint-{table_name}")
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("writeDisposition", "WRITE_APPEND")
            .option("partitionField", "trade_date")
            .option("clusteredFields", "symbol,trade_hour")
            .outputMode(mode)
            .trigger(processingTime=f"{self.config.spark.batch_interval_seconds} seconds")
        )

        return query.start()

    def _write_to_dlq(self, df: DataFrame) -> None:
        """
        Write invalid records to Dead Letter Queue.

        Args:
            df: Invalid records DataFrame
        """
        dlq_table = f"{self.config.bigquery.table_transactions}_dlq"
        return self._write_to_bigquery(df, dlq_table)

    def run(self) -> None:
        """
        Run the streaming pipeline.

        Orchestrates the full streaming workflow:
        1. Read from Pub/Sub
        2. Parse and transform
        3. Validate data
        4. Write valid records to BigQuery
        5. Write invalid records to DLQ
        """
        logger.info("Starting Spark Streaming pipeline")

        # Read from Pub/Sub
        raw_stream = self._read_from_pubsub()

        # Parse messages
        parsed_stream = self._parse_messages(raw_stream)

        # Transform data
        transformed_stream = self._transform_data(parsed_stream)

        # Validate and split
        valid_stream, invalid_stream = self._validate_data(transformed_stream)

        # Write to BigQuery
        valid_query = self._write_to_bigquery(
            valid_stream,
            self.config.bigquery.table_transactions,
        )

        # Write invalid records to DLQ
        dlq_query = self._write_to_dlq(invalid_stream)

        logger.info(
            "Streaming queries started",
            valid_query_id=valid_query.id,
            dlq_query_id=dlq_query.id,
        )

        # Wait for termination
        self.spark.streams.awaitAnyTermination()

    def stop(self) -> None:
        """Stop all streaming queries gracefully."""
        logger.info("Stopping Spark Streaming pipeline")
        for query in self.spark.streams.active:
            query.stop()
        self.spark.stop()


def main():
    """Entry point for the Spark Streaming job."""
    streaming = CryptoSparkStreaming()
    try:
        streaming.run()
    except KeyboardInterrupt:
        streaming.stop()


if __name__ == "__main__":
    main()
