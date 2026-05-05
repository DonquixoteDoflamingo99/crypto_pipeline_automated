"""
Configuration management for the crypto pipeline.
"""

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GCPConfig(BaseSettings):
    """GCP-specific configuration."""

    model_config = SettingsConfigDict(env_prefix="GCP_", env_file=".env", extra="ignore")

    project_id: str = Field(..., description="GCP Project ID")
    region: str = Field(default="us-central1", description="GCP Region")
    zone: str = Field(default="us-central1-a", description="GCP Zone")


class PubSubConfig(BaseSettings):
    """Pub/Sub configuration."""

    model_config = SettingsConfigDict(env_prefix="PUBSUB_", env_file=".env", extra="ignore")

    topic: str = Field(default="crypto-transactions", description="Main Pub/Sub topic")
    subscription: str = Field(
        default="crypto-transactions-sub", description="Pub/Sub subscription"
    )
    dlq_topic: str = Field(
        default="crypto-transactions-dlq", description="Dead letter queue topic"
    )
    max_messages: int = Field(default=1000, description="Max messages per pull")
    ack_deadline_seconds: int = Field(default=60, description="Acknowledgment deadline")


class BigQueryConfig(BaseSettings):
    """BigQuery configuration."""

    model_config = SettingsConfigDict(env_prefix="BIGQUERY_", env_file=".env", extra="ignore")

    dataset: str = Field(default="crypto_pipeline", description="BigQuery dataset")
    table_transactions: str = Field(default="transactions", description="Transactions table")
    table_aggregates: str = Field(default="hourly_aggregates", description="Aggregates table")
    location: str = Field(default="US", description="BigQuery location")


class DataprocConfig(BaseSettings):
    """Dataproc configuration."""

    model_config = SettingsConfigDict(env_prefix="DATAPROC_", env_file=".env", extra="ignore")

    cluster: str = Field(default="crypto-spark-cluster", description="Dataproc cluster name")
    region: str = Field(default="us-central1", description="Dataproc region")
    workers: int = Field(default=3, description="Number of worker nodes")
    worker_machine_type: str = Field(default="n2-standard-4", description="Worker machine type")
    master_machine_type: str = Field(default="n2-standard-4", description="Master machine type")


class StorageConfig(BaseSettings):
    """Cloud Storage configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gcs_bucket: str = Field(default="crypto-pipeline-data", description="Main GCS bucket")
    checkpoint_bucket: str = Field(
        default="crypto-pipeline-checkpoints", description="Checkpoint bucket"
    )
    temp_bucket: str = Field(default="crypto-pipeline-temp", description="Temp bucket")


class SparkConfig(BaseSettings):
    """Spark Streaming configuration."""

    model_config = SettingsConfigDict(env_prefix="SPARK_", env_file=".env", extra="ignore")

    batch_interval_seconds: int = Field(default=30, description="Micro-batch interval")
    checkpoint_interval: int = Field(default=60, description="Checkpoint interval")
    max_offsets_per_trigger: int = Field(default=100000, description="Max records per batch")


class CryptoAPIConfig(BaseSettings):
    """Crypto API configuration for data ingestion."""

    model_config = SettingsConfigDict(env_prefix="CRYPTO_", env_file=".env", extra="ignore")

    api_url: str = Field(
        default="wss://stream.binance.com:9443/ws", description="WebSocket API URL"
    )
    symbols: str = Field(
        default="btcusdt,ethusdt,bnbusdt,adausdt,dogeusdt",
        description="Comma-separated trading symbols",
    )

    @property
    def symbol_list(self) -> List[str]:
        """Get symbols as a list."""
        return [s.strip().lower() for s in self.symbols.split(",")]


class PipelineConfig(BaseSettings):
    """Main pipeline configuration aggregating all configs."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gcp: GCPConfig = Field(default_factory=GCPConfig)
    pubsub: PubSubConfig = Field(default_factory=PubSubConfig)
    bigquery: BigQueryConfig = Field(default_factory=BigQueryConfig)
    dataproc: DataprocConfig = Field(default_factory=DataprocConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)
    crypto_api: CryptoAPIConfig = Field(default_factory=CryptoAPIConfig)

    enable_monitoring: bool = Field(default=True, description="Enable Cloud Monitoring")


@lru_cache()
def get_config() -> PipelineConfig:
    """Get cached pipeline configuration."""
    return PipelineConfig()
