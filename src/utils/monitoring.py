"""
Monitoring and metrics collection for the crypto pipeline.
"""

import time
from contextlib import contextmanager
from typing import Dict, Generator, Optional

import structlog
from google.cloud import monitoring_v3
from google.protobuf import timestamp_pb2

from src.config import get_config

logger = structlog.get_logger(__name__)


class MetricsCollector:
    """
    Collects and reports metrics to Cloud Monitoring.

    Provides methods for tracking:
    - Counter metrics (e.g., messages processed)
    - Gauge metrics (e.g., queue depth)
    - Distribution metrics (e.g., latency)
    """

    METRIC_PREFIX = "custom.googleapis.com/crypto_pipeline"

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the metrics collector.

        Args:
            project_id: GCP project ID
        """
        config = get_config()
        self.project_id = project_id or config.gcp.project_id
        self.project_name = f"projects/{self.project_id}"

        self.enabled = config.enable_monitoring

        if self.enabled:
            try:
                self.client = monitoring_v3.MetricServiceClient()
                logger.info("MetricsCollector initialized", project=self.project_id)
            except Exception as e:
                logger.warning("Could not initialize monitoring client", error=str(e))
                self.enabled = False
        else:
            self.client = None

    def _create_time_series(
        self,
        metric_type: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> monitoring_v3.TimeSeries:
        """Create a time series data point."""
        series = monitoring_v3.TimeSeries()
        series.metric.type = f"{self.METRIC_PREFIX}/{metric_type}"

        if labels:
            for key, val in labels.items():
                series.metric.labels[key] = val

        series.resource.type = "global"
        series.resource.labels["project_id"] = self.project_id

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10**9)

        interval = monitoring_v3.TimeInterval()
        interval.end_time.seconds = seconds
        interval.end_time.nanos = nanos

        point = monitoring_v3.Point()
        point.interval = interval
        point.value.double_value = value

        series.points.append(point)

        return series

    def record_counter(
        self,
        metric_name: str,
        value: int = 1,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a counter metric.

        Args:
            metric_name: Name of the metric
            value: Value to add
            labels: Optional labels
        """
        if not self.enabled:
            return

        try:
            series = self._create_time_series(
                f"counter/{metric_name}",
                float(value),
                labels,
            )

            self.client.create_time_series(
                name=self.project_name,
                time_series=[series],
            )

            logger.debug(
                "Counter recorded",
                metric=metric_name,
                value=value,
            )

        except Exception as e:
            logger.warning(
                "Failed to record counter",
                metric=metric_name,
                error=str(e),
            )

    def record_gauge(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a gauge metric.

        Args:
            metric_name: Name of the metric
            value: Current value
            labels: Optional labels
        """
        if not self.enabled:
            return

        try:
            series = self._create_time_series(
                f"gauge/{metric_name}",
                value,
                labels,
            )

            self.client.create_time_series(
                name=self.project_name,
                time_series=[series],
            )

            logger.debug(
                "Gauge recorded",
                metric=metric_name,
                value=value,
            )

        except Exception as e:
            logger.warning(
                "Failed to record gauge",
                metric=metric_name,
                error=str(e),
            )

    @contextmanager
    def record_latency(
        self,
        metric_name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Generator[None, None, None]:
        """
        Context manager to record operation latency.

        Args:
            metric_name: Name of the metric
            labels: Optional labels

        Usage:
            with metrics.record_latency("processing_time"):
                # do work
        """
        start_time = time.perf_counter()

        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000

            if self.enabled:
                try:
                    series = self._create_time_series(
                        f"latency/{metric_name}",
                        elapsed_ms,
                        labels,
                    )

                    self.client.create_time_series(
                        name=self.project_name,
                        time_series=[series],
                    )

                    logger.debug(
                        "Latency recorded",
                        metric=metric_name,
                        latency_ms=elapsed_ms,
                    )

                except Exception as e:
                    logger.warning(
                        "Failed to record latency",
                        metric=metric_name,
                        error=str(e),
                    )

    def record_batch_metrics(
        self,
        messages_processed: int,
        messages_failed: int,
        batch_duration_ms: float,
        symbol: Optional[str] = None,
    ) -> None:
        """
        Record metrics for a processed batch.

        Args:
            messages_processed: Number of successfully processed messages
            messages_failed: Number of failed messages
            batch_duration_ms: Batch processing duration in milliseconds
            symbol: Optional trading symbol for labeling
        """
        labels = {"symbol": symbol} if symbol else None

        self.record_counter("messages_processed", messages_processed, labels)
        self.record_counter("messages_failed", messages_failed, labels)
        self.record_gauge("batch_duration_ms", batch_duration_ms, labels)

        if messages_processed > 0:
            throughput = messages_processed / (batch_duration_ms / 1000)
            self.record_gauge("throughput_per_second", throughput, labels)
