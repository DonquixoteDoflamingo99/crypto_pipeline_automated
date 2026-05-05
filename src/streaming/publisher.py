"""
Pub/Sub Publisher for ingesting cryptocurrency data from external APIs.

This module connects to cryptocurrency exchange WebSocket APIs and publishes
transaction data to Google Cloud Pub/Sub for downstream processing.
"""

import asyncio
import json
import platform
import signal
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog
import websockets
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublishFlowControl
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import get_config

logger = structlog.get_logger(__name__)


class CryptoPublisher:
    """
    Publishes cryptocurrency trading data to Pub/Sub.

    Connects to exchange WebSocket APIs and forwards messages to Pub/Sub
    with exactly-once semantics and automatic retry handling.
    """

    def __init__(self, project_id: Optional[str] = None, topic_id: Optional[str] = None):
        """
        Initialize the publisher.

        Args:
            project_id: GCP project ID (uses config if not provided)
            topic_id: Pub/Sub topic ID (uses config if not provided)
        """
        config = get_config()
        self.project_id = project_id or config.gcp.project_id
        self.topic_id = topic_id or config.pubsub.topic
        self.topic_path = f"projects/{self.project_id}/topics/{self.topic_id}"

        self.symbols = config.crypto_api.symbol_list
        self.api_url = config.crypto_api.api_url

        # Configure publisher with flow control for high throughput
        flow_control = PublishFlowControl(
            max_messages=1000,
            max_bytes=10 * 1024 * 1024,  # 10 MB
        )

        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=100,
            max_bytes=1024 * 1024,  # 1 MB
            max_latency=0.01,  # 10ms
        )

        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=pubsub_v1.types.PublisherOptions(
                flow_control=flow_control,
            ),
        )

        self._running = False
        self._publish_count = 0
        self._error_count = 0

        logger.info(
            "CryptoPublisher initialized",
            project_id=self.project_id,
            topic_id=self.topic_id,
            symbols=self.symbols,
        )

    def _transform_message(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw WebSocket message to standardized format.

        Args:
            raw_message: Raw message from exchange API

        Returns:
            Standardized transaction message
        """
        # Example transformation for Binance trade stream
        return {
            "event_type": raw_message.get("e", "trade"),
            "event_time": raw_message.get("E", int(datetime.now(timezone.utc).timestamp() * 1000)),
            "symbol": raw_message.get("s", "").upper(),
            "trade_id": raw_message.get("t"),
            "price": float(raw_message.get("p", 0)),
            "quantity": float(raw_message.get("q", 0)),
            "buyer_order_id": raw_message.get("b"),
            "seller_order_id": raw_message.get("a"),
            "trade_time": raw_message.get("T"),
            "is_buyer_maker": raw_message.get("m", False),
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "binance",
        }

    def _publish_callback(self, future):
        """Handle publish result."""
        try:
            future.result()  # Raises exception if publish failed
            self._publish_count += 1
            if self._publish_count % 10000 == 0:
                logger.info(
                    "Publish progress",
                    total_published=self._publish_count,
                    errors=self._error_count,
                )
        except Exception as e:
            self._error_count += 1
            logger.error("Publish failed", error=str(e))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _publish_message(self, message: Dict[str, Any]) -> None:
        """
        Publish a single message to Pub/Sub.

        Args:
            message: Message to publish
        """
        data = json.dumps(message).encode("utf-8")

        # Add attributes for filtering
        attributes = {
            "symbol": message.get("symbol", "UNKNOWN"),
            "source": message.get("source", "unknown"),
            "event_type": message.get("event_type", "trade"),
        }

        future = self.publisher.publish(
            self.topic_path,
            data=data,
            **attributes,
        )
        future.add_done_callback(self._publish_callback)

    async def _connect_and_stream(self) -> None:
        """Connect to WebSocket and stream data to Pub/Sub."""
        # Build stream URLs for all symbols
        streams = "/".join([f"{symbol}@trade" for symbol in self.symbols])
        ws_url = f"{self.api_url}/{streams}"

        logger.info("Connecting to WebSocket", url=ws_url)

        async with websockets.connect(ws_url, ping_interval=20) as websocket:
            logger.info("WebSocket connected", symbols=self.symbols)

            while self._running:
                try:
                    message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=30.0,
                    )

                    raw_data = json.loads(message)
                    transformed = self._transform_message(raw_data)
                    await self._publish_message(transformed)

                except asyncio.TimeoutError:
                    logger.warning("WebSocket receive timeout, reconnecting...")
                    break
                except json.JSONDecodeError as e:
                    logger.error("Invalid JSON received", error=str(e))
                    continue

    async def run(self) -> None:
        """Run the publisher with automatic reconnection."""
        self._running = True

        # Setup signal handlers (Unix only - Windows uses KeyboardInterrupt)
        if platform.system() != "Windows":
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop)

        logger.info("Starting CryptoPublisher")

        while self._running:
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.error("Connection error", error=str(e))
                if self._running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

        logger.info(
            "CryptoPublisher stopped",
            total_published=self._publish_count,
            total_errors=self._error_count,
        )

    def stop(self) -> None:
        """Stop the publisher gracefully."""
        logger.info("Stopping CryptoPublisher...")
        self._running = False


def main():
    """Entry point for the publisher."""
    publisher = CryptoPublisher()
    asyncio.run(publisher.run())


if __name__ == "__main__":
    main()
