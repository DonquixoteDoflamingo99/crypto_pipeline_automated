"""
Logging configuration for the crypto pipeline.
"""

import logging
import sys
from typing import Optional

import structlog
from google.cloud import logging as cloud_logging


def setup_logging(
    level: str = "INFO",
    use_cloud_logging: bool = True,
    project_id: Optional[str] = None,
) -> None:
    """
    Configure structured logging for the pipeline.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        use_cloud_logging: Whether to send logs to Cloud Logging
        project_id: GCP project ID for Cloud Logging
    """
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if use_cloud_logging and project_id:
        # Add Cloud Logging integration
        try:
            client = cloud_logging.Client(project=project_id)
            client.setup_logging()
            processors.append(structlog.processors.JSONRenderer())
        except Exception as e:
            logging.warning(f"Could not setup Cloud Logging: {e}")
            processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
