"""
BigQuery client wrapper with common operations.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig

from src.config import get_config

logger = structlog.get_logger(__name__)


class BigQueryClient:
    """
    Wrapper around BigQuery client with common operations.

    Provides simplified methods for:
    - Table management
    - Data loading
    - Query execution
    - Partition management
    """

    def __init__(self, project_id: Optional[str] = None, dataset_id: Optional[str] = None):
        """
        Initialize the BigQuery client.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        config = get_config()
        self.project_id = project_id or config.gcp.project_id
        self.dataset_id = dataset_id or config.bigquery.dataset
        self.location = config.bigquery.location

        self.client = bigquery.Client(
            project=self.project_id,
            location=self.location,
        )

        logger.info(
            "BigQueryClient initialized",
            project=self.project_id,
            dataset=self.dataset_id,
        )

    def _get_full_table_id(self, table_id: str) -> str:
        """Get fully qualified table ID."""
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

    def execute_query(
        self,
        query: str,
        parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        dry_run: bool = False,
    ) -> bigquery.table.RowIterator:
        """
        Execute a BigQuery query.

        Args:
            query: SQL query string
            parameters: Optional query parameters
            dry_run: If True, only estimate costs

        Returns:
            Query results iterator
        """
        job_config = QueryJobConfig(
            query_parameters=parameters or [],
            dry_run=dry_run,
        )

        logger.debug("Executing query", query=query[:100])

        job = self.client.query(query, job_config=job_config)

        if dry_run:
            logger.info(
                "Query dry run",
                bytes_processed=job.total_bytes_processed,
            )
            return None

        results = job.result()

        logger.debug(
            "Query completed",
            rows_affected=job.num_dml_affected_rows,
            bytes_processed=job.total_bytes_processed,
        )

        return results

    def insert_rows(
        self,
        table_id: str,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Insert rows into a table using streaming insert.

        Args:
            table_id: Target table ID
            rows: List of row dictionaries

        Returns:
            List of errors (empty if successful)
        """
        full_table_id = self._get_full_table_id(table_id)

        errors = self.client.insert_rows_json(full_table_id, rows)

        if errors:
            logger.error(
                "Insert errors",
                table=table_id,
                error_count=len(errors),
            )
        else:
            logger.debug(
                "Rows inserted",
                table=table_id,
                row_count=len(rows),
            )

        return errors

    def load_from_gcs(
        self,
        table_id: str,
        source_uri: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_APPEND",
        source_format: str = "PARQUET",
    ) -> bigquery.LoadJob:
        """
        Load data from Cloud Storage into BigQuery.

        Args:
            table_id: Target table ID
            source_uri: GCS URI (gs://bucket/path)
            schema: Optional schema (auto-detected for Parquet)
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
            source_format: PARQUET, AVRO, CSV, JSON, etc.

        Returns:
            Load job
        """
        full_table_id = self._get_full_table_id(table_id)

        job_config = LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, source_format),
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
            schema=schema,
            autodetect=schema is None,
        )

        logger.info(
            "Loading from GCS",
            table=table_id,
            source=source_uri,
        )

        job = self.client.load_table_from_uri(
            source_uri,
            full_table_id,
            job_config=job_config,
        )

        job.result()  # Wait for completion

        logger.info(
            "Load completed",
            table=table_id,
            rows_loaded=job.output_rows,
        )

        return job

    def get_table_info(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata about a table.

        Args:
            table_id: Table ID

        Returns:
            Dictionary with table metadata
        """
        full_table_id = self._get_full_table_id(table_id)

        try:
            table = self.client.get_table(full_table_id)

            return {
                "table_id": table_id,
                "full_table_id": full_table_id,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
                "schema_fields": len(table.schema),
                "partitioning": table.time_partitioning.field if table.time_partitioning else None,
                "clustering": table.clustering_fields,
            }

        except Exception as e:
            logger.warning("Could not get table info", table=table_id, error=str(e))
            return None

    def list_partitions(
        self,
        table_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[str]:
        """
        List partitions for a partitioned table.

        Args:
            table_id: Table ID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of partition IDs
        """
        query = f"""
            SELECT DISTINCT _PARTITIONDATE as partition_date
            FROM `{self._get_full_table_id(table_id)}`
            WHERE TRUE
        """

        if start_date:
            query += f" AND _PARTITIONDATE >= '{start_date.strftime('%Y-%m-%d')}'"
        if end_date:
            query += f" AND _PARTITIONDATE <= '{end_date.strftime('%Y-%m-%d')}'"

        query += " ORDER BY partition_date DESC"

        results = self.execute_query(query)

        return [row.partition_date.strftime("%Y%m%d") for row in results]

    def delete_partition(
        self,
        table_id: str,
        partition_id: str,
    ) -> bool:
        """
        Delete a specific partition.

        Args:
            table_id: Table ID
            partition_id: Partition ID (YYYYMMDD format)

        Returns:
            True if successful
        """
        full_table_id = self._get_full_table_id(table_id)
        partition_table = f"{full_table_id}${partition_id}"

        try:
            self.client.delete_table(partition_table)
            logger.info("Partition deleted", table=table_id, partition=partition_id)
            return True
        except Exception as e:
            logger.error("Partition deletion failed", error=str(e))
            return False
