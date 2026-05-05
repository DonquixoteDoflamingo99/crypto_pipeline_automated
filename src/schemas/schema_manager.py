"""
Schema Manager for handling BigQuery schema evolution.

Provides utilities for detecting schema changes, applying migrations,
and maintaining backward compatibility.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from src.config import get_config

logger = structlog.get_logger(__name__)


class SchemaManager:
    """
    Manages BigQuery schema evolution with backward compatibility.

    Features:
    - Automatic schema detection from data samples
    - Safe schema evolution (add columns only)
    - Schema versioning and tracking
    - Migration history
    """

    # Mapping from Python/JSON types to BigQuery types
    TYPE_MAPPING = {
        "str": "STRING",
        "int": "INTEGER",
        "float": "FLOAT64",
        "bool": "BOOLEAN",
        "list": "REPEATED",
        "dict": "RECORD",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "NoneType": "STRING",  # Default nullable strings
    }

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the Schema Manager.

        Args:
            project_id: GCP project ID
        """
        self.config = get_config()
        self.project_id = project_id or self.config.gcp.project_id
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = self.config.bigquery.dataset

        logger.info(
            "SchemaManager initialized",
            project_id=self.project_id,
            dataset=self.dataset_id,
        )

    def _python_type_to_bq(self, value: Any) -> str:
        """Convert Python value to BigQuery type string."""
        type_name = type(value).__name__
        return self.TYPE_MAPPING.get(type_name, "STRING")

    def _infer_schema_from_data(
        self,
        data: List[Dict[str, Any]],
    ) -> List[SchemaField]:
        """
        Infer BigQuery schema from sample data.

        Args:
            data: List of dictionaries representing records

        Returns:
            List of BigQuery SchemaField objects
        """
        if not data:
            return []

        # Collect all unique fields and their types
        field_types: Dict[str, set] = {}

        for record in data:
            for key, value in record.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(self._python_type_to_bq(value))

        # Build schema fields
        schema_fields = []
        for field_name, types in field_types.items():
            # Use the most specific type (prefer non-STRING if available)
            types_list = list(types)
            if len(types_list) == 1:
                bq_type = types_list[0]
            elif "STRING" in types_list:
                types_list.remove("STRING")
                bq_type = types_list[0] if types_list else "STRING"
            else:
                bq_type = types_list[0]

            schema_fields.append(
                SchemaField(
                    name=field_name,
                    field_type=bq_type,
                    mode="NULLABLE",
                )
            )

        return schema_fields

    def get_table_schema(self, table_id: str) -> Optional[List[SchemaField]]:
        """
        Get current schema of a BigQuery table.

        Args:
            table_id: BigQuery table ID

        Returns:
            List of SchemaField or None if table doesn't exist
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"

        try:
            table = self.client.get_table(full_table_id)
            return list(table.schema)
        except Exception as e:
            logger.warning(
                "Could not get table schema",
                table=full_table_id,
                error=str(e),
            )
            return None

    def detect_schema_changes(
        self,
        table_id: str,
        new_schema: List[SchemaField],
    ) -> Dict[str, List[SchemaField]]:
        """
        Detect differences between current and new schema.

        Args:
            table_id: BigQuery table ID
            new_schema: Proposed new schema

        Returns:
            Dictionary with 'added', 'removed', and 'modified' fields
        """
        current_schema = self.get_table_schema(table_id)

        if current_schema is None:
            return {"added": new_schema, "removed": [], "modified": []}

        current_fields = {f.name: f for f in current_schema}
        new_fields = {f.name: f for f in new_schema}

        added = [f for name, f in new_fields.items() if name not in current_fields]
        removed = [f for name, f in current_fields.items() if name not in new_fields]

        modified = []
        for name, new_field in new_fields.items():
            if name in current_fields:
                current_field = current_fields[name]
                if new_field.field_type != current_field.field_type:
                    modified.append(new_field)

        return {
            "added": added,
            "removed": removed,
            "modified": modified,
        }

    def evolve_schema(
        self,
        table_id: str,
        new_fields: List[SchemaField],
        dry_run: bool = False,
    ) -> bool:
        """
        Safely evolve table schema by adding new fields.

        Args:
            table_id: BigQuery table ID
            new_fields: New fields to add
            dry_run: If True, only validate without applying

        Returns:
            True if evolution was successful or would be successful
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"

        if not new_fields:
            logger.info("No schema evolution needed", table=table_id)
            return True

        try:
            table = self.client.get_table(full_table_id)
            current_schema = list(table.schema)

            # Add new fields
            updated_schema = current_schema + new_fields

            if dry_run:
                logger.info(
                    "Schema evolution (dry run)",
                    table=table_id,
                    new_fields=[f.name for f in new_fields],
                )
                return True

            table.schema = updated_schema
            self.client.update_table(table, ["schema"])

            # Log schema evolution
            self._log_schema_change(table_id, new_fields)

            logger.info(
                "Schema evolved successfully",
                table=table_id,
                new_fields=[f.name for f in new_fields],
            )
            return True

        except Exception as e:
            logger.error(
                "Schema evolution failed",
                table=table_id,
                error=str(e),
            )
            return False

    def _log_schema_change(
        self,
        table_id: str,
        new_fields: List[SchemaField],
    ) -> None:
        """Log schema change to schema history table."""
        history_table = f"{self.project_id}.{self.dataset_id}.schema_history"

        # Create schema version hash
        schema_str = json.dumps(
            [{"name": f.name, "type": f.field_type} for f in new_fields],
            sort_keys=True,
        )
        version_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:12]

        record = {
            "table_id": table_id,
            "version_hash": version_hash,
            "fields_added": json.dumps([f.name for f in new_fields]),
            "change_time": datetime.now(timezone.utc).isoformat(),
        }

        try:
            errors = self.client.insert_rows_json(history_table, [record])
            if errors:
                logger.warning("Failed to log schema change", errors=errors)
        except Exception as e:
            logger.warning("Schema history logging failed", error=str(e))

    def create_table_with_schema(
        self,
        table_id: str,
        schema: List[SchemaField],
        partition_field: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
    ) -> bool:
        """
        Create a new BigQuery table with the specified schema.

        Args:
            table_id: Table ID to create
            schema: Table schema
            partition_field: Optional field for time partitioning
            cluster_fields: Optional fields for clustering

        Returns:
            True if table was created successfully
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"

        table = bigquery.Table(full_table_id, schema=schema)

        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

        if cluster_fields:
            table.clustering_fields = cluster_fields

        try:
            self.client.create_table(table, exists_ok=True)
            logger.info(
                "Table created",
                table=full_table_id,
                partitioned_by=partition_field,
                clustered_by=cluster_fields,
            )
            return True
        except Exception as e:
            logger.error("Table creation failed", table=full_table_id, error=str(e))
            return False
