"""
Schema Registry for versioned schema management.

Provides a centralized registry for tracking schema versions,
compatibility checks, and schema metadata.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import structlog
from google.cloud import bigquery

from src.config import get_config

logger = structlog.get_logger(__name__)


class CompatibilityMode(Enum):
    """Schema compatibility modes."""

    BACKWARD = "backward"  # New schema can read old data
    FORWARD = "forward"    # Old schema can read new data
    FULL = "full"          # Both backward and forward
    NONE = "none"          # No compatibility checks


@dataclass
class SchemaVersion:
    """Represents a schema version."""

    version_id: str
    schema_hash: str
    schema_definition: Dict[str, Any]
    created_at: datetime
    is_active: bool
    compatibility_mode: CompatibilityMode


class SchemaRegistry:
    """
    Centralized schema registry for managing schema versions.

    Features:
    - Schema version tracking
    - Compatibility validation
    - Schema metadata storage
    - Version history
    """

    REGISTRY_TABLE = "schema_registry"

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the Schema Registry.

        Args:
            project_id: GCP project ID
        """
        self.config = get_config()
        self.project_id = project_id or self.config.gcp.project_id
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = self.config.bigquery.dataset

        self._ensure_registry_table()

        logger.info(
            "SchemaRegistry initialized",
            project_id=self.project_id,
            dataset=self.dataset_id,
        )

    def _ensure_registry_table(self) -> None:
        """Ensure the schema registry table exists."""
        full_table_id = f"{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}"

        schema = [
            bigquery.SchemaField("subject", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("version_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_definition", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("compatibility_mode", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("created_by", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        ]

        table = bigquery.Table(full_table_id, schema=schema)

        try:
            self.client.create_table(table, exists_ok=True)
        except Exception as e:
            logger.warning("Could not create registry table", error=str(e))

    def _compute_schema_hash(self, schema_definition: Dict[str, Any]) -> str:
        """Compute a hash of the schema definition."""
        schema_str = json.dumps(schema_definition, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

    def register_schema(
        self,
        subject: str,
        schema_definition: Dict[str, Any],
        compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD,
        description: Optional[str] = None,
    ) -> Optional[str]:
        """
        Register a new schema version.

        Args:
            subject: Schema subject (e.g., table name)
            schema_definition: The schema definition
            compatibility_mode: Compatibility mode for this schema
            description: Optional description

        Returns:
            Version ID if registered, None if schema already exists
        """
        schema_hash = self._compute_schema_hash(schema_definition)

        # Check if this exact schema already exists
        existing = self.get_schema_by_hash(subject, schema_hash)
        if existing:
            logger.info(
                "Schema already registered",
                subject=subject,
                version=existing.version_id,
            )
            return None

        # Check compatibility with latest version
        latest = self.get_latest_schema(subject)
        if latest and not self._check_compatibility(
            latest.schema_definition,
            schema_definition,
            compatibility_mode,
        ):
            logger.error(
                "Schema incompatible with latest version",
                subject=subject,
                mode=compatibility_mode.value,
            )
            return None

        # Generate new version ID
        version_num = 1 if latest is None else int(latest.version_id.split("-")[1]) + 1
        version_id = f"v-{version_num:04d}"

        # Deactivate previous versions
        self._deactivate_previous_versions(subject)

        # Insert new schema version
        full_table_id = f"{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}"

        record = {
            "subject": subject,
            "version_id": version_id,
            "schema_hash": schema_hash,
            "schema_definition": json.dumps(schema_definition),
            "compatibility_mode": compatibility_mode.value,
            "is_active": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "created_by": "pipeline",
            "description": description,
        }

        try:
            errors = self.client.insert_rows_json(full_table_id, [record])
            if errors:
                logger.error("Failed to register schema", errors=errors)
                return None

            logger.info(
                "Schema registered",
                subject=subject,
                version=version_id,
                hash=schema_hash[:12],
            )
            return version_id

        except Exception as e:
            logger.error("Schema registration failed", error=str(e))
            return None

    def get_latest_schema(self, subject: str) -> Optional[SchemaVersion]:
        """Get the latest active schema version for a subject."""
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}`
            WHERE subject = @subject AND is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("subject", "STRING", subject),
            ]
        )

        try:
            results = self.client.query(query, job_config=job_config).result()
            for row in results:
                return SchemaVersion(
                    version_id=row.version_id,
                    schema_hash=row.schema_hash,
                    schema_definition=json.loads(row.schema_definition),
                    created_at=row.created_at,
                    is_active=row.is_active,
                    compatibility_mode=CompatibilityMode(row.compatibility_mode),
                )
            return None
        except Exception as e:
            logger.error("Failed to get latest schema", error=str(e))
            return None

    def get_schema_by_hash(
        self,
        subject: str,
        schema_hash: str,
    ) -> Optional[SchemaVersion]:
        """Get a schema version by its hash."""
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}`
            WHERE subject = @subject AND schema_hash = @hash
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("subject", "STRING", subject),
                bigquery.ScalarQueryParameter("hash", "STRING", schema_hash),
            ]
        )

        try:
            results = self.client.query(query, job_config=job_config).result()
            for row in results:
                return SchemaVersion(
                    version_id=row.version_id,
                    schema_hash=row.schema_hash,
                    schema_definition=json.loads(row.schema_definition),
                    created_at=row.created_at,
                    is_active=row.is_active,
                    compatibility_mode=CompatibilityMode(row.compatibility_mode),
                )
            return None
        except Exception as e:
            logger.error("Failed to get schema by hash", error=str(e))
            return None

    def get_schema_history(self, subject: str) -> List[SchemaVersion]:
        """Get all schema versions for a subject."""
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}`
            WHERE subject = @subject
            ORDER BY created_at DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("subject", "STRING", subject),
            ]
        )

        versions = []
        try:
            results = self.client.query(query, job_config=job_config).result()
            for row in results:
                versions.append(SchemaVersion(
                    version_id=row.version_id,
                    schema_hash=row.schema_hash,
                    schema_definition=json.loads(row.schema_definition),
                    created_at=row.created_at,
                    is_active=row.is_active,
                    compatibility_mode=CompatibilityMode(row.compatibility_mode),
                ))
        except Exception as e:
            logger.error("Failed to get schema history", error=str(e))

        return versions

    def _deactivate_previous_versions(self, subject: str) -> None:
        """Deactivate all previous versions for a subject."""
        query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.REGISTRY_TABLE}`
            SET is_active = FALSE
            WHERE subject = @subject AND is_active = TRUE
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("subject", "STRING", subject),
            ]
        )

        try:
            self.client.query(query, job_config=job_config).result()
        except Exception as e:
            logger.warning("Failed to deactivate versions", error=str(e))

    def _check_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        mode: CompatibilityMode,
    ) -> bool:
        """
        Check if new schema is compatible with old schema.

        Args:
            old_schema: Previous schema definition
            new_schema: New schema definition
            mode: Compatibility mode to check

        Returns:
            True if schemas are compatible
        """
        if mode == CompatibilityMode.NONE:
            return True

        old_fields = set(old_schema.get("fields", {}).keys())
        new_fields = set(new_schema.get("fields", {}).keys())

        if mode == CompatibilityMode.BACKWARD:
            # All old fields must exist in new schema
            removed = old_fields - new_fields
            if removed:
                logger.warning("Backward incompatible: fields removed", fields=removed)
                return False
            return True

        elif mode == CompatibilityMode.FORWARD:
            # All new fields must exist in old schema
            added = new_fields - old_fields
            if added:
                logger.warning("Forward incompatible: fields added", fields=added)
                return False
            return True

        elif mode == CompatibilityMode.FULL:
            # Both directions must be compatible
            if old_fields != new_fields:
                logger.warning("Full incompatible: fields differ")
                return False
            return True

        return True
