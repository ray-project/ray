"""
Schema management for Ray Data SQL API.

This module provides the SchemaManager class for managing table schemas
and inferring schema information from Ray Datasets.
"""

from typing import Any, Dict, Optional

from ray.data import Dataset

from ray.data.sql.config import TableSchema
from ray.data.sql.utils import setup_logger


class SchemaManager:
    """Manages schema information for tables and columns.

    The SchemaManager maintains a registry of table schemas and provides
    methods for schema inference and retrieval.

    Examples:
        .. testcode::

            manager = SchemaManager()
            manager.register_schema("users", {"id": "INTEGER", "name": "VARCHAR"})
            schema = manager.get_schema("users")
    """

    def __init__(self):
        self._schemas: Dict[str, TableSchema] = {}
        self._logger = setup_logger("SchemaManager")

    def register_schema(self, table_name: str, schema: TableSchema) -> None:
        """Register schema for a table.

        Args:
            table_name: Name of the table.
            schema: TableSchema object containing column information.
        """
        self._schemas[table_name] = schema
        self._logger.debug(f"Registered schema for table '{table_name}': {schema}")

    def get_schema(self, table_name: str) -> Optional[TableSchema]:
        """Get schema for a table.

        Args:
            table_name: Name of the table.

        Returns:
            TableSchema object if found, None otherwise.
        """
        return self._schemas.get(table_name)

    def infer_schema_from_dataset(self, table_name: str, dataset: Dataset) -> None:
        """Infer schema from a Ray Dataset.

        Args:
            table_name: Name of the table.
            dataset: Ray Dataset to infer schema from.
        """
        try:
            sample_rows = dataset.take(min(10, dataset.count()))
            if not sample_rows:
                return

            schema = TableSchema(name=table_name)
            for row in sample_rows:
                for col, value in row.items():
                    if col not in schema.columns:
                        schema.add_column(col, self._infer_type(value))

            self.register_schema(table_name, schema)
        except Exception as e:
            self._logger.warning(f"Could not infer schema for table '{table_name}': {e}")

    def _infer_type(self, value: Any) -> str:
        """Infer SQL type from Python value.

        Args:
            value: Python value to infer type from.

        Returns:
            SQL type string.
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "DOUBLE"
        elif isinstance(value, str):
            return "VARCHAR"
        else:
            return "VARCHAR"

    def list_schemas(self) -> Dict[str, TableSchema]:
        """List all registered schemas.

        Returns:
            Dictionary mapping table names to schemas.
        """
        return self._schemas.copy()

    def clear_schemas(self) -> None:
        """Clear all registered schemas."""
        self._schemas.clear()
        self._logger.debug("Cleared all schemas")

    def has_schema(self, table_name: str) -> bool:
        """Check if a schema exists for a table.

        Args:
            table_name: Name of the table.

        Returns:
            True if schema exists, False otherwise.
        """
        return table_name in self._schemas 