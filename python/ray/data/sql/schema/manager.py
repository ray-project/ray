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

    The SchemaManager is responsible for maintaining metadata about table schemas
    in the SQL engine. It provides automatic schema inference from Ray Datasets
    and serves as a central registry for table structure information.

    Key responsibilities:
    - Automatic schema inference from Ray Dataset samples
    - Storage and retrieval of table schema metadata
    - Type mapping from Python values to SQL types
    - Schema validation and consistency checking

    The manager uses sampling-based inference to determine column types from
    actual data, supporting common SQL types like INTEGER, DOUBLE, VARCHAR, etc.

    Examples:
        .. testcode::

            manager = SchemaManager()
            manager.register_schema("users", {"id": "INTEGER", "name": "VARCHAR"})
            schema = manager.get_schema("users")
    """

    def __init__(self):
        """Initialize an empty schema manager.

        Creates internal storage for schema mappings and sets up logging
        for schema operations.
        """
        # Internal mapping from table names to TableSchema objects
        self._schemas: Dict[str, TableSchema] = {}

        # Logger for debugging schema operations and inference
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
        """Infer schema from a Ray Dataset using sample-based analysis.

        This method automatically determines the schema of a Ray Dataset by sampling
        data rows and analyzing the types of values in each column. It uses a
        representative sample to infer SQL types without needing to process the
        entire dataset, making it efficient for large distributed datasets.

        The inference process:
        1. Takes a small sample (up to 10 rows) from the dataset
        2. Examines each column in the sample rows
        3. Infers SQL types based on Python value types
        4. Creates and registers a TableSchema with the inferred information

        Args:
            table_name: Name of the table to register the schema under.
            dataset: Ray Dataset to analyze for schema inference.
        """
        try:
            # Take a small sample for type inference (efficient for large datasets)
            sample_size = min(10, dataset.count())
            sample_rows = dataset.take(sample_size)

            # Handle empty datasets gracefully
            if not sample_rows:
                self._logger.debug(
                    f"Dataset '{table_name}' is empty, skipping schema inference"
                )
                return

            # Create a new schema object for this table
            schema = TableSchema(name=table_name)

            # Process each row in the sample to discover columns and types
            for row in sample_rows:
                for col, value in row.items():
                    # Only add new columns (first occurrence determines type)
                    if col not in schema.columns:
                        inferred_type = self._infer_type(value)
                        schema.add_column(col, inferred_type)
                        self._logger.debug(
                            f"Inferred column '{col}' as type '{inferred_type}'"
                        )

            # Register the completed schema
            self.register_schema(table_name, schema)

        except Exception as e:
            # Log warning but don't fail registration - schema inference is optional
            self._logger.warning(
                f"Could not infer schema for table '{table_name}': {e}"
            )

    def _infer_type(self, value: Any) -> str:
        """Infer SQL type from Python value using type mapping rules.

        This method maps Python value types to corresponding SQL types following
        standard SQL type conventions. It handles the most common data types
        encountered in Ray Datasets and provides sensible defaults for unknown types.

        Type mapping rules:
        - None -> NULL (missing/null values)
        - bool -> BOOLEAN (true/false values)
        - int -> INTEGER (whole numbers)
        - float -> DOUBLE (decimal numbers)
        - str -> VARCHAR (text strings)
        - other -> VARCHAR (default fallback for complex types)

        Args:
            value: Python value to analyze for type inference.

        Returns:
            SQL type string that best represents the Python value type.
        """
        # Handle null/missing values
        if value is None:
            return "NULL"

        # Handle boolean values (must check before int since bool is subclass of int)
        elif isinstance(value, bool):
            return "BOOLEAN"

        # Handle integer values (whole numbers)
        elif isinstance(value, int):
            return "INTEGER"

        # Handle floating point values (decimal numbers)
        elif isinstance(value, float):
            return "DOUBLE"

        # Handle string values (text data)
        elif isinstance(value, str):
            return "VARCHAR"

        # Default fallback for complex types (lists, dicts, objects, etc.)
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
