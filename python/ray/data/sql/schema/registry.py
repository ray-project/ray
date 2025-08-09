"""
Dataset registry for Ray Data SQL API.

This module provides the DatasetRegistry class for managing named Ray Datasets
as SQL tables with automatic schema inference.
"""

from typing import Any, Dict, List, Optional

from ray.data import Dataset
from ray.data.sql.schema.manager import SchemaManager
from ray.data.sql.utils import setup_logger, validate_table_name


class DatasetRegistry:
    """Registry for managing named Ray Datasets as SQL tables.

    The DatasetRegistry maintains a mapping between table names and Ray
    Datasets, allowing SQL queries to reference datasets by name. It acts
    as a catalog that bridges SQL table names to Ray Dataset objects.

    This registry also manages schema information through its SchemaManager,
    automatically inferring column types and metadata when datasets are registered.

    Examples:
        .. testcode::

            registry = DatasetRegistry()
            registry.register("users", user_dataset)
            dataset = registry.get("users")
    """

    def __init__(self):
        """Initialize an empty dataset registry.

        Creates internal storage for table mappings and sets up logging
        and schema management components.
        """
        # Internal mapping from table names to Ray Dataset objects
        self._tables: Dict[str, Dataset] = {}

        # Logger for debugging dataset registration/lookup operations
        self._logger = setup_logger("DatasetRegistry")

        # Schema manager handles automatic type inference and validation
        self.schema_manager = SchemaManager()

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a Ray Dataset under a given table name.

        This method makes a Ray Dataset available for SQL queries by associating
        it with a table name. It also triggers automatic schema inference and
        adds metadata to the dataset for future reference.

        Args:
            name: Table name to register the dataset under (must be valid SQL identifier).
            dataset: Ray Dataset to register (must be an actual Ray Dataset object).

        Raises:
            TypeError: If dataset is not a Ray Dataset.
            ValueError: If table name is invalid (contains special characters, etc.).
        """
        # Validate that the input is actually a Ray Dataset
        if not isinstance(dataset, Dataset):
            raise TypeError(f"Expected Dataset, got {type(dataset)}")

        # Validate table name follows SQL identifier rules
        validate_table_name(name)

        # Store the dataset in our internal registry
        self._tables[name] = dataset

        # Add SQL metadata to the dataset for introspection
        dataset._sql_name = name

        # Automatically infer and store schema information
        self.schema_manager.infer_schema_from_dataset(name, dataset)

        # Log successful registration with row count for debugging
        self._logger.debug(f"Registered dataset '{name}' with {dataset.count()} rows")

    def unregister(self, name: str) -> None:
        """Remove a dataset from the registry by name.

        Args:
            name: Table name to unregister.
        """
        if name in self._tables:
            del self._tables[name]
            self._logger.debug(f"Unregistered dataset '{name}'")

    def get(self, name: str) -> Dataset:
        """Retrieve a registered dataset by name.

        Args:
            name: Table name to retrieve.

        Returns:
            Registered Ray Dataset.

        Raises:
            ValueError: If table is not found.
        """
        if name not in self._tables:
            available = list(self._tables.keys())
            raise ValueError(f"Table '{name}' not found. Available tables: {available}")
        return self._tables[name]

    def list_tables(self) -> List[str]:
        """List all registered table names.

        Returns:
            List of registered table names.
        """
        return list(self._tables.keys())

    def clear(self) -> None:
        """Remove all registered tables."""
        self._tables.clear()
        self.schema_manager.clear_schemas()
        self._logger.debug("Cleared all registered tables")

    def auto_register_from_frame(self, frame_locals: Dict[str, Any]) -> int:
        """Automatically register all Ray Datasets found in the given frame's locals.

        Args:
            frame_locals: Dictionary of local variables from a frame.

        Returns:
            Number of datasets registered.
        """
        count = 0
        for name, obj in frame_locals.items():
            if isinstance(obj, Dataset) and name not in self._tables:
                try:
                    self.register(name, obj)
                    count += 1
                except ValueError:
                    # Skip invalid table names
                    self._logger.debug(
                        f"Skipped registering '{name}' due to invalid table name"
                    )
        return count

    def get_default_table(self) -> Optional[Dataset]:
        """Get the default table if only one is registered.

        Returns:
            The single registered dataset, or None if multiple or no datasets.
        """
        if len(self._tables) == 1:
            return next(iter(self._tables.values()))
        return None

    def has_table(self, name: str) -> bool:
        """Check if a table is registered.

        Args:
            name: Table name to check.

        Returns:
            True if table exists, False otherwise.
        """
        return name in self._tables

    def get_table_info(self, name: str) -> Dict[str, Any]:
        """Get information about a registered table.

        Args:
            name: Table name to get info for.

        Returns:
            Dictionary with table information.

        Raises:
            ValueError: If table is not found.
        """
        if name not in self._tables:
            raise ValueError(f"Table '{name}' not found")

        dataset = self._tables[name]
        schema = self.schema_manager.get_schema(name)

        return {
            "name": name,
            "row_count": dataset.count(),
            "columns": list(dataset.columns()) if hasattr(dataset, "columns") else [],
            "schema": schema,
        }

    def rename_table(self, old_name: str, new_name: str) -> None:
        """Rename a registered table.

        Args:
            old_name: Current table name.
            new_name: New table name.

        Raises:
            ValueError: If old table doesn't exist or new name is invalid.
        """
        if old_name not in self._tables:
            raise ValueError(f"Table '{old_name}' not found")

        validate_table_name(new_name)

        if new_name in self._tables:
            raise ValueError(f"Table '{new_name}' already exists")

        dataset = self._tables[old_name]
        del self._tables[old_name]

        self._tables[new_name] = dataset
        dataset._sql_name = new_name

        # Update schema
        schema = self.schema_manager.get_schema(old_name)
        if schema:
            schema.name = new_name
            self.schema_manager.register_schema(new_name, schema)

        self._logger.debug(f"Renamed table '{old_name}' to '{new_name}'")
