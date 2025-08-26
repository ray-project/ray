"""Base table registry for Ray Data SQL API."""

import logging
from typing import Dict, List, Optional

from ray.data import Dataset
from ray.data.sql.exceptions import TableNotFoundError


class TableRegistry:
    """Registry for managing SQL table name to Ray Dataset mappings.

    This class provides a centralized registry for mapping SQL table names
    to Ray Dataset objects. It supports table registration, lookup, and
    management operations.

    Examples:
        >>> registry = TableRegistry()
        >>> users_ds = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> registry.register("users", users_ds)
        >>> dataset = registry.get("users")
    """

    # SQL reserved words that cannot be used as table names
    SQL_RESERVED_WORDS = {
        "select",
        "from",
        "where",
        "order",
        "by",
        "group",
        "having",
        "join",
        "inner",
        "left",
        "right",
        "full",
        "on",
        "and",
        "or",
        "not",
        "null",
        "true",
        "false",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "distinct",
        "union",
        "intersect",
        "except",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "in",
        "between",
        "like",
        "is",
        "exists",
        "all",
        "any",
        "some",
        "cast",
        "coalesce",
        "if",
        "ifnull",
        "isnull",
        "nullif",
        "greatest",
        "least",
        "upper",
        "lower",
        "length",
        "trim",
        "substring",
        "concat",
        "date",
        "time",
        "timestamp",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "index",
        "primary",
        "foreign",
        "key",
        "constraint",
        "check",
        "default",
        "auto_increment",
        "unique",
        "table",
        "database",
        "schema",
        "view",
        "procedure",
        "function",
        "trigger",
        "event",
        "user",
        "grant",
        "revoke",
        "commit",
        "rollback",
        "savepoint",
        "transaction",
        "lock",
        "unlock",
        "analyze",
        "explain",
        "describe",
        "show",
        "use",
        "set",
        "declare",
        "begin",
        "loop",
        "while",
        "repeat",
        "until",
        "iterate",
        "leave",
        "signal",
        "resignal",
        "get",
        "diagnostics",
    }

    def __init__(self):
        """Initialize a new table registry."""
        self._tables: Dict[str, Dataset] = {}
        self._logger = logging.getLogger(__name__)

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a dataset under the given SQL table name.

        Args:
            name: SQL table name to register the dataset under.
            dataset: Ray Dataset to register.

        Raises:
            TypeError: If dataset is not a Ray Dataset.
            ValueError: If name is invalid or reserved.
        """
        if not isinstance(dataset, Dataset):
            raise TypeError(f"Expected Ray Dataset, got {type(dataset)}")

        if not name or not isinstance(name, str):
            raise ValueError("Table name must be a non-empty string")

        # Check for SQL reserved words
        if name.lower() in self.SQL_RESERVED_WORDS:
            raise ValueError(f"Table name '{name}' is a SQL reserved word")

        # Check for invalid characters
        if not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                "Table name can only contain alphanumeric characters, underscores, and hyphens"
            )

        # Check for length limits
        if len(name) > 64:
            raise ValueError("Table name cannot exceed 64 characters")

        self._tables[name] = dataset
        self._logger.debug(f"Registered table '{name}' with {dataset.count()} rows")

    def get(self, name: str) -> Dataset:
        """Retrieve a registered dataset by table name.

        Args:
            name: SQL table name to look up.

        Returns:
            The Ray Dataset registered under the given name.

        Raises:
            TableNotFoundError: If the table name is not found.
        """
        if name not in self._tables:
            available = list(self._tables.keys())
            raise TableNotFoundError(name, available_tables=available)

        return self._tables[name]

    def list_tables(self) -> List[str]:
        """List all registered table names.

        Returns:
            List of registered table names.
        """
        return list(self._tables.keys())

    def clear(self) -> None:
        """Clear all registered tables."""
        self._tables.clear()
        self._logger.debug("Cleared all registered tables")

    def get_schema(self, name: str) -> Optional[Dict[str, str]]:
        """Get the schema for a registered table.

        Args:
            name: Table name to get schema for.

        Returns:
            Dictionary mapping column names to types, or None if not available.

        Raises:
            TableNotFoundError: If the table name is not found.
        """
        dataset = self.get(name)

        try:
            # Try to get schema from dataset
            if hasattr(dataset, "schema"):
                schema = dataset.schema()
                if schema:
                    return {str(col): str(schema[col]) for col in schema}

            # Fallback: infer schema from first row
            first_row = dataset.take(1)
            if first_row:
                row = first_row[0]
                return {str(col): type(val).__name__ for col, val in row.items()}

            return None
        except Exception as e:
            self._logger.warning(f"Could not extract schema for table '{name}': {e}")
            return None

    def exists(self, name: str) -> bool:
        """Check if a table exists in the registry.

        Args:
            name: Table name to check.

        Returns:
            True if the table exists, False otherwise.
        """
        return name in self._tables

    def remove(self, name: str) -> bool:
        """Remove a table from the registry.

        Args:
            name: Table name to remove.

        Returns:
            True if the table was removed, False if it didn't exist.
        """
        if name in self._tables:
            del self._tables[name]
            self._logger.debug(f"Removed table '{name}' from registry")
            return True
        return False

    def get_table_info(self, name: str) -> Optional[Dict[str, any]]:
        """Get comprehensive information about a table.

        Args:
            name: Table name to get info for.

        Returns:
            Dictionary with table information, or None if not found.
        """
        if name not in self._tables:
            return None

        dataset = self._tables[name]
        info = {
            "name": name,
            "type": "ray_dataset",
            "dataset_id": str(id(dataset)),
        }

        try:
            # Get row count
            count = dataset.count()
            info["row_count"] = count

            # Get schema
            schema = self.get_schema(name)
            if schema:
                info["schema"] = schema
                info["column_count"] = len(schema)

            # Get size estimate if available
            if hasattr(dataset, "size_bytes"):
                try:
                    size_bytes = dataset.size_bytes()
                    info["size_bytes"] = size_bytes
                except:
                    pass

        except Exception as e:
            self._logger.warning(f"Could not get complete info for table '{name}': {e}")

        return info

    def get_all_table_info(self) -> Dict[str, Dict[str, any]]:
        """Get information about all registered tables.

        Returns:
            Dictionary mapping table names to their information.
        """
        return {name: self.get_table_info(name) for name in self._tables}

    def validate_table_name(self, name: str) -> bool:
        """Validate if a table name is acceptable.

        Args:
            name: Table name to validate.

        Returns:
            True if the name is valid, False otherwise.
        """
        if not name or not isinstance(name, str):
            return False

        if name.lower() in self.SQL_RESERVED_WORDS:
            return False

        if not name.replace("_", "").replace("-", "").isalnum():
            return False

        if len(name) > 64:
            return False

        return True
