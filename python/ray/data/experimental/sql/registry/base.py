"""Base table registry for Ray Data SQL API.

This module provides the TableRegistry class for registering, retrieving,
and managing Ray Datasets as SQL tables.
"""

import logging
import threading
from typing import Dict, List, Optional

from ray.data import Dataset
from ray.data.experimental.sql.exceptions import TableNotFoundError


class TableRegistry:
    """Registry for managing SQL table name to Ray Dataset mappings."""

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
        self._lock = threading.Lock()

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a dataset under the given SQL table name."""
        if not isinstance(dataset, Dataset):
            raise TypeError(f"Expected Ray Dataset, got {type(dataset)}")
        if not name or not isinstance(name, str):
            raise ValueError("Table name must be a non-empty string")
        if name.lower() in self.SQL_RESERVED_WORDS:
            raise ValueError(f"Table name '{name}' is a SQL reserved word")
        if not name.replace("_", "").isalnum():
            raise ValueError("Table name can only contain alphanumeric characters and underscores")
        if len(name) > 64:
            raise ValueError("Table name cannot exceed 64 characters")
        with self._lock:
            if name in self._tables:
                self._logger.warning(f"Overwriting existing table '{name}'")
            self._tables[name] = dataset
        self._logger.debug(f"Registered table '{name}'")

    def get(self, name: str) -> Dataset:
        """Retrieve a registered dataset by table name."""
        if not isinstance(name, str):
            raise ValueError(f"Table name must be a string, got {type(name)}")
        with self._lock:
            if name not in self._tables:
                raise TableNotFoundError(name, available_tables=list(self._tables.keys()))
            dataset = self._tables[name]
        if dataset is None:
            raise ValueError(f"Table '{name}' has None dataset")
        return dataset

    def list_tables(self) -> List[str]:
        """List all registered table names.

        Returns:
            List of registered table names.
        """
        with self._lock:
            return list(self._tables.keys())

    def clear(self) -> None:
        """Clear all registered tables."""
        with self._lock:
            self._tables.clear()
        self._logger.debug("Cleared all registered tables")

    def get_schema(self, name: str) -> Optional[Dict[str, str]]:
        """Get the schema for a registered table."""
        dataset = self.get(name)
        if dataset is None:
            return None
        try:
            if hasattr(dataset, "schema"):
                schema = dataset.schema()
                if schema:
                    return {str(col): str(schema[col]) for col in schema}
            first_row = dataset.take(1)
            if first_row and len(first_row) > 0:
                return {str(col): type(val).__name__ for col, val in first_row[0].items()}
            return None
        except Exception as e:
            self._logger.warning(f"Could not extract schema for table '{name}': {e}")
            return None

    def unregister(self, name: str) -> None:
        """Unregister a table by name."""
        if not isinstance(name, str):
            raise ValueError(f"Table name must be a string, got {type(name)}")
        with self._lock:
            if name in self._tables:
                del self._tables[name]
                self._logger.debug(f"Unregistered table '{name}'")
            else:
                self._logger.debug(f"Table '{name}' not found for unregistration")
