"""Ray Data SQL API - Public interface.

This module provides the public SQL API for Ray Data, allowing users to
execute SQL queries on Ray Datasets using familiar SQL syntax.

**EXPERIMENTAL**: This API is experimental and may change in future releases.

Examples:
    Basic query execution:
        >>> import ray.data
        >>> from ray.data.sql import sql, register_table
        >>>
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> register_table("users", users)
        >>> result = sql("SELECT * FROM users WHERE id = 1")

    Auto-discovery (no registration needed):
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> result = ray.data.sql("SELECT * FROM users WHERE id = 1")
"""

# Re-export all public SQL API functions and classes from experimental module
from ray.data.experimental.sql import (
    ColumnNotFoundError,
    SchemaError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
)
from ray.data.experimental.sql.config import LogLevel, SQLConfig, SQLDialect
from ray.data.experimental.sql.core import get_schema
from ray.data.experimental.sql_api import (
    clear_tables,
    config,
    list_tables,
    register,
    sql,
)

# Aliases for compatibility
register_table = register  # Common alias

__all__ = [
    # Core functions
    "sql",
    "register",
    "register_table",
    "list_tables",
    "clear_tables",
    "get_schema",
    "config",
    # Configuration classes
    "SQLConfig",
    "SQLDialect",
    "LogLevel",
    # Exception classes
    "SQLError",
    "SQLParseError",
    "SQLExecutionError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "SchemaError",
    "UnsupportedOperationError",
]
