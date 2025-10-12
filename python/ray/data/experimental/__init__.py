"""
Experimental features for Ray Data.

This module contains experimental Ray Data features that are still under active
development and may change in future releases. Use with caution in production.

Features:
- SQL API: Query Ray Datasets using SQL syntax
"""

import warnings

# Import SQL API components with experimental warning
try:
    from ray.data.experimental.sql import (
        SQLError,
        SQLExecutionError,
        SQLParseError,
        TableNotFoundError,
    )
    from ray.data.experimental.sql_api import (
        clear_tables,
        config as sql_config,
        list_tables,
        register,
        sql,
    )

    SQL_AVAILABLE = True

    # Emit experimental warning when SQL module is imported
    warnings.warn(
        "Ray Data SQL API is experimental and may change in future releases. "
        "Use with caution in production environments.",
        FutureWarning,
        stacklevel=2,
    )
except ImportError:
    SQL_AVAILABLE = False

__all__ = [
    "sql",
    "register",
    "clear_tables",
    "list_tables",
    "sql_config",
    "SQLError",
    "SQLExecutionError",
    "SQLParseError",
    "TableNotFoundError",
    "SQL_AVAILABLE",
]
