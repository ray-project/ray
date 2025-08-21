"""Ray Data SQL API - Production-ready SQL interface for Ray Datasets.

This module provides a comprehensive SQL interface for Ray Datasets, allowing you to
execute SQL queries against distributed data using Ray's parallel processing
capabilities. The engine supports a wide range of SQL operations including
SELECT, JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, and aggregate functions.

The implementation follows Ray Dataset API patterns for:
- Lazy evaluation of transformations
- Proper return types (Dataset objects)
- Method chaining and composition
- Error handling and validation
- Performance optimization

Examples:
    Basic Usage:
        >>> import ray.data
        >>> from ray.data.sql import register_table, sql
        >>>
        >>> # Create datasets
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        >>> orders = ray.data.from_items([{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}])
        >>>
        >>> # Register datasets as tables
        >>> register_table("users", users)
        >>> register_table("orders", orders)
        >>>
        >>> # Execute SQL queries
        >>> result = sql("SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name")
        >>> print(result.take_all())

    Advanced Usage with DataContext:
        >>> from ray.data import DataContext
        >>> ctx = DataContext.get_current()
        >>> ctx.sql_config = {
        ...     "log_level": "DEBUG",
        ...     "case_sensitive": False,
        ...     "max_join_partitions": 20,
        ...     "enable_optimization": True,
        ...     "strict_mode": True
        ... }
        >>> result = ray.data.sql("SELECT * FROM users WHERE id > 1")

    Method Chaining:
        >>> ds = ray.data.sql("SELECT * FROM users WHERE id > 1")
        >>> transformed = ds.map(lambda row: {"name_upper": row["name"].upper()})
        >>> filtered = transformed.filter(lambda row: len(row["name_upper"]) > 4)
        >>> print(filtered.take_all())
"""

from ray.data.sql.config import LogLevel, SQLConfig
from ray.data.sql.core import (
    RaySQL,
    clear_tables,
    get_engine,
    get_registry,
    get_schema,
    list_tables,
    register_table,
    sql,
)
from ray.data.sql.exceptions import (
    ColumnNotFoundError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
)

# Public API exports
__all__ = [
    # Core public API
    "sql",
    "register_table",
    "list_tables",
    "get_schema",
    "clear_tables",
    "get_engine",
    "get_registry",
    # Core classes for type hinting and advanced usage
    "RaySQL",
    "SQLConfig",
    "LogLevel",
    # Exception classes
    "SQLError",
    "SQLParseError",
    "SQLExecutionError",
    "TableNotFoundError",
    "ColumnNotFoundError",
]

# Module-level documentation
__version__ = "1.0.0"
__author__ = "Ray Data Team"
