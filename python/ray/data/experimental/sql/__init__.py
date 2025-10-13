"""Ray Data SQL API - SQL interface for Ray Datasets.

This module is internal to Ray Data's experimental SQL implementation.
Users should access the SQL API through ray.data.experimental.sql_api instead.

For public API, use:
    from ray.data.experimental.sql_api import sql, register, list_tables, clear_tables, config
"""

# Internal imports for SQL module implementation
from ray.data.experimental.sql.config import LogLevel, SQLConfig, SQLDialect
from ray.data.experimental.sql.core import (
    RaySQL,
    clear_tables,
    configure,
    enable_optimization,
    enable_predicate_pushdown,
    enable_projection_pushdown,
    enable_sqlglot_optimizer,
    get_config_summary,
    get_dialect,
    get_engine,
    get_global_config,
    get_log_level,
    get_registry,
    get_schema,
    list_tables,
    register_table,
    reset_config,
    set_dialect,
    set_global_config,
    set_join_partitions,
    set_log_level,
    set_query_timeout,
    sql,
)
from ray.data.experimental.sql.exceptions import (
    ColumnNotFoundError,
    SchemaError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
)

# Internal exports (for use within ray.data.experimental.sql package)
__all__ = [
    # Core internal components
    "RaySQL",
    "SQLConfig",
    "SQLDialect",
    "LogLevel",
    # Internal functions
    "sql",
    "register_table",
    "list_tables",
    "get_schema",
    "clear_tables",
    "get_engine",
    "get_registry",
    "configure",
    "get_global_config",
    "set_global_config",
    "get_dialect",
    "set_dialect",
    "get_log_level",
    "set_log_level",
    "enable_optimization",
    "set_join_partitions",
    "enable_predicate_pushdown",
    "enable_projection_pushdown",
    "set_query_timeout",
    "enable_sqlglot_optimizer",
    "reset_config",
    "get_config_summary",
    # Exception classes
    "SQLError",
    "SQLParseError",
    "SQLExecutionError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "SchemaError",
    "UnsupportedOperationError",
]
