"""Ray Data SQL API - SQL interface for Ray Datasets.

Execute SQL queries on Ray Datasets using standard SQL syntax.
Results are returned as Ray Datasets for seamless integration.

Examples:
    Basic Usage:
        >>> import ray.data.sql
        >>>
        >>> # Create and register datasets
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ray.data.sql.register_table("users", users)
        >>>
        >>> # Execute SQL queries
        >>> result = ray.data.sql("SELECT * FROM users")
        >>> print(result.take_all())
"""

import sys
import types

from ray.data.sql.config import LogLevel, SQLConfig, SQLDialect
from ray.data.sql.core import (
    RaySQL,
    clear_tables,
    # Configuration functions
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
from ray.data.sql.exceptions import (
    ColumnNotFoundError,
    SchemaError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
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
    "SQLDialect",
    "LogLevel",
    # Configuration functions
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

# Module-level documentation
__version__ = "1.0.0"
__author__ = "Ray Data Team"


# Module-level properties for easy access
def _get_dialect():
    """Get the current SQL dialect."""
    from .core import get_dialect

    return get_dialect()


def _set_dialect(value):
    """Set the SQL dialect."""
    from .core import set_dialect

    set_dialect(value)


def _get_log_level():
    """Get the current logging level."""
    from .core import get_log_level

    return get_log_level()


def _set_log_level(value):
    """Set the logging level."""
    from .core import set_log_level

    set_log_level(value)


# Create a custom callable module class
class CallableSQLModule(types.ModuleType):
    """A callable module that forwards calls to the sql function."""

    def __call__(self, query, *args, **kwargs):
        """Make the module callable by forwarding to the sql function."""
        return sql(query, *args, **kwargs)

    @property
    def dialect(self):
        """Get the current SQL dialect."""
        return get_dialect()

    @dialect.setter
    def dialect(self, value):
        """Set the SQL dialect."""
        set_dialect(value)

    @property
    def log_level(self):
        """Get the current logging level."""
        return get_log_level()

    @log_level.setter
    def log_level(self, value):
        """Set the logging level."""
        set_log_level(value)


# Replace the current module with our callable version
current_module = sys.modules[__name__]
callable_module = CallableSQLModule(__name__, current_module.__doc__)

# Copy all attributes from the current module, but skip properties that we define ourselves
for attr_name in dir(current_module):
    if not attr_name.startswith("_") and attr_name not in ["dialect", "log_level"]:
        setattr(callable_module, attr_name, getattr(current_module, attr_name))

# Replace the module in sys.modules
sys.modules[__name__] = callable_module
