"""
Clean SQL API for Ray Data - Native Dataset Operations.

**EXPERIMENTAL**: This API is experimental and may change in future releases.

This provides a clean SQL interface that converts SQL directly into
Ray Dataset native operations for maximum performance and compatibility.
"""

import inspect
import warnings
from typing import List

from ray.data import Dataset
from ray.data.experimental.sql.core import get_engine
from ray.data.experimental.sql.utils import extract_table_names_from_query
from ray.util.annotations import PublicAPI


def _emit_experimental_warning():
    """Emit warning about experimental SQL API."""
    warnings.warn(
        "Ray Data SQL API is experimental and may change in future releases. "
        "Use with caution in production environments.",
        FutureWarning,
        stacklevel=3,
    )


@PublicAPI(stability="alpha")
def sql(query: str, **datasets) -> Dataset:
    """Execute SQL using Ray Dataset native operations. Variables auto-discovered.

    **EXPERIMENTAL**: This API is experimental and may change in future releases.

    This function converts SQL queries directly into Ray Dataset native operations
    like filter(expr=...), join(), groupby().aggregate(), sort(), and limit()
    for maximum performance and compatibility.

    Args:
        query: SQL query string.
        **datasets: Optional explicit dataset mappings.

    Returns:
        Dataset with query results using Ray Dataset native operations.

    Examples:
        Basic filtering (uses dataset.filter(expr=...) with native Arrow optimization):
            >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
            >>> result = ray.data.sql("SELECT * FROM users WHERE id > 0")
            >>> # Internally: users.filter(expr="id > 0")

        Joins (uses dataset.join() with native distributed join):
            >>> orders = ray.data.from_items([{"user_id": 1, "amount": 100}])
            >>> result = ray.data.sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
            >>> # Internally: users.join(orders, on="id", right_on="user_id")

        Aggregation (uses dataset.groupby().aggregate() with native aggregates):
            >>> result = ray.data.sql("SELECT COUNT(*) as count FROM users")
            >>> # Internally: users.aggregate(Count())

        Complex queries (chains native operations):
            >>> result = ray.data.sql('''
            ...     SELECT city, AVG(age) as avg_age
            ...     FROM users
            ...     WHERE age BETWEEN 25 AND 65
            ...     GROUP BY city
            ...     ORDER BY avg_age DESC
            ...     LIMIT 10
            ... ''')
            >>> # Internally: users.filter(expr="(age >= 25) and (age <= 65)")
            >>> #              .groupby("city").aggregate(Mean("age"))
            >>> #              .sort("avg_age", descending=True).limit(10)
    """
    _emit_experimental_warning()

    # Get caller's variables (optimized)
    current_frame = inspect.currentframe()
    if current_frame is None or current_frame.f_back is None:
        # No frame available, skip auto-discovery
        return get_engine().sql(query, **datasets)

    frame = current_frame.f_back
    caller_locals = frame.f_locals
    caller_globals = frame.f_globals

    # Extract table names from query
    try:
        table_names = extract_table_names_from_query(query)
    except Exception:
        # Fallback if parsing fails
        return get_engine().sql(query)

    # Auto-register datasets
    engine = get_engine()
    registered = []

    for table in table_names:
        # Skip if already registered
        if table in engine.list_tables():
            continue

        # Use explicit dataset if provided
        if table in datasets and isinstance(datasets[table], Dataset):
            engine.register_table(table, datasets[table])
            registered.append(table)
            continue

        # Auto-discover from caller's variables (check locals first, then globals)
        if table in caller_locals and isinstance(caller_locals[table], Dataset):
            engine.register_table(table, caller_locals[table])
            registered.append(table)
        elif table in caller_globals and isinstance(caller_globals[table], Dataset):
            engine.register_table(table, caller_globals[table])
            registered.append(table)

    # Execute query and cleanup
    try:
        return engine.sql(query)
    finally:
        # Clean up auto-registered tables
        for table in registered:
            try:
                engine.unregister_table(table)
            except Exception:
                pass


@PublicAPI(stability="alpha")
def register(name: str, dataset: Dataset) -> None:
    """Register a dataset as a SQL table (optional - auto-discovery usually works).

    **EXPERIMENTAL**: This API is experimental and may change in future releases.

    Args:
        name: Table name for SQL queries.
        dataset: Ray Dataset to register.

    Examples:
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ray.data.register("users", users)
        >>> result = ray.data.sql("SELECT * FROM users")
    """
    _emit_experimental_warning()
    get_engine().register_table(name, dataset)


@PublicAPI(stability="alpha")
def clear_tables() -> None:
    """Clear all registered tables.

    **EXPERIMENTAL**: This API is experimental and may change in future releases.
    """
    get_engine().clear_tables()


@PublicAPI(stability="alpha")
def list_tables() -> List[str]:
    """List all registered tables.

    **EXPERIMENTAL**: This API is experimental and may change in future releases.
    """
    return get_engine().list_tables()


# Simple configuration proxy that delegates to DataContext
class _Config:
    """Configuration proxy for SQL API that delegates to DataContext.

    All SQL configuration is stored in ray.data.DataContext for consistency
    with Ray Data patterns. This class provides a convenient interface.

    All properties map to DataContext.sql_* fields.
    """

    @property
    def dialect(self) -> str:
        """SQL dialect for query parsing.

        Default: 'duckdb'
        Supported: duckdb, postgres, mysql, sqlite, snowflake, bigquery, etc.
        Maps to: DataContext.sql_dialect
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_dialect

    @dialect.setter
    def dialect(self, value: str) -> None:
        from ray.data import DataContext
        from ray.data.experimental.sql.config import SQLDialect

        # Validate
        try:
            SQLDialect(value.lower())
        except ValueError:
            raise ValueError(
                f"Invalid dialect '{value}'. Supported: {[d.value for d in SQLDialect]}"
            )

        DataContext.get_current().sql_dialect = value.lower()

    @property
    def log_level(self) -> str:
        """SQL logging level.

        Default: 'INFO'
        Supported: DEBUG, INFO, WARNING, ERROR
        Maps to: DataContext.sql_log_level
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_log_level

    @log_level.setter
    def log_level(self, value: str) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_log_level = value.upper()

    @property
    def case_sensitive(self) -> bool:
        """Whether SQL identifiers are case-sensitive.

        Default: True
        Maps to: DataContext.sql_case_sensitive
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_case_sensitive

    @case_sensitive.setter
    def case_sensitive(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_case_sensitive = bool(value)

    @property
    def strict_mode(self) -> bool:
        """Whether to enable strict SQL validation mode.

        Default: False
        Maps to: DataContext.sql_strict_mode
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_strict_mode

    @strict_mode.setter
    def strict_mode(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_strict_mode = bool(value)

    @property
    def enable_optimization(self) -> bool:
        """Whether to enable SQL query optimization.

        Default: True
        Maps to: DataContext.sql_enable_optimization
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_enable_optimization

    @enable_optimization.setter
    def enable_optimization(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_enable_optimization = bool(value)

    @property
    def max_join_partitions(self) -> int:
        """Maximum partitions for SQL join operations.

        Default: 20
        Maps to: DataContext.sql_max_join_partitions
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_max_join_partitions

    @max_join_partitions.setter
    def max_join_partitions(self, value: int) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_max_join_partitions = int(value)

    @property
    def enable_predicate_pushdown(self) -> bool:
        """Whether to enable predicate pushdown optimization.

        Default: True
        Maps to: DataContext.sql_enable_predicate_pushdown
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_enable_predicate_pushdown

    @enable_predicate_pushdown.setter
    def enable_predicate_pushdown(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_enable_predicate_pushdown = bool(value)

    @property
    def enable_projection_pushdown(self) -> bool:
        """Whether to enable projection pushdown optimization.

        Default: True
        Maps to: DataContext.sql_enable_projection_pushdown
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_enable_projection_pushdown

    @enable_projection_pushdown.setter
    def enable_projection_pushdown(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_enable_projection_pushdown = bool(value)

    @property
    def query_timeout_seconds(self):
        """SQL query timeout in seconds.

        Default: None (no timeout)
        Maps to: DataContext.sql_query_timeout_seconds
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_query_timeout_seconds

    @query_timeout_seconds.setter
    def query_timeout_seconds(self, value) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_query_timeout_seconds = value

    @property
    def enable_sqlglot_optimizer(self) -> bool:
        """Whether to enable experimental SQLGlot AST optimization.

        Default: False
        Maps to: DataContext.sql_enable_sqlglot_optimizer
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_enable_sqlglot_optimizer

    @enable_sqlglot_optimizer.setter
    def enable_sqlglot_optimizer(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_enable_sqlglot_optimizer = bool(value)

    @property
    def use_datafusion(self) -> bool:
        """Whether to use Apache DataFusion for query optimization.

        Default: True
        When True, uses DataFusion's cost-based optimizer then executes with
        Ray Data. Falls back to SQLGlot if DataFusion unavailable.
        Maps to: DataContext.sql_use_datafusion
        """
        from ray.data import DataContext

        return DataContext.get_current().sql_use_datafusion

    @use_datafusion.setter
    def use_datafusion(self, value: bool) -> None:
        from ray.data import DataContext

        DataContext.get_current().sql_use_datafusion = bool(value)


config = _Config()
