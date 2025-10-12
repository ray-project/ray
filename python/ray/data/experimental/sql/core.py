"""Core SQL engine implementation for Ray Data.

This module provides SQL query execution for Ray Datasets using standard SQL syntax.
"""

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

from ray.data import Dataset
from ray.data.experimental.sql.config import LogLevel, SQLConfig
from ray.data.experimental.sql.engines.datafusion.datafusion_executor import (
    execute_with_datafusion_hints,
)
from ray.data.experimental.sql.exceptions import (
    SQLExecutionError,
    SQLParseError,
    UnsupportedOperationError,
)
from ray.data.experimental.sql.execution.executor import QueryExecutor
from ray.data.experimental.sql.registry.base import TableRegistry
from ray.data.experimental.sql.validators.base import CompositeValidator
from ray.data.experimental.sql.validators.features import FeatureValidator
from ray.data.experimental.sql.validators.syntax import SyntaxValidator
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RaySQL:
    """Main SQL engine for Ray Data.

    The RaySQL class provides the primary interface for executing SQL queries
    against Ray Datasets. It manages table registration, query parsing,
    optimization, and execution.

    Examples:
        Basic usage:
            >>> engine = RaySQL()
            >>> engine.register_table("my_table", my_dataset)
            >>> result = engine.sql("SELECT * FROM my_table")

        With configuration:
            >>> config = SQLConfig(log_level=LogLevel.DEBUG)
            >>> engine = RaySQL(config)

    Args:
        config: SQL engine configuration. Uses default if not provided.
    """

    def __init__(self, config: Optional[SQLConfig] = None):
        """Initialize the SQL engine.

        Args:
            config: SQL engine configuration. Uses default if not provided.
        """
        self.config = config or SQLConfig()
        self.registry = TableRegistry()
        self.execution_engine = QueryExecutor(self.registry, self.config)
        self._logger = logging.getLogger(__name__)
        self._setup_logging()

        # Create composite validator
        self.validator = CompositeValidator(
            [
                SyntaxValidator(),
                FeatureValidator(),
            ]
        )

        # Simple query cache for performance
        self._query_cache: Dict[str, exp.Expression] = {}

    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        log_level_mapping = {
            LogLevel.ERROR: logging.ERROR,
            LogLevel.INFO: logging.INFO,
            LogLevel.DEBUG: logging.DEBUG,
        }
        self._logger.setLevel(log_level_mapping[self.config.log_level])

    def register_table(self, name: str, dataset: Dataset) -> None:
        """Register a Ray Dataset as a SQL table.

        Args:
            name: SQL table name.
            dataset: Ray Dataset to register.
        """
        self.registry.register(name, dataset)
        # Avoid expensive count() operation during registration

    def unregister_table(self, name: str) -> None:
        """Unregister a table by name.

        Args:
            name: Table name to unregister.
        """
        self.registry.unregister(name)

    def sql(self, query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
        """Execute a SQL query with optional DataFusion optimization.

        Args:
            query: SQL query string.
            default_dataset: Default dataset for queries without FROM clause.

        Returns:
            Ray Dataset containing the query results.

        Raises:
            SQLParseError: If the query cannot be parsed.
            UnsupportedOperationError: If the query uses unsupported features.
            SQLExecutionError: If query execution fails.
            ValidationError: If query validation fails.
            ConfigurationError: If configuration is invalid.
        """
        # Simple input validation
        if not isinstance(query, str) or not query.strip():
            raise SQLParseError("Query must be a non-empty string", query=query)

        if default_dataset is not None and not isinstance(default_dataset, Dataset):
            raise SQLExecutionError(
                "default_dataset must be a Ray Dataset", query=query
            )

        start_time = time.time()
        self._logger.info(
            f"Executing SQL query: {query.strip()[:100]}{'...' if len(query.strip()) > 100 else ''}"
        )

        try:
            # Try DataFusion optimization if enabled
            if self._should_use_datafusion():
                try:
                    result = self._execute_with_datafusion(query)
                    if result is not None:
                        execution_time = time.time() - start_time
                        self._logger.info(
                            f"Query executed with DataFusion optimization in {execution_time:.3f}s"
                        )
                        return result
                except Exception as e:
                    self._logger.info(
                        f"DataFusion optimization failed ({e}), falling back to SQLGlot"
                    )
                    # Fall through to SQLGlot execution

            # Standard SQLGlot execution path
            # Check query cache first for performance
            ast = self._get_cached_query(query)
            if ast is None:
                # Parse the SQL query
                ast = sqlglot.parse_one(query)
                if not ast:
                    raise SQLParseError("Query could not be parsed", query=query)

                # Validate the query
                self.validator.validate(query, ast)

                # Cache the parsed and validated query
                self._cache_query(query, ast)
            else:
                self._logger.debug("Using cached query AST")

            # Handle WITH clauses (CTEs) before main query execution
            if hasattr(ast, "with_") and ast.with_ is not None:
                self._execute_ctes(ast.with_)

            # Execute the query using the unified executor
            if isinstance(ast, exp.Select):
                result = self.execution_engine.execute(ast)
            elif isinstance(ast, exp.Union):
                result = self._execute_union(ast, default_dataset)
            else:
                raise UnsupportedOperationError(
                    f"{type(ast).__name__} statements",
                    suggestion="Only SELECT and UNION statements are currently supported",
                    query=query,
                )

            execution_time = time.time() - start_time
            self._logger.info(f"Query executed in {execution_time:.3f}s")
            return result

        except (SQLParseError, SQLExecutionError, UnsupportedOperationError):
            # Re-raise known SQL errors without wrapping
            raise
        except ValueError as e:
            # Convert validation errors to SQL execution errors
            raise SQLExecutionError(f"Validation error: {str(e)}", query=query) from e
        except Exception as e:
            # Wrap unexpected errors
            raise SQLExecutionError(
                f"Unexpected error during query execution: {str(e)}",
                query=query,
            ) from e

    def _should_use_datafusion(self) -> bool:
        """Check if DataFusion should be used for optimization.

        Returns:
            True if DataFusion is enabled in config and available.
        """
        try:
            from ray.data import DataContext

            ctx = DataContext.get_current()
            return ctx.sql_use_datafusion and self._is_datafusion_available()
        except Exception:
            return False

    def _is_datafusion_available(self) -> bool:
        """Check if DataFusion is available.

        Returns:
            True if DataFusion can be imported and used.
        """
        try:
            from ray.data.experimental.sql.engines.datafusion.datafusion_optimizer import (
                is_datafusion_available,
            )

            return is_datafusion_available()
        except ImportError:
            return False

    def _execute_with_datafusion(self, query: str) -> Optional[Dataset]:
        """
        Execute query using DataFusion optimization + Ray Data execution.

        Uses DataFusion for query optimization and planning, then executes
        with Ray Data for distributed execution with resource and backpressure
        management.

        Args:
            query: SQL query string.

        Returns:
            Dataset if DataFusion optimization succeeds, None to fallback to SQLGlot.
        """
        try:
            from ray.data.experimental.sql.engines.datafusion.datafusion_optimizer import (
                get_datafusion_optimizer,
            )

            # Get DataFusion optimizer
            optimizer = get_datafusion_optimizer()
            if optimizer is None or not optimizer.is_available():
                return None

            # Get registered datasets for DataFusion
            registered_datasets = {}
            for table_name in self.registry.list_tables():
                registered_datasets[table_name] = self.registry.get(table_name)

            # Optimize query with DataFusion
            optimizations = optimizer.optimize_query(query, registered_datasets)

            if optimizations is None:
                self._logger.info(
                    "DataFusion optimization returned None, using fallback"
                )
                return None

            # Step 2: Parse SQL with SQLGlot for compatibility
            ast = sqlglot.parse_one(query)
            if not ast or not isinstance(ast, exp.Select):
                return None

            # Step 3: Execute with Ray Data applying DataFusion hints
            # REUSES existing QueryExecutor (no code duplication!)
            # DataFusion hints guide execution order and placement
            # This preserves Ray Data's:
            # - Distributed execution across cluster
            # - Resource management (CPU/GPU/memory budgets)
            # - Backpressure control (3 policies)
            # - Streaming execution model
            result = execute_with_datafusion_hints(
                ast, optimizations, self.registry, self.config
            )

            if result is not None:
                return result
            else:
                return None

        except Exception:
            return None

    def list_tables(self) -> List[str]:
        """List all registered table names.

        Returns:
            List of registered table names.
        """
        return self.registry.list_tables()

    def get_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Get the schema for a registered table.

        Args:
            table_name: Name of the table.

        Returns:
            Dictionary mapping column names to types, or None if not available.
        """
        return self.registry.get_schema(table_name)

    def clear_tables(self) -> None:
        """Clear all registered tables."""
        self.registry.clear()
        """Get the set of supported SQL features.

        Returns:
            Set of supported feature names.
        """
        return self.validator.get_supported_features()

    def get_unsupported_features(self) -> Set[str]:
        """Get the set of unsupported SQL features.

        Returns:
            Set of unsupported feature names.
        """
        return self.validator.get_unsupported_features()

    def _execute_union(
        self, ast: exp.Union, default_dataset: Optional[Dataset] = None
    ) -> Dataset:
        """Execute a UNION operation.

        Args:
            ast: UNION AST node.
            default_dataset: Default dataset for queries without FROM clause.

        Returns:
            Dataset containing the union of all SELECT results.
        """
        # Execute left side
        left_result = self.execution_engine.execute(ast.left)

        # Execute right side
        right_result = self.execution_engine.execute(ast.right)

        # Use Ray Dataset union operation
        result = left_result.union(right_result)

        # Handle DISTINCT vs ALL
        if not ast.args.get("distinct", True):  # UNION ALL
            return result
        else:  # UNION (with implicit DISTINCT)
            # DISTINCT is not supported in Ray Dataset API yet
            raise UnsupportedOperationError(
                "UNION with DISTINCT",
                suggestion="Use UNION ALL instead, or apply deduplication manually with Ray Dataset operations",
            )

    def _execute_ctes(self, with_clause: Any) -> None:
        """Execute Common Table Expressions (WITH clauses).

        CTEs are intermediate datasets that get registered as temporary tables.

        Args:
            with_clause: The WITH clause containing CTE definitions.
        """
        # Handle the case where with_ might be a method or property
        if callable(with_clause):
            # If it's a method, call it to get the actual WITH clause
            try:
                actual_with = with_clause()
                if actual_with:
                    with_clause = actual_with
                else:
                    return  # No CTEs to process
            except Exception:
                return  # Can't access CTEs, skip

        # Process CTEs from the WITH clause
        if hasattr(with_clause, "expressions") and with_clause.expressions:
            for cte in with_clause.expressions:
                if not isinstance(cte, exp.CTE):
                    continue

                # Get the CTE name and query
                cte_name = str(cte.alias)
                cte_query = cte.this

                # Execute the CTE query to get a dataset
                if isinstance(cte_query, exp.Select):
                    cte_result = self.execution_engine.execute(cte_query)
                elif isinstance(cte_query, exp.Union):
                    cte_result = self._execute_union(cte_query)
                else:
                    raise UnsupportedOperationError(
                        f"CTE with {type(cte_query).__name__} statement",
                        suggestion="CTEs only support SELECT and UNION statements",
                    )

                # Register the CTE result as a temporary table
                self.register_table(cte_name, cte_result)

    def _get_cache_key(self, query: str) -> str:
        """Generate a cache key for the query."""
        # Normalize whitespace and create hash
        normalized = " ".join(query.strip().split())
        return hashlib.md5(normalized.encode()).hexdigest()

    def _get_cached_query(self, query: str) -> Optional[exp.Expression]:
        """Get cached parsed query AST if available."""
        cache_key = self._get_cache_key(query)
        return self._query_cache.get(cache_key)

    def _cache_query(self, query: str, ast: exp.Expression) -> None:
        """Cache a parsed and validated query AST."""
        cache_key = self._get_cache_key(query)

        # Simple cache with size limit
        if len(self._query_cache) >= 100:  # Keep cache reasonable
            self._query_cache.clear()  # Simple eviction

        self._query_cache[cache_key] = ast

    def clear_query_cache(self) -> None:
        """Clear the query plan cache."""
        self._query_cache.clear()


# Global engine instance
_global_engine: Optional[RaySQL] = None


def get_engine() -> RaySQL:
    """Get the global SQL engine instance.

    Returns:
        Global RaySQL engine instance configured from DataContext.
    """
    global _global_engine
    if _global_engine is None:
        # Build configuration from DataContext
        config = get_global_config()
        _global_engine = RaySQL(config)
    return _global_engine


def get_registry() -> TableRegistry:
    """Get the global table registry.

    Returns:
        Global table registry instance.
    """
    return get_engine().registry


def get_global_config() -> SQLConfig:
    """Get the SQL configuration from DataContext.

    Returns:
        SQL configuration built from DataContext settings.
    """
    from ray.data import DataContext

    ctx = DataContext.get_current()

    # Build SQLConfig from DataContext SQL settings
    from ray.data.experimental.sql.config import LogLevel, SQLDialect

    try:
        dialect = SQLDialect(ctx.sql_dialect.lower())
    except (ValueError, AttributeError):
        dialect = SQLDialect.DUCKDB

    try:
        log_level = LogLevel(ctx.sql_log_level.upper())
    except (ValueError, AttributeError):
        log_level = LogLevel.INFO

    return SQLConfig(
        log_level=log_level,
        dialect=dialect,
        case_sensitive=ctx.sql_case_sensitive,
        strict_mode=ctx.sql_strict_mode,
        enable_optimization=ctx.sql_enable_optimization,
        max_join_partitions=ctx.sql_max_join_partitions,
        enable_predicate_pushdown=ctx.sql_enable_predicate_pushdown,
        enable_projection_pushdown=ctx.sql_enable_projection_pushdown,
        query_timeout_seconds=ctx.sql_query_timeout_seconds,
        enable_sqlglot_optimizer=ctx.sql_enable_sqlglot_optimizer,
    )


def set_global_config(config: SQLConfig) -> None:
    """Set SQL configuration via DataContext.

    Args:
        config: SQL configuration to apply to DataContext.
    """
    from ray.data import DataContext

    ctx = DataContext.get_current()

    # Update DataContext with SQLConfig values
    ctx.sql_dialect = config.dialect.value
    ctx.sql_log_level = config.log_level.value
    ctx.sql_case_sensitive = config.case_sensitive
    ctx.sql_strict_mode = config.strict_mode
    ctx.sql_enable_optimization = config.enable_optimization
    ctx.sql_max_join_partitions = config.max_join_partitions
    ctx.sql_enable_predicate_pushdown = config.enable_predicate_pushdown
    ctx.sql_enable_projection_pushdown = config.enable_projection_pushdown
    ctx.sql_query_timeout_seconds = config.query_timeout_seconds
    ctx.sql_enable_sqlglot_optimizer = config.enable_sqlglot_optimizer

    # Reset global engine to pick up new config
    global _global_engine
    _global_engine = None


def configure(**kwargs: Any) -> None:
    """Configure SQL settings via DataContext with keyword arguments.

    This is a convenience function for setting SQL configuration options.
    All settings are stored in ray.data.DataContext for consistency with
    Ray Data patterns.

    Args:
        **kwargs: Configuration options to set (prefixed with sql_ in DataContext).
            - dialect: SQL dialect (str)
            - log_level: Logging level (str)
            - case_sensitive: Whether names are case-sensitive (bool)
            - strict_mode: Whether to enable strict mode (bool)
            - enable_optimization: Whether to enable optimization (bool)
            - max_join_partitions: Maximum join partitions (int)
            - enable_predicate_pushdown: Whether to enable predicate pushdown (bool)
            - enable_projection_pushdown: Whether to enable projection pushdown (bool)
            - query_timeout_seconds: Query timeout in seconds (int)
            - enable_sqlglot_optimizer: Whether to enable SQLGlot optimization (bool)

    Examples:
        Set dialect:
            >>> import ray.data
            >>> ray.data.sql.configure(dialect="postgres")

        Set multiple options:
            >>> ray.data.sql.configure(
            ...     dialect="mysql",
            ...     strict_mode=True,
            ...     log_level="debug",
            ...     enable_sqlglot_optimizer=True
            ... )

        Direct DataContext access (alternative):
            >>> ctx = ray.data.DataContext.get_current()
            >>> ctx.sql_dialect = "postgres"
            >>> ctx.sql_enable_sqlglot_optimizer = True
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import LogLevel, SQLDialect

    ctx = DataContext.get_current()

    for key, value in kwargs.items():
        if key == "dialect":
            if isinstance(value, str):
                try:
                    SQLDialect(value.lower())  # Validate
                    ctx.sql_dialect = value.lower()
                except ValueError:
                    raise ValueError(
                        f"Invalid dialect '{value}'. Supported: {[d.value for d in SQLDialect]}"
                    )
        elif key == "log_level":
            if isinstance(value, str):
                try:
                    LogLevel(value.upper())  # Validate
                    ctx.sql_log_level = value.upper()
                except ValueError:
                    raise ValueError(
                        f"Invalid log_level '{value}'. Supported: {[l.value for l in LogLevel]}"
                    )
        elif key == "case_sensitive":
            ctx.sql_case_sensitive = bool(value)
        elif key == "strict_mode":
            ctx.sql_strict_mode = bool(value)
        elif key == "enable_optimization":
            ctx.sql_enable_optimization = bool(value)
        elif key == "max_join_partitions":
            if value <= 0:
                raise ValueError("max_join_partitions must be positive")
            ctx.sql_max_join_partitions = int(value)
        elif key == "enable_predicate_pushdown":
            ctx.sql_enable_predicate_pushdown = bool(value)
        elif key == "enable_projection_pushdown":
            ctx.sql_enable_projection_pushdown = bool(value)
        elif key == "query_timeout_seconds":
            if value is not None and value <= 0:
                raise ValueError("query_timeout_seconds must be positive")
            ctx.sql_query_timeout_seconds = value
        elif key == "enable_sqlglot_optimizer":
            ctx.sql_enable_sqlglot_optimizer = bool(value)
        elif key == "sql_use_datafusion":
            ctx.sql_use_datafusion = bool(value)
        else:
            raise ValueError(f"Unknown SQL configuration option: {key}")

    # Reset global engine to pick up new config
    global _global_engine
    _global_engine = None


def get_dialect() -> str:
    """Get the current SQL dialect from DataContext.

    Returns:
        Current SQL dialect as string.
    """
    from ray.data import DataContext

    return DataContext.get_current().sql_dialect


def set_dialect(dialect: str) -> None:
    """Set the SQL dialect for parsing and validation via DataContext.

    Args:
        dialect: SQL dialect to use. Supported: duckdb, postgres, mysql, sqlite, spark, bigquery, snowflake, redshift

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_dialect = "postgres"
        >>> # Or use convenience function:
        >>> ray.data.sql.set_dialect("mysql")
    """
    from ray.data import DataContext

    # Validate dialect
    from ray.data.experimental.sql.config import SQLDialect

    try:
        SQLDialect(dialect.lower())  # Validate it's a valid dialect
    except ValueError:
        raise ValueError(
            f"Invalid dialect '{dialect}'. Supported: {[d.value for d in SQLDialect]}"
        )

    DataContext.get_current().sql_dialect = dialect.lower()


def get_log_level() -> str:
    """Get the current SQL logging level from DataContext.

    Returns:
        Current logging level as string.
    """
    from ray.data import DataContext

    return DataContext.get_current().sql_log_level


def set_log_level(level: str) -> None:
    """Set the logging level for SQL operations via DataContext.

    Args:
        level: Logging level. Supported: debug, info, warning, error

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_log_level = "DEBUG"
        >>> # Or use convenience function:
        >>> ray.data.sql.set_log_level("debug")
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import LogLevel

    # Validate log level
    try:
        LogLevel(level.upper())  # Validate it's a valid level
    except ValueError:
        raise ValueError(
            f"Invalid log_level '{level}'. Supported: {[l.value for l in LogLevel]}"
        )

    DataContext.get_current().sql_log_level = level.upper()


def enable_optimization(enable: bool = True) -> None:
    """Enable or disable query optimization via DataContext.

    Args:
        enable: Whether to enable optimization.

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_enable_optimization = True
        >>> # Or use convenience function:
        >>> ray.data.sql.enable_optimization(True)
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_optimization = enable


def set_join_partitions(max_partitions: int) -> None:
    """Set the maximum number of partitions for join operations via DataContext.

    Args:
        max_partitions: Maximum number of partitions.

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_max_join_partitions = 50
        >>> # Or use convenience function:
        >>> ray.data.sql.set_join_partitions(50)
    """
    if max_partitions <= 0:
        raise ValueError("max_partitions must be positive")
    from ray.data import DataContext

    DataContext.get_current().sql_max_join_partitions = max_partitions


def enable_predicate_pushdown(enable: bool = True) -> None:
    """Enable or disable predicate pushdown optimization via DataContext.

    Args:
        enable: Whether to enable predicate pushdown.

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_enable_predicate_pushdown = True
        >>> # Or use convenience function:
        >>> ray.data.sql.enable_predicate_pushdown(True)
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_predicate_pushdown = enable


def enable_projection_pushdown(enable: bool = True) -> None:
    """Enable or disable projection pushdown optimization via DataContext.

    Args:
        enable: Whether to enable projection pushdown.

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_enable_projection_pushdown = True
        >>> # Or use convenience function:
        >>> ray.data.sql.enable_projection_pushdown(True)
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_projection_pushdown = enable


def set_query_timeout(seconds: int) -> None:
    """Set the query timeout in seconds via DataContext.

    Args:
        seconds: Timeout in seconds.

    Examples:
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_query_timeout_seconds = 300
        >>> # Or use convenience function:
        >>> ray.data.sql.set_query_timeout(300)  # 5 minutes
    """
    if seconds <= 0:
        raise ValueError("timeout must be positive")
    from ray.data import DataContext

    DataContext.get_current().sql_query_timeout_seconds = seconds


def enable_sqlglot_optimizer(enable: bool = True) -> None:
    """Enable or disable SQLGlot query optimization via DataContext (experimental).

    When enabled, applies safe SQLGlot optimization rules to the parsed SQL AST:
    - normalize: Normalize SQL expressions
    - simplify: Simplify boolean and arithmetic expressions
    - pushdown_predicates: Push WHERE clauses closer to data sources
    - pushdown_projections: Push column selection earlier
    - eliminate_subqueries: Flatten unnecessary subqueries
    - merge_subqueries: Combine compatible subqueries

    These optimizations can improve query performance by:
    - Reducing data processed by filtering earlier
    - Minimizing columns read from storage
    - Simplifying complex expressions
    - Flattening query structure

    Note: This is experimental. Ray Data execution uses native operations
    regardless of optimization. Defaults to False for stability.

    Args:
        enable: Whether to enable SQLGlot optimization.

    Examples:
        >>> # Enable experimental query optimization via DataContext
        >>> import ray.data
        >>> ctx = ray.data.DataContext.get_current()
        >>> ctx.sql_enable_sqlglot_optimizer = True
        >>>
        >>> # Or use convenience function:
        >>> ray.data.sql.enable_sqlglot_optimizer(True)
        >>>
        >>> # Optimization will apply to subsequent queries
        >>> result = ray.data.sql("SELECT * FROM users WHERE age > 25 AND age < 65")
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_sqlglot_optimizer = enable


def reset_config() -> None:
    """Reset all SQL configuration to default values via DataContext.

    Examples:
        >>> import ray.data
        >>> ray.data.sql.reset_config()
    """
    from ray.data import DataContext

    ctx = DataContext.get_current()

    # Reset all SQL configuration to defaults
    ctx.sql_dialect = "duckdb"
    ctx.sql_log_level = "INFO"
    ctx.sql_case_sensitive = True
    ctx.sql_strict_mode = False
    ctx.sql_enable_optimization = True
    ctx.sql_max_join_partitions = 20
    ctx.sql_enable_predicate_pushdown = True
    ctx.sql_enable_projection_pushdown = True
    ctx.sql_query_timeout_seconds = None
    ctx.sql_enable_sqlglot_optimizer = False
    ctx.sql_use_datafusion = True  # Default to True

    # Reset global engine to pick up new config
    global _global_engine
    _global_engine = None


def get_config_summary() -> Dict[str, Any]:
    """Get a summary of current SQL configuration from DataContext.

    Returns:
        Dictionary with current configuration values.

    Examples:
        >>> import ray.data
        >>> config = ray.data.sql.get_config_summary()
        >>> print(f"Dialect: {config['dialect']}")
        >>> print(f"Log level: {config['log_level']}")
    """
    from ray.data import DataContext

    ctx = DataContext.get_current()

    return {
        "dialect": ctx.sql_dialect,
        "log_level": ctx.sql_log_level.lower(),
        "case_sensitive": ctx.sql_case_sensitive,
        "strict_mode": ctx.sql_strict_mode,
        "enable_optimization": ctx.sql_enable_optimization,
        "max_join_partitions": ctx.sql_max_join_partitions,
        "enable_predicate_pushdown": ctx.sql_enable_predicate_pushdown,
        "enable_projection_pushdown": ctx.sql_enable_projection_pushdown,
        "query_timeout_seconds": ctx.sql_query_timeout_seconds,
        "enable_sqlglot_optimizer": ctx.sql_enable_sqlglot_optimizer,
        "use_datafusion": ctx.sql_use_datafusion,
    }


# Public API functions
@PublicAPI(stability="alpha")
def sql(query: str, **datasets) -> Dataset:
    """Execute a SQL query on Ray Datasets with automatic variable discovery.

    This function provides a simple, Pythonic SQL interface similar to DuckDB.
    Ray Datasets can be referenced directly in SQL queries by their variable names,
    or passed explicitly as keyword arguments.

    Args:
        query: SQL query string.
        **datasets: Optional explicit dataset mappings (name=dataset).

    Returns:
        Dataset containing the query results.

    Examples:
        Automatic dataset discovery (DuckDB-style):
            >>> import ray.data
            >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
            >>> orders = ray.data.from_items([{"id": 1, "user_id": 1, "amount": 100}])
            >>> # Datasets automatically available by variable name
            >>> result = ray.data.sql("SELECT * FROM users WHERE id = 1")
            >>> result = ray.data.sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")

        Explicit dataset passing:
            >>> result = ray.data.sql("SELECT * FROM my_table", my_table=users)

        Mixed usage:
            >>> ds = ray.data.from_items([{"x": 1}, {"x": 2}])
            >>> result = ray.data.sql("SELECT * FROM ds WHERE x > 1")
    """
    import inspect

    from ray.data.experimental.sql.utils import extract_table_names_from_query

    # Get the caller's frame to access their local variables
    caller_frame = inspect.currentframe().f_back
    caller_locals = caller_frame.f_locals
    caller_globals = caller_frame.f_globals

    # Extract table names from the SQL query
    try:
        table_names = extract_table_names_from_query(query)
    except Exception:
        # If we can't parse table names, fall back to current behavior
        engine = get_engine()
        return engine.sql(query)

    # Auto-register datasets from caller's namespace
    engine = get_engine()
    auto_registered = []

    for table_name in table_names:
        # Skip if already registered
        if table_name in engine.registry.list_tables():
            continue

        # Look for dataset in explicit kwargs first
        if table_name in datasets:
            dataset = datasets[table_name]
            if isinstance(dataset, Dataset):
                engine.register_table(table_name, dataset)
                auto_registered.append(table_name)
            continue

        # Look for dataset in caller's local variables
        if table_name in caller_locals:
            var = caller_locals[table_name]
            if isinstance(var, Dataset):
                engine.register_table(table_name, var)
                auto_registered.append(table_name)
                continue

        # Look for dataset in caller's global variables
        if table_name in caller_globals:
            var = caller_globals[table_name]
            if isinstance(var, Dataset):
                engine.register_table(table_name, var)
                auto_registered.append(table_name)

    try:
        # Execute the query
        result = engine.sql(query)
        return result
    finally:
        # Clean up auto-registered tables to avoid namespace pollution
        for table_name in auto_registered:
            try:
                engine.unregister_table(table_name)
            except Exception:
                pass  # Ignore cleanup errors


@PublicAPI(stability="alpha")
def register_table(name: str, dataset: Dataset) -> None:
    """Register a Dataset as a SQL table.

    Args:
        name: Table name for SQL queries.
        dataset: Ray Dataset to register.

    Examples:
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ray.data.sql.register_table("users", users)
        >>> result = ray.data.sql("SELECT * FROM users")
    """
    get_engine().register_table(name, dataset)


@PublicAPI(stability="alpha")
def list_tables() -> List[str]:
    """List all registered table names.

    Returns:
        List of registered table names.
    """
    return get_engine().list_tables()


@PublicAPI(stability="alpha")
def get_schema(table_name: str) -> Optional[Dict[str, str]]:
    """Get the schema for a registered table.

    Args:
        table_name: Name of the table.

    Returns:
        Dictionary mapping column names to types, or None if not available.
    """
    return get_engine().get_schema(table_name)


@PublicAPI(stability="alpha")
def clear_tables() -> None:
    """Clear all registered tables."""
    get_engine().clear_tables()


def get_supported_features() -> Dict[str, List[str]]:
    """Get information about supported SQL features.

    Returns:
        Dictionary mapping feature categories to lists of supported features.
    """
    engine = get_engine()
    return {
        "supported": list(engine.get_supported_features()),
        "unsupported": list(engine.get_unsupported_features()),
    }
