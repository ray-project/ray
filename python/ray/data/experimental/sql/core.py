"""Core SQL engine implementation for Ray Data.

This module provides the main RaySQL engine class that orchestrates SQL query
parsing, optimization, and execution against Ray Datasets.

SQLGlot: SQL parser, transpiler, and optimizer
https://github.com/tobymao/sqlglot
"""

import hashlib
import logging
import threading
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
from ray.data.experimental.sql.parser import SQLParser
from ray.data.experimental.sql.registry.base import TableRegistry
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RaySQL:
    """Main SQL engine for executing SQL queries against Ray Datasets.

    The RaySQL engine provides a complete SQL interface for Ray Data, supporting
    SELECT queries, JOINs, aggregations, and other SQL operations. It uses
    SQLGlot for parsing and validation, and can optionally use Apache DataFusion
    for advanced query optimization.

    Examples:
        >>> from ray.data.experimental.sql import RaySQL
        >>> engine = RaySQL()
        >>> engine.register_table("users", user_dataset)
        >>> result = engine.sql("SELECT * FROM users WHERE age > 25")
    """

    def __init__(self, config: Optional[SQLConfig] = None):
        """Initialize the SQL engine.

        Args:
            config: SQL configuration. Uses defaults if not provided.
        """
        self.config = config or SQLConfig()
        self.registry = TableRegistry()
        self.execution_engine = QueryExecutor(self.registry, self.config)
        self.parser = SQLParser(self.config)
        self._logger = logging.getLogger(__name__)
        self._setup_logging()
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
        """Register a Ray Dataset as a SQL table."""
        self.registry.register(name, dataset)

    def unregister_table(self, name: str) -> None:
        """Unregister a table by name."""
        self.registry.unregister(name)

    def sql(self, query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
        """Execute a SQL query against registered Ray Datasets.

        Args:
            query: SQL query string to execute.
            default_dataset: Optional default dataset for queries without FROM clause.

        Returns:
            Ray Dataset containing query results.

        Raises:
            SQLParseError: If query parsing fails.
            SQLExecutionError: If query execution fails.
            UnsupportedOperationError: If query contains unsupported operations.
        """
        self._validate_query(query, default_dataset)
        start_time = time.time()
        query_preview = query.strip()[:100]
        self._logger.info(
            f"Executing SQL query: {query_preview}"
            f"{'...' if len(query.strip()) > 100 else ''}"
        )
        try:
            result = self._execute_query(query, default_dataset)
            self._logger.info(
                f"Query executed in {time.time() - start_time:.3f}s"
            )
            return result
        except (SQLParseError, SQLExecutionError, UnsupportedOperationError):
            raise
        except ValueError as e:
            raise SQLExecutionError(
                f"Validation error: {str(e)}", query=query
            ) from e
        except Exception as e:
            raise SQLExecutionError(
                f"Unexpected error during query execution: {str(e)}",
                query=query,
            ) from e

    def _validate_query(
        self, query: str, default_dataset: Optional[Dataset]
    ) -> None:
        """Validate query string and default dataset."""
        if not isinstance(query, str):
            raise SQLParseError("Query must be a string", query=query)
        if not query.strip():
            raise SQLParseError("Query must be a non-empty string", query=query)
        if len(query) > 1_000_000:
            raise SQLParseError(
                "Query string too long (max 1MB)", query=query[:100]
            )
        if default_dataset is not None and not isinstance(
            default_dataset, Dataset
        ):
            raise SQLExecutionError(
                "default_dataset must be a Ray Dataset", query=query
            )

    def _execute_query(
        self, query: str, default_dataset: Optional[Dataset]
    ) -> Dataset:
        """Execute query with optional DataFusion optimization."""
        if self._should_use_datafusion():
            result = self._execute_with_datafusion(query)
            if result is not None:
                return result
        ast = self._parse_query(query)
        cte_tables = []
        if hasattr(ast, "with_") and ast.with_:
            cte_tables = self._execute_ctes(ast.with_)
        try:
            if isinstance(ast, exp.Select):
                return self.execution_engine.execute(ast)
            elif isinstance(ast, exp.Union):
                return self._execute_union(ast, default_dataset)
            else:
                raise UnsupportedOperationError(
                    f"{type(ast).__name__} statements",
                    suggestion="Only SELECT and UNION statements are currently supported",
                    query=query,
                )
        finally:
            self._cleanup_ctes(cte_tables)

    def _parse_query(self, query: str) -> exp.Expression:
        """Parse query and return AST, using cache if available."""
        ast = self._get_cached_query(query)
        if ast is None:
            ast = self.parser.parse(query)
            self._cache_query(query, ast)
        else:
            self._logger.debug("Using cached query AST")
        return ast

    def _cleanup_ctes(self, cte_tables: List[str]) -> None:
        """Clean up registered CTE tables."""
        for cte_name in cte_tables:
            try:
                self.unregister_table(cte_name)
            except Exception:
                pass

    def _should_use_datafusion(self) -> bool:
        """Check if DataFusion should be used for optimization."""
        from ray.data import DataContext

        ctx = DataContext.get_current()
        return ctx.sql_use_datafusion and self._is_datafusion_available()

    def _is_datafusion_available(self) -> bool:
        """Check if DataFusion is available."""
        try:
            from ray.data.experimental.sql.engines.datafusion.datafusion_optimizer import (
                is_datafusion_available,
            )

            return is_datafusion_available()
        except ImportError:
            return False

    def _execute_with_datafusion(self, query: str) -> Optional[Dataset]:
        """Execute query using DataFusion optimization.

        Apache DataFusion: https://datafusion.apache.org/

        Returns:
            Dataset if DataFusion optimization succeeds, None otherwise.
        """
        from ray.data.experimental.sql.engines.datafusion.datafusion_optimizer import (
            get_datafusion_optimizer,
        )

        optimizer = get_datafusion_optimizer()
        if not optimizer or not optimizer.is_available():
            return None

        registered_datasets = {
            name: self.registry.get(name)
            for name in self.registry.list_tables()
        }
        optimizations = optimizer.optimize_query(query, registered_datasets)

        if not optimizations:
            return None

        ast = sqlglot.parse_one(query)
        if not isinstance(ast, exp.Select):
            return None

        return execute_with_datafusion_hints(
            ast, optimizations, self.registry, self.config
        )

    def list_tables(self) -> List[str]:
        """List all registered table names."""
        return self.registry.list_tables()

    def get_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Get the schema for a registered table."""
        return self.registry.get_schema(table_name)

    def clear_tables(self) -> None:
        """Clear all registered tables."""
        self.registry.clear()

    def get_supported_features(self) -> Set[str]:
        """Get the set of supported SQL features."""
        return {"SELECT statements", "FROM clause", "WHERE clause", "JOIN operations (INNER, LEFT, RIGHT, FULL)",
                "GROUP BY with aggregation", "ORDER BY", "LIMIT clause", "Aggregate functions (COUNT, SUM, AVG, MIN, MAX)",
                "String functions", "Mathematical functions", "Date/time functions"}

    def get_unsupported_features(self) -> Set[str]:
        """Get the set of unsupported SQL features."""
        return {"DISTINCT", "Window functions", "INTERSECT", "EXCEPT", "INSERT statements", "UPDATE statements",
                "DELETE statements", "CREATE statements", "DROP statements"}

    def _execute_union(self, ast: exp.Union, default_dataset: Optional[Dataset] = None) -> Dataset:
        """Execute a UNION operation."""
        if ast.args.get("distinct", True):
            raise UnsupportedOperationError("UNION with DISTINCT", suggestion="Use UNION ALL instead, or apply deduplication manually with Ray Dataset operations")
        left_result = self.execution_engine.execute(ast.left)
        right_result = self.execution_engine.execute(ast.right)
        if left_result is None or right_result is None:
            raise SQLExecutionError("UNION operands must return valid datasets")
        return left_result.union(right_result)

    def _execute_ctes(self, with_clause: Any) -> List[str]:
        """Execute Common Table Expressions (WITH clauses). Returns list of registered CTE names."""
        if callable(with_clause):
            with_clause = with_clause() or None
        if not with_clause or not hasattr(with_clause, "expressions") or not with_clause.expressions:
            return []
        cte_names = []
        seen_aliases = set()
        for cte in with_clause.expressions:
            if not isinstance(cte, exp.CTE):
                continue
            cte_alias = str(cte.alias)
            if cte_alias in seen_aliases:
                raise SQLExecutionError(f"Duplicate CTE alias: {cte_alias}")
            seen_aliases.add(cte_alias)
            cte_query = cte.this
            if not isinstance(cte_query, (exp.Select, exp.Union)):
                raise UnsupportedOperationError(f"CTE with {type(cte_query).__name__} statement", suggestion="CTEs only support SELECT and UNION statements")
            cte_result = (self.execution_engine.execute(cte_query) if isinstance(cte_query, exp.Select) else self._execute_union(cte_query))
            if cte_result is None:
                raise SQLExecutionError(f"CTE '{cte_alias}' returned None result")
            self.register_table(cte_alias, cte_result)
            cte_names.append(cte_alias)
        return cte_names

    def _get_cache_key(self, query: str) -> str:
        """Generate cache key for query."""
        return hashlib.md5(" ".join(query.strip().split()).encode()).hexdigest()

    def _get_cached_query(self, query: str) -> Optional[exp.Expression]:
        """Get cached parsed query AST if available."""
        return self._query_cache.get(self._get_cache_key(query))

    def _cache_query(self, query: str, ast: exp.Expression) -> None:
        """Cache a parsed and validated query AST."""
        if len(self._query_cache) >= 100:
            self._query_cache.clear()
        cache_key = self._get_cache_key(query)
        self._query_cache[cache_key] = ast

    def clear_query_cache(self) -> None:
        """Clear the query plan cache."""
        self._query_cache.clear()


# Global engine instance
_global_engine: Optional[RaySQL] = None
_global_engine_lock = threading.Lock()

def get_engine() -> RaySQL:
    """Get the global SQL engine instance."""
    global _global_engine
    if _global_engine is None:
        with _global_engine_lock:
            if _global_engine is None:
                config = get_global_config()
                _global_engine = RaySQL(config)
    return _global_engine

def get_registry() -> TableRegistry:
    """Get the global table registry."""
    return get_engine().registry

def get_global_config() -> SQLConfig:
    """Get the SQL configuration from DataContext.

    Returns:
        SQLConfig instance with current settings from DataContext.
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import LogLevel, SQLDialect

    ctx = DataContext.get_current()
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
        config: SQL configuration to apply.
    """
    from ray.data import DataContext

    ctx = DataContext.get_current()
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
    global _global_engine
    with _global_engine_lock:
        _global_engine = None

def configure(**kwargs: Any) -> None:
    """Configure SQL settings via DataContext.

    Args:
        **kwargs: Configuration options to set. Supported keys:
            - dialect: SQL dialect (duckdb, postgres, mysql, etc.)
            - log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            - case_sensitive: Whether identifiers are case-sensitive
            - strict_mode: Whether to enforce strict validation
            - enable_optimization: Whether to enable optimizations
            - max_join_partitions: Maximum partitions for joins
            - enable_predicate_pushdown: Enable predicate pushdown
            - enable_projection_pushdown: Enable projection pushdown
            - query_timeout_seconds: Query timeout in seconds
            - enable_sqlglot_optimizer: Enable SQLGlot optimizer
            - sql_use_datafusion: Use DataFusion optimizer

    Raises:
        ValueError: If invalid configuration option or value provided.
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import LogLevel, SQLDialect

    ctx = DataContext.get_current()
    config_map = {
        "dialect": lambda v: setattr(
            ctx, "sql_dialect", SQLDialect(v.lower()).value
        ),
        "log_level": lambda v: setattr(
            ctx, "sql_log_level", LogLevel(v.upper()).value
        ),
        "case_sensitive": lambda v: setattr(ctx, "sql_case_sensitive", bool(v)),
        "strict_mode": lambda v: setattr(ctx, "sql_strict_mode", bool(v)),
        "enable_optimization": lambda v: setattr(
            ctx, "sql_enable_optimization", bool(v)
        ),
        "max_join_partitions": lambda v: setattr(
            ctx, "sql_max_join_partitions", int(v)
        )
        if v > 0
        else None,
        "enable_predicate_pushdown": lambda v: setattr(
            ctx, "sql_enable_predicate_pushdown", bool(v)
        ),
        "enable_projection_pushdown": lambda v: setattr(
            ctx, "sql_enable_projection_pushdown", bool(v)
        ),
        "query_timeout_seconds": lambda v: setattr(
            ctx,
            "sql_query_timeout_seconds",
            v if v is None or v > 0 else None,
        ),
        "enable_sqlglot_optimizer": lambda v: setattr(
            ctx, "sql_enable_sqlglot_optimizer", bool(v)
        ),
        "sql_use_datafusion": lambda v: setattr(
            ctx, "sql_use_datafusion", bool(v)
        ),
    }

    for key, value in kwargs.items():
        if key not in config_map:
            raise ValueError(f"Unknown SQL configuration option: {key}")
        try:
            config_map[key](value)
        except (ValueError, AttributeError):
            if key == "dialect":
                raise ValueError(
                    f"Invalid dialect '{value}'. "
                    f"Supported: {[d.value for d in SQLDialect]}"
                )
            elif key == "log_level":
                raise ValueError(
                    f"Invalid log_level '{value}'. "
                    f"Supported: {[l.value for l in LogLevel]}"
                )
            elif key in ("max_join_partitions", "query_timeout_seconds"):
                if value is not None and value <= 0:
                    raise ValueError(f"{key} must be positive")
            raise

    global _global_engine
    with _global_engine_lock:
        _global_engine = None


def get_dialect() -> str:
    """Get the current SQL dialect from DataContext.

    Returns:
        Current SQL dialect string.
    """
    from ray.data import DataContext

    return DataContext.get_current().sql_dialect


def set_dialect(dialect: str) -> None:
    """Set the SQL dialect for parsing and validation.

    Args:
        dialect: SQL dialect name (duckdb, postgres, mysql, etc.).

    Raises:
        ValueError: If dialect is not supported.
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import SQLDialect

    try:
        SQLDialect(dialect.lower())
    except ValueError:
        raise ValueError(
            f"Invalid dialect '{dialect}'. "
            f"Supported: {[d.value for d in SQLDialect]}"
        )
    DataContext.get_current().sql_dialect = dialect.lower()


def get_log_level() -> str:
    """Get the current SQL logging level from DataContext.

    Returns:
        Current log level string.
    """
    from ray.data import DataContext

    return DataContext.get_current().sql_log_level


def set_log_level(level: str) -> None:
    """Set the logging level for SQL operations.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).

    Raises:
        ValueError: If log level is not supported.
    """
    from ray.data import DataContext
    from ray.data.experimental.sql.config import LogLevel

    try:
        LogLevel(level.upper())
    except ValueError:
        raise ValueError(
            f"Invalid log_level '{level}'. "
            f"Supported: {[l.value for l in LogLevel]}"
        )
    DataContext.get_current().sql_log_level = level.upper()


def enable_optimization(enable: bool = True) -> None:
    """Enable or disable query optimization.

    Args:
        enable: Whether to enable optimization.
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_optimization = enable


def set_join_partitions(max_partitions: int) -> None:
    """Set the maximum number of partitions for join operations.

    Args:
        max_partitions: Maximum number of partitions (must be positive).

    Raises:
        ValueError: If max_partitions is not positive.
    """
    if max_partitions <= 0:
        raise ValueError("max_partitions must be positive")
    from ray.data import DataContext

    DataContext.get_current().sql_max_join_partitions = max_partitions


def enable_predicate_pushdown(enable: bool = True) -> None:
    """Enable or disable predicate pushdown optimization.

    Args:
        enable: Whether to enable predicate pushdown.
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_predicate_pushdown = enable


def enable_projection_pushdown(enable: bool = True) -> None:
    """Enable or disable projection pushdown optimization.

    Args:
        enable: Whether to enable projection pushdown.
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_projection_pushdown = enable


def set_query_timeout(seconds: int) -> None:
    """Set the query timeout in seconds.

    Args:
        seconds: Timeout in seconds (must be positive).

    Raises:
        ValueError: If seconds is not positive.
    """
    if seconds <= 0:
        raise ValueError("timeout must be positive")
    from ray.data import DataContext

    DataContext.get_current().sql_query_timeout_seconds = seconds


def enable_sqlglot_optimizer(enable: bool = True) -> None:
    """Enable or disable SQLGlot query optimization.

    SQLGlot optimizer: https://github.com/tobymao/sqlglot

    Args:
        enable: Whether to enable SQLGlot optimizer.
    """
    from ray.data import DataContext

    DataContext.get_current().sql_enable_sqlglot_optimizer = enable


def reset_config() -> None:
    """Reset all SQL configuration to default values."""
    from ray.data import DataContext
    ctx = DataContext.get_current()
    ctx.sql_dialect, ctx.sql_log_level = "duckdb", "INFO"
    ctx.sql_case_sensitive, ctx.sql_strict_mode = True, False
    ctx.sql_enable_optimization, ctx.sql_max_join_partitions = True, 20
    ctx.sql_enable_predicate_pushdown, ctx.sql_enable_projection_pushdown = True, True
    ctx.sql_query_timeout_seconds, ctx.sql_enable_sqlglot_optimizer = None, False
    ctx.sql_use_datafusion = True
    global _global_engine
    with _global_engine_lock:
        _global_engine = None


def get_config_summary() -> Dict[str, Any]:
    """Get a summary of current SQL configuration."""
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

    This function automatically discovers datasets from local variables, global
    variables, or keyword arguments, making it convenient to use without explicit
    table registration.

    Args:
        query: SQL query string to execute.
        **datasets: Optional keyword arguments mapping table names to datasets.

    Returns:
        Ray Dataset containing query results.

    Examples:
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> result = sql("SELECT * FROM users WHERE id > 0")
    """
    import inspect
    from ray.data.experimental.sql.utils import extract_table_names_from_query

    engine = get_engine()
    current_frame = inspect.currentframe()
    if current_frame is None or current_frame.f_back is None:
        return engine.sql(query, **datasets)

    caller_frame = current_frame.f_back
    caller_locals = caller_frame.f_locals
    caller_globals = caller_frame.f_globals
    table_names = extract_table_names_from_query(query)
    auto_registered = []

    def register_if_dataset(name: str, ds: Any) -> None:
        """Register dataset if it's a Ray Dataset."""
        if isinstance(ds, Dataset):
            engine.register_table(name, ds)
            auto_registered.append(name)

    for table_name in table_names:
        if table_name in engine.registry.list_tables():
            continue
        for source in [datasets, caller_locals, caller_globals]:
            if table_name in source:
                register_if_dataset(table_name, source[table_name])
                break

    try:
        return engine.sql(query)
    finally:
        for table_name in auto_registered:
            engine.unregister_table(table_name)


@PublicAPI(stability="alpha")
def register_table(name: str, dataset: Dataset) -> None:
    """Register a Dataset as a SQL table."""
    get_engine().register_table(name, dataset)


@PublicAPI(stability="alpha")
def list_tables() -> List[str]:
    """List all registered table names."""
    return get_engine().list_tables()


@PublicAPI(stability="alpha")
def get_schema(table_name: str) -> Optional[Dict[str, str]]:
    """Get the schema for a registered table."""
    return get_engine().get_schema(table_name)


@PublicAPI(stability="alpha")
def clear_tables() -> None:
    """Clear all registered tables."""
    get_engine().clear_tables()


def get_supported_features() -> Dict[str, List[str]]:
    """Get information about supported SQL features."""
    engine = get_engine()
    return {
        "supported": list(engine.get_supported_features()),
        "unsupported": list(engine.get_unsupported_features()),
    }
