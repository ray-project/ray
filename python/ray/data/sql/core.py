"""Streamlined core SQL engine implementation for Ray Data.

This module provides the main SQL engine classes and functions for executing
SQL queries against Ray Datasets. It follows Ray API patterns and provides
comprehensive error handling, performance optimizations, and ease-of-use features.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

from ray.data import Dataset
from ray.data.sql.config import LogLevel, SQLConfig
from ray.data.sql.exceptions import (
    SQLExecutionError,
    SQLParseError,
    UnsupportedOperationError,
)
from ray.data.sql.execution.engine import SQLExecutionEngine
from ray.data.sql.registry.base import TableRegistry
from ray.data.sql.validators.base import CompositeValidator
from ray.data.sql.validators.features import FeatureValidator
from ray.data.sql.validators.syntax import SyntaxValidator


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
    """

    def __init__(self, config: Optional[SQLConfig] = None):
        """Initialize the SQL engine.

        Args:
            config: SQL engine configuration. Uses default if not provided.
        """
        self.config = config or SQLConfig()
        self.registry = TableRegistry()
        self.execution_engine = SQLExecutionEngine(self.registry, self.config)
        self._logger = logging.getLogger(__name__)
        self._setup_logging()

        # Create composite validator
        self.validator = CompositeValidator(
            [
                SyntaxValidator(),
                FeatureValidator(),
            ]
        )

    def _setup_logging(self):
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
        self._logger.info(f"Registered table '{name}' with {dataset.count()} rows")

    def unregister_table(self, name: str) -> None:
        """Unregister a table by name.

        Args:
            name: Table name to unregister.
        """
        self.registry.unregister(name)
        self._logger.info(f"Unregistered table '{name}'")

    def sql(self, query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
        """Execute a SQL query.

        Args:
            query: SQL query string.
            default_dataset: Default dataset for queries without FROM clause.

        Returns:
            Ray Dataset containing the query results.

        Raises:
            SQLParseError: If the query cannot be parsed.
            UnsupportedOperationError: If the query uses unsupported features.
            SQLExecutionError: If query execution fails.
        """
        start_time = time.time()
        self._logger.info(f"Executing SQL query: {query}")

        try:
            # Parse the SQL query
            ast = sqlglot.parse_one(query)
            if not ast:
                raise SQLParseError("Query could not be parsed", query=query)

            # Validate the query
            self.validator.validate(query, ast)

            # Execute the query
            if isinstance(ast, exp.Select):
                result = self.execution_engine.execute(ast, default_dataset)
            else:
                raise UnsupportedOperationError(
                    f"{type(ast).__name__} statements",
                    suggestion="Only SELECT statements are currently supported",
                    query=query,
                )

            execution_time = time.time() - start_time
            self._logger.info(f"Query executed successfully in {execution_time:.3f}s")

            return result

        except Exception as e:
            if isinstance(
                e, (SQLParseError, UnsupportedOperationError, SQLExecutionError)
            ):
                raise e

            # Wrap unexpected errors
            raise SQLExecutionError(
                f"Unexpected error during query execution: {str(e)}",
                query=query,
            ) from e

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
        self._logger.info("Cleared all registered tables")

    def get_supported_features(self) -> Set[str]:
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


# Global engine instance
_global_engine: Optional[RaySQL] = None

# Global configuration
_global_config: Optional[SQLConfig] = None


def get_engine() -> RaySQL:
    """Get the global SQL engine instance.

    Returns:
        Global RaySQL engine instance.
    """
    global _global_engine
    if _global_engine is None:
        # Use global configuration if available
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
    """Get the global SQL configuration.

    Returns:
        Global SQL configuration instance.
    """
    global _global_config
    if _global_config is None:
        _global_config = SQLConfig()
    return _global_config


def set_global_config(config: SQLConfig) -> None:
    """Set the global SQL configuration.

    Args:
        config: SQL configuration to set globally.
    """
    global _global_config, _global_engine
    _global_config = config
    # Reset global engine to use new config
    _global_engine = None


def configure(**kwargs: Any) -> None:
    """Configure the global SQL settings with keyword arguments.

    This is a convenience function for setting common configuration options.

    Args:
        **kwargs: Configuration options to set.
            - dialect: SQL dialect (str or SQLDialect)
            - log_level: Logging level (str or LogLevel)
            - case_sensitive: Whether names are case-sensitive
            - strict_mode: Whether to enable strict mode
            - enable_optimization: Whether to enable optimization
            - max_join_partitions: Maximum join partitions
            - enable_predicate_pushdown: Whether to enable predicate pushdown
            - enable_projection_pushdown: Whether to enable projection pushdown
            - query_timeout_seconds: Query timeout in seconds
            - enable_sqlglot_optimizer: Whether to enable SQLGlot optimization

    Examples:
        Set dialect:
            >>> ray.data.sql.configure(dialect="postgres")

        Set multiple options:
            >>> ray.data.sql.configure(
            ...     dialect="mysql",
            ...     strict_mode=True,
            ...     log_level="debug"
            ... )
    """
    from .config import LogLevel, SQLDialect

    config = get_global_config()

    for key, value in kwargs.items():
        if key == "dialect":
            if isinstance(value, str):
                try:
                    value = SQLDialect(value.lower())
                except ValueError:
                    raise ValueError(
                        f"Invalid dialect '{value}'. Supported: {[d.value for d in SQLDialect]}"
                    )
            config.dialect = value
        elif key == "log_level":
            if isinstance(value, str):
                try:
                    value = LogLevel(value.upper())
                except ValueError:
                    raise ValueError(
                        f"Invalid log_level '{value}'. Supported: {[l.value for l in LogLevel]}"
                    )
            config.log_level = value
        elif hasattr(config, key):
            setattr(config, key, value)
        else:
            raise ValueError(f"Unknown configuration option: {key}")

    set_global_config(config)


def get_dialect() -> str:
    """Get the current SQL dialect.

    Returns:
        Current SQL dialect as string.
    """
    return get_global_config().dialect.value


def set_dialect(dialect: str) -> None:
    """Set the SQL dialect for parsing and validation.

    Args:
        dialect: SQL dialect to use. Supported: duckdb, postgres, mysql, sqlite, spark, bigquery, snowflake, redshift

    Examples:
        >>> ray.data.sql.set_dialect("postgres")
        >>> ray.data.sql.set_dialect("mysql")
    """
    configure(dialect=dialect)


def get_log_level() -> str:
    """Get the current logging level.

    Returns:
        Current logging level as string.
    """
    return get_global_config().log_level.value


def set_log_level(level: str) -> None:
    """Set the logging level for SQL operations.

    Args:
        level: Logging level. Supported: debug, info, warning, error

    Examples:
        >>> ray.data.sql.set_log_level("debug")
        >>> ray.data.sql.set_log_level("info")
    """
    configure(log_level=level)


def enable_optimization(enable: bool = True) -> None:
    """Enable or disable query optimization.

    Args:
        enable: Whether to enable optimization.

    Examples:
        >>> ray.data.sql.enable_optimization(True)
        >>> ray.data.sql.enable_optimization(False)
    """
    configure(enable_optimization=enable)


def set_join_partitions(max_partitions: int) -> None:
    """Set the maximum number of partitions for join operations.

    Args:
        max_partitions: Maximum number of partitions.

    Examples:
        >>> ray.data.sql.set_join_partitions(50)
    """
    if max_partitions <= 0:
        raise ValueError("max_partitions must be positive")
    configure(max_join_partitions=max_partitions)


def enable_predicate_pushdown(enable: bool = True) -> None:
    """Enable or disable predicate pushdown optimization.

    Args:
        enable: Whether to enable predicate pushdown.

    Examples:
        >>> ray.data.sql.enable_predicate_pushdown(True)
    """
    configure(enable_predicate_pushdown=enable)


def enable_projection_pushdown(enable: bool = True) -> None:
    """Enable or disable projection pushdown optimization.

    Args:
        enable: Whether to enable projection pushdown.

    Examples:
        >>> ray.data.sql.enable_projection_pushdown(True)
    """
    configure(enable_projection_pushdown=enable)


def set_query_timeout(seconds: int) -> None:
    """Set the query timeout in seconds.

    Args:
        seconds: Timeout in seconds.

    Examples:
        >>> ray.data.sql.set_query_timeout(300)  # 5 minutes
    """
    if seconds <= 0:
        raise ValueError("timeout must be positive")
    configure(query_timeout_seconds=seconds)


def enable_sqlglot_optimizer(enable: bool = True) -> None:
    """Enable or disable SQLGlot query optimization.

    Args:
        enable: Whether to enable SQLGlot optimization.

    Examples:
        >>> ray.data.sql.enable_sqlglot_optimizer(True)
    """
    configure(enable_sqlglot_optimizer=enable)


def reset_config() -> None:
    """Reset all configuration to default values.

    Examples:
        >>> ray.data.sql.reset_config()
    """
    global _global_config, _global_engine
    _global_config = SQLConfig()
    _global_engine = None


def get_config_summary() -> Dict[str, Any]:
    """Get a summary of the current configuration.

    Returns:
        Dictionary with current configuration values.

    Examples:
        >>> config = ray.data.sql.get_config_summary()
        >>> print(f"Dialect: {config['dialect']}")
        >>> print(f"Log level: {config['log_level']}")
    """
    config = get_global_config()
    return {
        "dialect": config.dialect.value,
        "log_level": config.log_level.value.lower(),  # Return lowercase for consistency
        "case_sensitive": config.case_sensitive,
        "strict_mode": config.strict_mode,
        "enable_optimization": config.enable_optimization,
        "max_join_partitions": config.max_join_partitions,
        "enable_predicate_pushdown": config.enable_predicate_pushdown,
        "enable_projection_pushdown": config.enable_projection_pushdown,
        "query_timeout_seconds": config.query_timeout_seconds,
        "enable_sqlglot_optimizer": config.enable_sqlglot_optimizer,
    }


# Public API functions
def sql(query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
    """Execute a SQL query using the global engine.

    Args:
        query: SQL query string.
        default_dataset: Default dataset for queries without FROM clause.

    Returns:
        Ray Dataset containing the query results.
    """
    return get_engine().sql(query, default_dataset)


def register_table(name: str, dataset: Dataset) -> None:
    """Register a Ray Dataset as a SQL table.

    Args:
        name: SQL table name.
        dataset: Ray Dataset to register.
    """
    get_engine().register_table(name, dataset)


def list_tables() -> List[str]:
    """List all registered table names.

    Returns:
        List of registered table names.
    """
    return get_engine().list_tables()


def get_schema(table_name: str) -> Optional[Dict[str, str]]:
    """Get the schema for a registered table.

    Args:
        table_name: Name of the table.

    Returns:
        Dictionary mapping column names to types, or None if not available.
    """
    return get_engine().get_schema(table_name)


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
