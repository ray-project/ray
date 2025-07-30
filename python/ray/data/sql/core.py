"""
Core SQL engine and global API for Ray Data SQL.

This module provides the main RaySQL engine class and global API functions
for SQL query execution, table registration, and dataset management.
"""

import inspect
import time
from typing import List, Optional

from ray.data import Dataset
from ray.data.sql.config import QueryResult, SQLConfig
from ray.data.sql.execution import QueryExecutor
from ray.data.sql.parser import ASTOptimizer, LogicalPlanner, SQLParser
from ray.data.sql.schema import DatasetRegistry
from ray.data.sql.utils import (
    _is_create_table_as,
    extract_table_names_from_query,
    get_config_from_context,
    setup_logger,
)


class RaySQL:
    """Main entry point for the RaySQL engine with SQLGlot-inspired architecture.

    The RaySQL class provides the main API for executing SQL queries on Ray Datasets.
    It manages dataset registration, parsing, optimization, planning, and execution.

    Examples:
        .. testcode::

            engine = RaySQL()
            engine.register("my_table", my_dataset)
            result = engine.sql("SELECT * FROM my_table")
    """

    def __init__(self, config: Optional[SQLConfig] = None, registry: Optional[DatasetRegistry] = None):
        self.config = config or SQLConfig()
        self.registry = registry or DatasetRegistry()
        self.parser = SQLParser(self.config)
        self.optimizer = ASTOptimizer(self.config)
        self.planner = LogicalPlanner(self.config)
        self.executor = QueryExecutor(self.registry, self.config)
        self._logger = setup_logger("RaySQL")
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        import logging

        log_level_mapping = {
            "ERROR": logging.ERROR,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
        }
        level_name = self.config.log_level.name
        self._logger.setLevel(log_level_mapping[level_name])

    def _auto_register_datasets_from_caller(self):
        """Register all Ray Datasets in the caller's local scope as tables using their variable names.

        Warning: This feature uses inspect.currentframe() and can be brittle. If called from
        within helper functions or nested scopes, it will register datasets from the helper's
        scope, not the intended user scope. Use explicit register_table() calls for more
        predictable behavior in complex scenarios.
        """
        frame = (
            inspect.currentframe().f_back.f_back
        )  # Go two frames up: sql() -> RaySQL.sql() -> user
        try:
            for name, obj in frame.f_locals.items():
                if isinstance(obj, Dataset) and name not in self.registry._tables:
                    try:
                        self.registry.register(name, obj)
                    except ValueError:
                        # Skip invalid table names
                        self._logger.debug(
                            f"Skipped registering '{name}' due to invalid table name"
                        )
        finally:
            del frame

    def sql(self, query: str, auto_register: bool = True) -> Dataset:
        """Execute a SQL query using the full pipeline: parse -> optimize -> plan -> execute.

        Args:
            query: SQL query string.
            auto_register: Whether to auto-register datasets from the caller's scope.

        Returns:
            Ray Dataset with the query result.

        Raises:
            Exception: If query execution fails.
        """
        start_time = time.time()
        stats = QueryResult(
            dataset=None,  # Will be set later
            execution_time=0.0,
            row_count=0,
            query_text=query,
        )

        try:
            # Step 1: Auto-register datasets from caller scope
            if auto_register:
                self._auto_register_datasets_from_caller()

            # Check for missing tables
            table_names = extract_table_names_from_query(query)
            missing = [t for t in table_names if t not in self.registry._tables]
            if missing:
                raise ValueError(
                    f"Tables not found: {missing}. Available tables: {list(self.registry._tables.keys())}"
                )

            self._logger.info(f"Executing SQL: {query}")

            # Step 2: Parse SQL to AST
            parse_start = time.time()
            ast = self.parser.parse(query)
            stats.parse_time = time.time() - parse_start
            self._logger.debug(f"Parsing completed in {stats.parse_time:.3f}s")

            # Step 3: Apply SQLGlot optimizations
            if self.config.enable_sqlglot_optimizer:
                from sqlglot.optimizer import optimize

                sqlglot_opt_start = time.time()
                ast = optimize(ast)
                sqlglot_opt_time = time.time() - sqlglot_opt_start
                stats.optimize_time += sqlglot_opt_time
                self._logger.debug(
                    f"SQLGlot optimization completed in {sqlglot_opt_time:.3f}s"
                )

            # Step 4: Apply custom AST optimizations
            custom_opt_start = time.time()
            ast = self.optimizer.optimize(ast, self.registry.schema_manager)
            custom_opt_time = time.time() - custom_opt_start
            stats.optimize_time += custom_opt_time
            self._logger.debug(
                f"Custom optimization completed in {custom_opt_time:.3f}s"
            )

            # Step 5: Generate logical plan
            plan_start = time.time()
            logical_plan = self.planner.plan(ast)
            stats.plan_time = time.time() - plan_start
            if logical_plan:
                self._logger.debug(
                    f"Logical planning completed in {stats.plan_time:.3f}s: {logical_plan}"
                )

            # Step 6: Execute the query
            exec_start = time.time()
            result = self._execute_query(ast)
            stats.execute_time = time.time() - exec_start

            # Calculate final statistics
            stats.execution_time = time.time() - start_time
            stats.dataset = result
            stats.row_count = result.count()

            # Log execution statistics
            stats.log_stats(self._logger)

            return result

        except Exception as e:
            stats.execution_time = time.time() - start_time
            self._logger.error(
                f"Query execution failed after {stats.execution_time:.3f}s: {e}"
            )
            self._log_enhanced_error(e)
            raise

    def _execute_query(self, ast):
        """Execute the parsed AST."""
        if _is_create_table_as(ast):
            table_name = str(ast.this.name)
            select_ast = ast.args["expression"]
            result = self.executor.execute(select_ast)
            self.register(table_name, result)
            self._logger.info(f"Created table '{table_name}' from SELECT")
            return result
        else:
            result = self.executor.execute(ast)
            return result

    def _log_enhanced_error(self, e: Exception):
        """Log enhanced error information."""
        if hasattr(e, "line_no") and hasattr(e, "col_no"):
            self._logger.error(f"Error at line {e.line_no}, column {e.col_no}")

        if "not found" in str(e):
            available_tables = self.registry.list_tables()
            self._logger.error(f"Available tables: {available_tables}")

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a Ray Dataset as a SQL table."""
        self.registry.register(name, dataset)

    def unregister(self, name: str) -> None:
        """Unregister a table by name."""
        self.registry.unregister(name)

    def tables(self) -> List[str]:
        """List all registered table names."""
        return self.registry.list_tables()

    def clear(self) -> None:
        """Remove all registered tables."""
        self.registry.clear()

    def get_schema(self, table_name: str):
        """Get schema information for a table."""
        return self.registry.schema_manager.get_schema(table_name)


# Global registry and engine instances for the module-level API
_global_registry = DatasetRegistry()
_global_engine = None


def _get_engine() -> RaySQL:
    """Get or create the SQL engine with current configuration."""
    global _global_engine
    if _global_engine is None:
        config = get_config_from_context()
        _global_engine = RaySQL(config, registry=_global_registry)
    return _global_engine


def _auto_register_datasets():
    """Automatically register Ray Datasets from the current frame."""
    frame = inspect.currentframe().f_back
    try:
        # Only register datasets that aren't already in the registry
        count = 0
        for name, obj in frame.f_locals.items():
            if isinstance(obj, Dataset) and name not in _global_registry._tables:
                try:
                    _global_registry.register(name, obj)
                    count += 1
                except ValueError:
                    # Skip invalid table names
                    pass
        if count > 0:
            logger = setup_logger("RaySQL")
            logger.debug(f"Auto-registered {count} datasets")
    finally:
        del frame


def _ray_data_sql(query: str, **kwargs) -> Dataset:
    """Enhanced ray.data.sql function with full SQL support.

    This function follows Ray Dataset API patterns for lazy evaluation
    and proper return types.

    Args:
        query: SQL query string to execute.
        **kwargs: Additional arguments passed to the SQL engine.

    Returns:
        Dataset: A Ray Dataset containing the query results.

    Raises:
        ValueError: If the query is invalid or execution fails.
        NotImplementedError: If unsupported SQL features are used.
    """
    table_names = extract_table_names_from_query(query)
    missing = [t for t in table_names if t not in _global_registry._tables]

    # Only auto-register if there are missing tables
    if missing:
        _auto_register_datasets()
        # Check again after auto-registration
        still_missing = [t for t in table_names if t not in _global_registry._tables]
        if still_missing:
            raise ValueError(
                f"Tables not found: {still_missing}. Available tables: {list(_global_registry._tables.keys())}"
            )

    engine = _get_engine()
    return engine.sql(query, **kwargs)


def _ray_data_register_table(name: str, dataset: Dataset) -> None:
    """Register a dataset as a SQL table.

    This function follows Ray Dataset API patterns for table registration.

    Args:
        name: Table name to register the dataset under.
        dataset: Ray Dataset to register as a table.

    Raises:
        TypeError: If dataset is not a Ray Dataset.
        ValueError: If table name is invalid.
    """
    if not isinstance(dataset, Dataset):
        raise TypeError(f"Expected Dataset, got {type(dataset)}")
    if not name or not isinstance(name, str):
        raise ValueError("Table name must be a non-empty string")
    _global_registry.register(name, dataset)


def _dataset_name(self, table_name: str) -> Dataset:
    """Register this dataset as a SQL table and return self.

    This method follows Ray Dataset API patterns for method chaining
    and returns the dataset for further operations.

    Args:
        table_name: Name to register this dataset under.

    Returns:
        Dataset: Self for method chaining.

    Raises:
        ValueError: If table name is invalid.
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Table name must be a non-empty string")
    _global_registry.register(table_name, self)
    self._sql_name = table_name
    return self


# Global functions following Ray Dataset API patterns
def sql(query: str, **kwargs) -> Dataset:
    """Global SQL entry point using ray.data.sql.

    This function follows Ray Dataset API patterns for global functions.

    Args:
        query: SQL query string to execute.
        **kwargs: Additional arguments passed to the SQL engine.

    Returns:
        Dataset: A Ray Dataset containing the query results.
    """
    return _ray_data_sql(query, **kwargs)


def register_table(name: str, dataset: Dataset) -> None:
    """Register a dataset as a SQL table.

    This function follows Ray Dataset API patterns for global functions.

    Args:
        name: Table name to register the dataset under.
        dataset: Ray Dataset to register as a table.
    """
    _ray_data_register_table(name, dataset)


def list_tables() -> List[str]:
    """List all registered table names.

    This function follows Ray Dataset API patterns for global functions.

    Returns:
        List[str]: List of registered table names.
    """
    return _global_registry.list_tables()


def get_schema(table_name: str):
    """Get schema information for a table.

    This function follows Ray Dataset API patterns for global functions.

    Args:
        table_name: Name of the table to get schema for.

    Returns:
        Schema information if table exists, None otherwise.
    """
    return _global_registry.schema_manager.get_schema(table_name)


def clear_tables() -> None:
    """Remove all registered tables.

    This function follows Ray Dataset API patterns for global functions.
    """
    global _global_engine
    _global_registry.clear()
    # Reset the global engine to ensure clean state
    _global_engine = None


def get_engine() -> RaySQL:
    """Get the global SQL engine instance.

    Returns:
        RaySQL: The global SQL engine instance.
    """
    return _get_engine()


def get_registry() -> DatasetRegistry:
    """Get the global dataset registry.

    Returns:
        DatasetRegistry: The global dataset registry.
    """
    return _global_registry
