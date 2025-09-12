"""
Simplified SQL API for Ray Data - DuckDB-style interface.

This module provides a clean, Pythonic SQL interface that automatically
discovers Ray Datasets from the caller's namespace, similar to how DuckDB
automatically discovers Pandas DataFrames.
"""

import inspect
from typing import Any, Dict, Set

from ray.data import Dataset
from ray.data.sql.core import get_engine
from ray.data.sql.utils import extract_table_names_from_query
from ray.util.annotations import PublicAPI


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

        Your requested pattern:
            >>> ds = ray.data.from_items([{"x": 1}, {"x": 2}])
            >>> new_ds = ray.data.sql("SELECT * FROM ds WHERE x > 1")
            >>> # That's it! No registration needed!
    """
    # Extract table names from the SQL query
    try:
        table_names = extract_table_names_from_query(query)
    except Exception:
        # If we can't parse table names, fall back to current behavior
        engine = get_engine()
        return engine.sql(query)

    # Get the caller's frame to access their local variables
    caller_frame = inspect.currentframe().f_back
    caller_locals = caller_frame.f_locals
    caller_globals = caller_frame.f_globals

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
def register(name: str, dataset: Dataset) -> None:
    """Register a Dataset as a SQL table (optional - for persistent registration).

    Args:
        name: SQL table name.
        dataset: Ray Dataset to register.

    Examples:
        >>> import ray.data
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ray.data.register("users", users)  # Persistent registration
        >>> result = ray.data.sql("SELECT * FROM users")
    """
    engine = get_engine()
    engine.register_table(name, dataset)


@PublicAPI(stability="alpha")
def clear_tables() -> None:
    """Clear all registered SQL tables.

    Examples:
        >>> ray.data.clear_tables()  # Clean slate
    """
    engine = get_engine()
    engine.clear_tables()


@PublicAPI(stability="alpha")
def list_tables() -> List[str]:
    """List all registered SQL tables.

    Returns:
        List of registered table names.

    Examples:
        >>> tables = ray.data.list_tables()
        >>> print(f"Available tables: {tables}")
    """
    engine = get_engine()
    return engine.list_tables()


# Simple configuration through module-level properties
class _SQLConfig:
    """Simple SQL configuration accessible as ray.data.sql_config.*"""

    def __init__(self):
        self._dialect = "duckdb"
        self._case_sensitive = True

    @property
    def dialect(self) -> str:
        """SQL dialect (duckdb, postgres, mysql, etc.)."""
        return self._dialect

    @dialect.setter
    def dialect(self, value: str) -> None:
        from ray.data.sql.core import set_dialect

        set_dialect(value)
        self._dialect = value

    @property
    def case_sensitive(self) -> bool:
        """Whether identifiers are case sensitive."""
        return self._case_sensitive

    @case_sensitive.setter
    def case_sensitive(self, value: bool) -> None:
        from ray.data.sql.core import configure

        configure(case_sensitive=value)
        self._case_sensitive = value


# Create global config instance
sql_config = _SQLConfig()
