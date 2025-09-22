"""
Clean SQL API for Ray Data - Native Dataset Operations.

This provides a clean SQL interface that converts SQL directly into
Ray Dataset native operations for maximum performance and compatibility.
"""

import inspect
from typing import List

from ray.data import Dataset
from ray.data.sql.core import get_engine
from ray.data.sql.utils import extract_table_names_from_query
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def sql(query: str, **datasets) -> Dataset:
    """Execute SQL using Ray Dataset native operations. Variables auto-discovered.
    
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
    # Get caller's variables (optimized)
    frame = inspect.currentframe().f_back
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

    Args:
        name: Table name for SQL queries.
        dataset: Ray Dataset to register.

    Examples:
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ray.data.register("users", users)
        >>> result = ray.data.sql("SELECT * FROM users")
    """
    get_engine().register_table(name, dataset)


@PublicAPI(stability="alpha")
def clear_tables() -> None:
    """Clear all registered tables."""
    get_engine().clear_tables()


@PublicAPI(stability="alpha")
def list_tables() -> List[str]:
    """List all registered tables."""
    return get_engine().list_tables()


# Simple configuration - just the essentials
class _Config:
    @property
    def dialect(self) -> str:
        """SQL dialect. Default: duckdb"""
        from ray.data.sql.core import get_dialect

        return get_dialect()

    @dialect.setter
    def dialect(self, value: str) -> None:
        from ray.data.sql.core import set_dialect

        set_dialect(value)


config = _Config()
