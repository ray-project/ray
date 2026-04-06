"""
SQL utility functions for Kinetica Ray Data integration.

This module provides convenience functions for reading from and writing to
Kinetica using Ray Data's SQL capabilities via the DB-API 2.0 interface.
"""

from typing import Any, Dict, Optional, TYPE_CHECKING

from ray.data._internal.datasource.kinetica_sql_connection import (
    create_kinetica_connection_factory,
)

if TYPE_CHECKING:
    from ray.data import Dataset


def read_kinetica_sql(
    sql: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    oauth_token: Optional[str] = None,
    default_schema: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> "Dataset":
    """
    Read data from Kinetica using a SQL query into a Ray Dataset.

    This function uses Ray Data's native `read_sql` method with Kinetica's
    DB-API 2.0 compliant connection. It supports parallel reads when the
    query can be partitioned.

    Args:
        sql: SQL query to execute (e.g., "SELECT * FROM my_table WHERE x > 10").
        url: URL of the Kinetica server.
        username: Username for authentication.
        password: Password for authentication.
        oauth_token: OAuth token for authentication (alternative to username/password).
        default_schema: Default schema to use for queries.
        options: Additional GPUdb client options.
        ray_remote_args: Additional arguments to pass to Ray remote functions.
        concurrency: Maximum number of concurrent read tasks.
        override_num_blocks: Override the number of output blocks.

    Returns:
        A Ray Dataset containing the query results.

    Example:
        >>> from ray.data import read_kinetica_sql
        >>>
        >>> # Simple query
        >>> ds = read_kinetica_sql(
        ...     sql="SELECT id, name, value FROM my_table WHERE value > 100",
        ...     url="http://localhost:9191",
        ...     username="admin",
        ...     password="password",
        ... )
        >>>
        >>> # Process results
        >>> for row in ds.iter_rows():
        ...     print(row)
        >>>
        >>> # With schema
        >>> ds = read_kinetica_sql(
        ...     sql="SELECT * FROM sales.transactions",
        ...     url="http://localhost:9191",
        ...     username="admin",
        ...     password="password",
        ...     default_schema="sales",
        ... )

    Note:
        For parallel reads, Ray Data attempts to shard the query across
        multiple workers. This works best with simple SELECT queries.
        Complex queries with JOINs, aggregations, or subqueries may
        fall back to single-threaded execution.
    """
    import ray.data

    connection_factory = create_kinetica_connection_factory(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        default_schema=default_schema,
        options=options,
    )

    # Build kwargs for read_sql
    read_kwargs: Dict[str, Any] = {
        "sql": sql,
        "connection_factory": connection_factory,
    }

    if ray_remote_args is not None:
        read_kwargs["ray_remote_args"] = ray_remote_args

    if concurrency is not None:
        read_kwargs["concurrency"] = concurrency

    if override_num_blocks is not None:
        read_kwargs["override_num_blocks"] = override_num_blocks

    return ray.data.read_sql(**read_kwargs)


def write_kinetica_sql(
    dataset: "Dataset",
    sql: str,
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    oauth_token: Optional[str] = None,
    default_schema: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """
    Write a Ray Dataset to Kinetica using a SQL INSERT statement.

    This function uses Ray Data's native `write_sql` method with Kinetica's
    DB-API 2.0 compliant connection. It supports parallel writes across
    multiple workers.

    Args:
        dataset: The Ray Dataset to write.
        sql: SQL INSERT statement with parameter placeholders.
            Use '?' for placeholders (qmark paramstyle).
            Example: "INSERT INTO my_table (id, name, value) VALUES (?, ?, ?)"
        url: URL of the Kinetica server.
        username: Username for authentication.
        password: Password for authentication.
        oauth_token: OAuth token for authentication (alternative to username/password).
        default_schema: Default schema to use for queries.
        options: Additional GPUdb client options.
        ray_remote_args: Additional arguments to pass to Ray remote functions.
        concurrency: Maximum number of concurrent write tasks.

    Example:
        >>> import ray
        >>> from ray.data import write_kinetica_sql
        >>>
        >>> # Create a dataset
        >>> data = [
        ...     {"id": 1, "name": "Alice", "value": 100.0},
        ...     {"id": 2, "name": "Bob", "value": 200.0},
        ... ]
        >>> ds = ray.data.from_items(data)
        >>>
        >>> # Write to Kinetica
        >>> write_kinetica_sql(
        ...     dataset=ds,
        ...     sql="INSERT INTO my_table (id, name, value) VALUES (?, ?, ?)",
        ...     url="http://localhost:9191",
        ...     username="admin",
        ...     password="password",
        ... )

    Note:
        - The table must already exist in Kinetica.
        - Column order in the INSERT statement must match the dataset columns.
        - Use Kinetica's qmark paramstyle (?) for parameter placeholders.
    """
    connection_factory = create_kinetica_connection_factory(
        url=url,
        username=username,
        password=password,
        oauth_token=oauth_token,
        default_schema=default_schema,
        options=options,
    )

    # Build kwargs for write_sql
    write_kwargs: Dict[str, Any] = {
        "sql": sql,
        "connection_factory": connection_factory,
    }

    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args

    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency

    dataset.write_sql(**write_kwargs)
