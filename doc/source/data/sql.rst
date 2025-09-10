.. _data_sql:

============================
Ray Data SQL: SQL for Data
============================

.. warning::
    Ray Data SQL is currently experimental and APIs are subject to change.

.. toctree::
    :hidden:

    sql-quickstart
    sql-user-guide
    api/sql

Ray Data SQL provides a comprehensive SQL interface for Ray Datasets, enabling familiar SQL operations on distributed data. Execute complex queries, joins, aggregations, and transformations using standard SQL syntax while leveraging Ray's scalable processing capabilities.

What is Ray Data SQL?
---------------------

Ray Data SQL is a distributed SQL engine that runs on Ray clusters. It allows you to query Ray Datasets using standard SQL syntax, making distributed data processing accessible to users familiar with SQL.

Key features:

- **Familiar SQL Interface**: Use standard SQL syntax you already know to process distributed datasets.
- **Seamless Integration**: SQL queries return Ray Datasets, allowing you to seamlessly mix SQL operations with Ray Data's rich API.
- **Distributed Processing**: Automatically scales across your Ray cluster, handling large datasets efficiently.
- **Rich Data Type Support**: Works with Arrow data types, supporting complex nested structures, timestamps, and more.
- **Performance Optimized**: Built-in query optimization and pushdown operations for efficient execution.

Why use Ray Data SQL?
---------------------

Ray Data SQL is ideal for:

- **Data analysts** who want to use familiar SQL syntax on distributed datasets
- **Data scientists** who need to combine SQL queries with machine learning workflows
- **Data engineers** who want to process large datasets using standard SQL operations
- **Teams** that need to bridge the gap between SQL-based tools and distributed computing

Common use cases:

- **Data exploration**: Quickly explore large datasets using familiar SELECT queries
- **Data transformation**: Transform and clean data using SQL before applying machine learning
- **Analytics**: Perform complex aggregations and joins on distributed data
- **ETL pipelines**: Build data pipelines that combine SQL operations with Ray Data transformations

Install Ray Data SQL
---------------------

Ray Data SQL is included with Ray Data. To install:

.. code-block:: console

    pip install -U 'ray[data]'

Quick start
-----------

Here's a simple example of using SQL with Ray Data:

.. testcode::

    import ray.data
    from ray.data.sql import register_table, sql

    # Create datasets
    users = ray.data.from_items([
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ])
    
    orders = ray.data.from_items([
        {"user_id": 1, "amount": 100, "product": "laptop"},
        {"user_id": 2, "amount": 50, "product": "book"},
        {"user_id": 1, "amount": 75, "product": "mouse"}
    ])

    # Register datasets as SQL tables
    register_table("users", users)
    register_table("orders", orders)

    # Execute SQL queries
    result = ray.data.sql("""
        SELECT u.name, SUM(o.amount) as total_spent
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.age > 25
        GROUP BY u.name
        ORDER BY total_spent DESC
    """)

    print(result.take_all())
    # Output: [{'name': 'Alice', 'total_spent': 175}, {'name': 'Charlie', 'total_spent': 0}]

Supported SQL features
-----------------------

Ray Data SQL supports a wide range of SQL operations:

Basic queries
  - SELECT with column projections and aliases
  - WHERE clauses with complex conditions
  - ORDER BY with multiple columns
  - LIMIT and OFFSET for pagination

Aggregations
  - GROUP BY with multiple columns  
  - Aggregate functions: COUNT, SUM, AVG, MIN, MAX
  - HAVING clauses for post-aggregation filtering

.. vale off

Joins
  - INNER JOIN for combining datasets
  - LEFT JOIN for preserving left dataset records
  - Multiple table joins with complex conditions

Advanced features
  - Subqueries and common table expressions (CTEs)

.. vale on
  - UNION operations for combining datasets
  - Window functions (coming soon)
  - User-defined functions (coming soon)

Learn more
----------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-5

    .. grid-item-card::

        **SQL Quickstart**
        ^^^

        Get started with Ray Data SQL with step-by-step examples.

        +++
        .. button-ref:: data_sql_quickstart
            :color: primary
            :outline:
            :expand:

            SQL Quickstart

    .. grid-item-card::

        **SQL User Guide**
        ^^^

        Learn advanced SQL operations and best practices.

        +++
        .. button-ref:: data_sql_user_guide
            :color: primary
            :outline:
            :expand:

            SQL User Guide

    .. grid-item-card::

        **SQL API Reference**
        ^^^

        Complete API reference for SQL functions and classes.

        +++
        .. button-ref:: data_sql_api
            :color: primary
            :outline:
            :expand:

            SQL API Reference

    .. grid-item-card::

        **Ray Data Examples**
        ^^^

        Find more SQL examples in the Ray Data examples collection.

        +++
        .. button-ref:: examples
            :color: primary
            :outline:
            :expand:

            More Examples

Configuration and advanced usage
---------------------------------

Ray Data SQL provides extensive configuration options for different use cases:

.. testcode::

    from ray.data.sql import SQLConfig, LogLevel

    # Basic configuration
    # .. vale off
    config = SQLConfig(
        log_level=LogLevel.DEBUG,          # Control logging verbosity
        case_sensitive=False,              # Case-insensitive column names
        enable_optimization=True,          # Enable built-in optimizations
        strict_mode=True,                  # Strict SQL compliance
        enable_sqlglot_optimizer=True,     # Use SQLGlot's query optimizer
        max_join_partitions=100,           # Limit join complexity
        enable_predicate_pushdown=True     # Push filters to data sources
    )
    # .. vale on

    # Use with context manager for session-specific settings
    with ray.data.DataContext() as ctx:
        ctx.sql_config = config
        result = ray.data.sql("SELECT * FROM users WHERE age > 30")

Advanced configuration options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    # Production configuration example
    production_config = SQLConfig(
        # Performance settings
        enable_optimization=True,
        enable_sqlglot_optimizer=True,
        max_join_partitions=200,
        enable_predicate_pushdown=True,
        
        # Behavior settings
        case_sensitive=False,
        strict_mode=False,              # Allow flexible type handling
        enable_auto_registration=False, # Disable for security
        
        # Logging and debugging
        log_level=LogLevel.WARNING,     # Reduce noise in production
        enable_query_timing=True,       # Track performance metrics
        
        # Memory management
        enable_streaming_execution=True, # For large datasets
        max_memory_usage_gb=16          # Memory limit for operations
    )

SQL dialect configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    # Configure SQL dialect handling
    dialect_config = SQLConfig(
        # SQLGlot dialect settings
        sqlglot_read_dialect="duckdb",    # Input SQL dialect
        sqlglot_write_dialect="duckdb",   # Output dialect for optimization
        enable_dialect_conversion=True,   # Auto-convert between dialects
        
        # Compatibility settings
        enable_mysql_compatibility=False, # MySQL-specific features
        enable_postgres_compatibility=False, # PostgreSQL-specific features
        strict_ansi_compliance=True       # Strict ANSI SQL compliance
    )

Integration with Ray Data
-------------------------

SQL queries seamlessly integrate with Ray Data operations:

.. testcode::

    # Start with SQL
    filtered_data = sql("SELECT * FROM users WHERE age > 25")
    
    # Continue with Ray Data operations  
    transformed = filtered_data.map(lambda row: {
        "name": row["name"].upper(),
        "age_group": "senior" if row["age"] > 60 else "adult"
    })
    
    # Back to SQL
    register_table("transformed_users", transformed)
    final_result = ray.data.sql("SELECT age_group, COUNT(*) FROM transformed_users GROUP BY age_group")

Limitations and considerations
------------------------------

.. vale off

Current limitations
  - **SQL Dialect**: Currently uses DuckDB SQL dialect through SQLGlot parsing
  - **Window Functions**: Limited support for advanced window functions
  - **Subqueries**: Complex nested subqueries may have performance implications

.. vale on
  - **User-Defined Functions**: Custom SQL functions aren't yet supported
  - **Transactions**: No transaction support (operations aren't atomic)
  - **Indexes**: No index support for query optimization
  - **Materialized Views**: Views aren't supported, only direct table queries

Performance considerations
  - **Memory Usage**: Large JOIN operations may require significant memory
  - **Data Shuffling**: Cross-partition operations can be expensive
  - **Optimization**: Limited query optimization compared to dedicated SQL engines

Data type mapping
  - **Arrow Integration**: Best performance with Arrow-native types
  - **Type Coercion**: Automatic type conversion may impact performance
  - **Nested Data**: Complex nested structures have limited SQL support

Performance tips
----------------

- **Use explicit table registration** for better performance than auto-registration
- **Apply filters early** in your SQL queries to reduce data processing
- **Use appropriate data types** for better Arrow integration performance  
- **Leverage column pruning** by selecting only needed columns
- **Consider partitioning** for very large datasets
- **Avoid complex nested queries** that may trigger expensive data movements

What's next?
------------

- **Learn SQL basics**: Check out the :ref:`SQL Quickstart <data_sql_quickstart>`
- **Explore advanced features**: Read the :ref:`SQL User Guide <data_sql_user_guide>`
- **Browse the API**: See the complete :ref:`SQL API Reference <data_sql_api>`
- **Try examples**: Find SQL examples in :ref:`Ray Data Examples <examples>` 