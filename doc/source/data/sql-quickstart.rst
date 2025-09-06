.. _data_sql_quickstart:

=================
SQL Quickstart
=================

This quickstart guide shows you how to get started with Ray Data SQL. You will learn how to execute SQL queries on Ray Datasets using familiar SQL syntax.

Installation
============

Ray Data SQL is included with Ray Data. Install Ray Data with:

.. code-block:: console

    pip install -U 'ray[data]'

Basic usage
===========

Step 1: Create and register datasets
------------------------------------

Start by creating Ray Datasets and registering them as SQL tables:

.. testcode::

    import ray.data
    from ray.data.sql import register_table, sql

    # Create sample datasets
    users = ray.data.from_items([
        {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Los Angeles"},
        {"id": 4, "name": "Diana", "age": 28, "city": "New York"}
    ])
    
    orders = ray.data.from_items([
        {"id": 101, "user_id": 1, "amount": 150.0, "product": "Laptop"},
        {"id": 102, "user_id": 2, "amount": 75.0, "product": "Book"},
        {"id": 103, "user_id": 1, "amount": 50.0, "product": "Mouse"},
        {"id": 104, "user_id": 3, "amount": 200.0, "product": "Monitor"},
        {"id": 105, "user_id": 4, "amount": 85.0, "product": "Keyboard"}
    ])

    # Register datasets as SQL tables
    register_table("users", users)
    register_table("orders", orders)

Step 2: Execute basic SQL queries
---------------------------------

Now you can execute SQL queries using the familiar SELECT syntax:

.. testcode::

    # Simple SELECT query
    result = sql("SELECT name, age FROM users WHERE age > 25")
    print(result.take_all())
    # Output: [{'name': 'Alice', 'age': 30}, {'name': 'Charlie', 'age': 35}, {'name': 'Diana', 'age': 28}]

    # Query with ORDER BY
    result = sql("SELECT name, age FROM users ORDER BY age DESC")
    print(result.take_all())
    # Output: [{'name': 'Charlie', 'age': 35}, {'name': 'Alice', 'age': 30}, {'name': 'Diana', 'age': 28}, {'name': 'Bob', 'age': 25}]

    # Query with aggregation
    result = sql("SELECT city, COUNT(*) as user_count FROM users GROUP BY city")
    print(result.take_all())
    # Output: [{'city': 'New York', 'user_count': 2}, {'city': 'San Francisco', 'user_count': 1}, {'city': 'Los Angeles', 'user_count': 1}]

Step 3: Join multiple tables
----------------------------

Perform JOIN operations to combine data from multiple tables:

.. testcode::

    # INNER JOIN to combine users and orders
    result = sql("""
        SELECT u.name, u.city, o.product, o.amount
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 100
    """)
    print(result.take_all())
    # Output: [{'name': 'Alice', 'city': 'New York', 'product': 'Laptop', 'amount': 150.0}, 
    #          {'name': 'Charlie', 'city': 'Los Angeles', 'product': 'Monitor', 'amount': 200.0}]

    # LEFT JOIN to include all users
    result = sql("""
        SELECT u.name, 
               COALESCE(SUM(o.amount), 0) as total_spent,
               COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
        ORDER BY total_spent DESC
    """)
    print(result.take_all())

Step 4: Advanced queries
------------------------

Use more advanced SQL features like subqueries and window functions:

.. testcode::

    # Subquery example
    result = sql("""
        SELECT name, age
        FROM users
        WHERE id IN (
            SELECT user_id 
            FROM orders 
            WHERE amount > 100
        )
    """)
    print(result.take_all())

    # Complex aggregation with HAVING
    result = sql("""
        SELECT u.city, 
               AVG(u.age) as avg_age,
               SUM(o.amount) as total_revenue
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        GROUP BY u.city
        HAVING SUM(o.amount) > 100
    """)
    print(result.take_all())

Integration with Ray Data
=========================

SQL results are Ray Datasets, so you can seamlessly mix SQL with Ray Data operations:

.. testcode::

    # Start with SQL
    high_spenders = sql("""
        SELECT u.name, SUM(o.amount) as total
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
        HAVING SUM(o.amount) > 100
    """)

    # Continue with Ray Data operations
    processed = high_spenders.map(lambda row: {
        "customer": row["name"].upper(),
        "spending_tier": "Premium" if row["total"] > 150 else "Standard",
        "total": row["total"]
    })

    # Back to SQL (register the processed dataset)
    register_table("customer_tiers", processed)
    final_result = sql("""
        SELECT spending_tier, 
               COUNT(*) as customer_count,
               AVG(total) as avg_spending
        FROM customer_tiers 
        GROUP BY spending_tier
    """)

    print(final_result.take_all())

Configuration options
=====================

Customize SQL engine behavior with configuration:

.. testcode::

    from ray.data.sql import SQLConfig, LogLevel

    # Create custom configuration
    config = SQLConfig(
        log_level=LogLevel.DEBUG,  # Enable debug logging
        case_sensitive=False,      # Case-insensitive column names
        enable_optimization=True,  # Enable query optimization
        strict_mode=False         # Allow flexible type conversions
    )

    # Apply configuration globally
    from ray.data import DataContext
    with DataContext() as ctx:
        ctx.sql_config = config
        result = sql("SELECT NAME from USERS where AGE > 30")  # Case-insensitive

Auto-registration
=================

For convenience, you can use auto-registration with variable names:

.. testcode::

    # Create datasets (variable names become table names)
    customers = ray.data.from_items([{"id": 1, "name": "Alice"}])
    purchases = ray.data.from_items([{"customer_id": 1, "item": "book"}])

    # Auto-register and query (uses variable names as table names)
    result = sql("""
        SELECT c.name, p.item
        FROM customers c
        JOIN purchases p ON c.id = p.customer_id
    """)

Table management
================

Manage your SQL tables with utility functions:

.. testcode::

    from ray.data.sql import list_tables, get_schema, clear_tables

    # List all registered tables
    tables = list_tables()
    print(f"Available tables: {tables}")

    # Get schema information
    schema = get_schema("users")
    print(f"Users table schema: {schema}")

    # Clear all tables when done
    clear_tables()

Important limitations to know
=============================

Before diving deeper, be aware of these current limitations:

SQL feature limitations
  - **Window Functions**: Limited support for ROW_NUMBER(), RANK(), etc.
  - **User-Defined Functions**: Custom SQL functions aren't supported
  - **Recursive CTEs**: Recursive Common Table Expressions aren't available
  - **Materialized Views**: Only direct table queries, no view support

Performance considerations
  - **Large JOINs**: Cross-joins and large JOINs can be memory-intensive
  - **Complex Subqueries**: May have performance implications
  - **Data Types**: Best performance with Arrow-native types

Workarounds available
Most limitations can be worked around using Ray Data operations:

.. testcode::

    # Instead of window functions, use Ray Data groupby
    # ❌ Limited: SELECT name, ROW_NUMBER() OVER (...) FROM users
    # ✅ Use: users.groupby("dept").map_groups(lambda group: ...)
    
    # Instead of UDFs, use Ray Data map
    # ❌ Not supported: SELECT custom_function(name) FROM users  
    # ✅ Use: users.map(lambda row: {"result": custom_function(row["name"])})

SQL dialect support
  - **Primary**: DuckDB SQL dialect (recommended)
  - **Supported**: PostgreSQL, MySQL, BigQuery syntax (with conversion)
  - **Automatic Conversion**: SQLGlot handles dialect differences

What's next?
============

Now that you've learned the basics, explore more advanced features:

- **Advanced SQL Operations**: Check out the :ref:`SQL User Guide <data_sql_user_guide>` for complex queries, optimization tips, and best practices.

- **API Reference**: Browse the complete :ref:`SQL API Reference <data_sql_api>` for detailed function and class documentation.

- **Ray Data Integration**: Learn how to seamlessly integrate SQL with other Ray Data features in the main :ref:`Ray Data documentation <data>`.

- **Examples**: Find more SQL examples and use cases in the :ref:`Ray Data Examples <examples>` section. 