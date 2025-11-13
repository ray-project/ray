.. _data_sql:

=================
Ray Data SQL API
=================

.. warning::
    Ray Data SQL is experimental and APIs are subject to change.

Overview
========

Ray Data SQL provides SQL query execution on Ray Datasets using standard SQL syntax. Queries are converted to native Ray Data operations for distributed execution.

Quick Start
===========

.. testcode::

    import ray.data

    # Create datasets
    users = ray.data.from_items([
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ])
    
    orders = ray.data.from_items([
        {"user_id": 1, "amount": 100},
        {"user_id": 2, "amount": 200}
    ])

    # Execute SQL query (auto-discovers tables)
    result = ray.data.sql("""
        SELECT u.name, SUM(o.amount) as total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.name
    """)

Supported Features
==================

- SELECT, WHERE, JOIN (INNER, LEFT, RIGHT, FULL)
- GROUP BY, HAVING, ORDER BY, LIMIT
- Aggregates: COUNT, SUM, AVG, MIN, MAX, STD
- CTEs and UNION operations
- Multi-dialect support (MySQL, PostgreSQL, Spark SQL, BigQuery, etc.)

Configuration
=============

.. testcode::

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.sql_dialect = "mysql"  # or "postgres", "spark", "bigquery"
    ctx.sql_use_datafusion = True  # Use DataFusion optimizer (default)

API Reference
=============

.. toctree::
    :maxdepth: 1

    api/sql 