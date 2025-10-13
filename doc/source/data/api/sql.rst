.. _data_sql_api:

=================
Ray Data SQL API
=================

.. warning::
    Ray Data SQL is experimental and APIs are subject to change.

Overview
========

The Ray Data SQL API provides SQL query execution capabilities for Ray Datasets. You can execute SQL queries directly on Ray Datasets using familiar SQL syntax, with queries automatically converted to native Ray Data operations for distributed execution.

The SQL API supports standard SQL operations including:

- SELECT queries with filtering and column selection
- JOIN operations (INNER, LEFT, RIGHT, FULL OUTER)
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY clauses with aggregations
- ORDER BY for sorting results
- LIMIT and OFFSET for pagination

.. note::
   The SQL API converts SQL queries into native Ray Data operations like ``filter()``, ``join()``, ``groupby().aggregate()``, and ``sort()`` for maximum performance and compatibility with Ray's distributed execution model.

Common use cases
================

Use the SQL API when you need to:

- Query Ray Datasets using familiar SQL syntax
- Join multiple datasets using SQL JOIN clauses
- Perform complex filtering and aggregations with SQL
- Prototype data pipelines with SQL before converting to Ray Data API
- Integrate with SQL-based tools or workflows

API reference
=============

.. currentmodule:: ray.data

.. autosummary::
   :nosignatures:
   :toctree: doc/

   sql
   register  
   list_tables
   clear_tables

Query execution
---------------

sql
~~~

Execute SQL queries on Ray Datasets with automatic table discovery.

.. autofunction:: ray.data.sql

Table management
----------------

register
~~~~~~~~

Register a dataset as a SQL table for query execution.

.. autofunction:: ray.data.register

list_tables
~~~~~~~~~~~

List all currently registered SQL tables.

.. autofunction:: ray.data.list_tables

clear_tables
~~~~~~~~~~~~

Clear all registered SQL tables from memory.

.. autofunction:: ray.data.clear_tables 