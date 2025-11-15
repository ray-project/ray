.. _data_sql_api:

=================
Ray Data SQL API
=================

.. warning::
    Ray Data SQL is experimental and APIs are subject to change.

.. currentmodule:: ray.data.sql

.. autosummary::
   :nosignatures:
   :toctree: doc/

   sql
   register
   list_tables
   clear_tables

sql
~~~

Execute SQL queries on Ray Datasets with automatic table discovery.

.. autofunction:: sql

register
~~~~~~~~

Register a dataset as a SQL table for query execution.

.. autofunction:: register

list_tables
~~~~~~~~~~~

List all currently registered SQL tables.

.. autofunction:: list_tables

clear_tables
~~~~~~~~~~~~

Clear all registered SQL tables from memory.

.. autofunction:: clear_tables 