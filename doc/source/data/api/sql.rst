.. _data_sql_api:

=================
Ray Data SQL API
=================

.. warning::
    Ray Data SQL is currently experimental and APIs are subject to change.

This page provides the API reference for Ray Data SQL functions and classes.

Query Execution
===============

.. currentmodule:: ray.data

.. autosummary::
   :nosignatures:
   :toctree: doc/

   sql
   register_table  
   list_tables
   get_schema
   clear_tables

Core Classes
============

.. autosummary::
   :nosignatures:
   :toctree: doc/

   sql.RaySQL
   sql.SQLConfig
   sql.LogLevel

Engine Management
=================

.. autosummary::
   :nosignatures:
   :toctree: doc/

   sql.get_engine
   sql.get_registry

Query Execution Functions
=========================

sql
---

.. autofunction:: ray.data.sql

register_table
--------------

.. autofunction:: ray.data.register_table

list_tables
-----------

.. autofunction:: ray.data.sql.list_tables

get_schema
----------

.. autofunction:: ray.data.sql.get_schema

clear_tables
------------

.. autofunction:: ray.data.sql.clear_tables

Core Classes
============

RaySQL
------

.. autoclass:: ray.data.sql.RaySQL
   :members:
   :special-members: __init__

SQLConfig  
---------

.. autoclass:: ray.data.sql.SQLConfig
   :members:
   :special-members: __init__

LogLevel
--------

.. autoclass:: ray.data.sql.LogLevel
   :members:

Engine Management Functions
===========================

get_engine
----------

.. autofunction:: ray.data.sql.get_engine

get_registry
------------

.. autofunction:: ray.data.sql.get_registry 