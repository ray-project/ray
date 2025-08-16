.. _data_sql_api:

=================
Ray Data SQL API
=================

This page provides the API reference for Ray Data SQL functions and classes.

Query Execution
===============

.. currentmodule:: ray.data.sql

.. autosummary::
   :toctree: doc/

   sql
   register_table  
   list_tables
   get_schema
   clear_tables

Core Classes
============

.. autosummary::
   :toctree: doc/

   RaySQL
   SQLConfig
   LogLevel
   QueryResult

Engine Management
=================

.. autosummary::
   :toctree: doc/

   get_engine
   get_registry

Testing and Examples
====================

.. autosummary::
   :toctree: doc/

   run_comprehensive_tests
   example_usage
   example_sqlglot_features

Query Execution Functions
=========================

sql
---

.. autofunction:: sql

register_table
--------------

.. autofunction:: register_table

list_tables
-----------

.. autofunction:: list_tables

get_schema
----------

.. autofunction:: get_schema

clear_tables
------------

.. autofunction:: clear_tables

Core Classes
============

RaySQL
------

.. autoclass:: RaySQL
   :members:
   :special-members: __init__

SQLConfig  
---------

.. autoclass:: SQLConfig
   :members:
   :special-members: __init__

LogLevel
--------

.. autoclass:: LogLevel
   :members:

QueryResult
-----------

.. autoclass:: QueryResult
   :members:
   :special-members: __init__

Engine Management Functions
===========================

get_engine
----------

.. autofunction:: get_engine

get_registry
------------

.. autofunction:: get_registry

Testing and Example Functions
=============================

run_comprehensive_tests
-----------------------

.. autofunction:: run_comprehensive_tests

example_usage
-------------

.. autofunction:: example_usage

example_sqlglot_features
-----------------------

.. autofunction:: example_sqlglot_features 