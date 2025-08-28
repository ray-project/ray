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

Engine Management
=================

.. autosummary::
   :toctree: doc/

   get_engine
   get_registry

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

Engine Management Functions
===========================

get_engine
----------

.. autofunction:: get_engine

get_registry
------------

.. autofunction:: get_registry 