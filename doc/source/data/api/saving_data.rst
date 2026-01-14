.. _saving-data-api:

Saving Data API
===============

.. currentmodule:: ray.data

.. test comment for minimal PR

Public APIs
-----------

Arrow
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_arrow_refs

BigQuery
--------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_bigquery

CSV
---

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_csv

ClickHouse
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_clickhouse

Daft
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_daft

Dask
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_dask

Iceberg
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_iceberg

Images
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_images

JSON
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_json

Lance
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_lance

Mars
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_mars

Modin
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_modin

MongoDB
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_mongo

NumPy
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_numpy
   Dataset.to_numpy_refs

Pandas
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_pandas
   Dataset.to_pandas_refs

Parquet
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_parquet

SQL Databases
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_sql

Snowflake
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_snowflake

Spark
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_spark

TFRecords
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_tfrecords

Developer APIs
--------------

Datasink API
------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Datasink
   Dataset.write_datasink
   datasource.RowBasedFileDatasink
   datasource.BlockBasedFileDatasink
   datasource.WriteResult
   datasource.WriteReturnType

Datasource API
--------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.FilenameProvider

