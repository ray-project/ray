.. _saving-data-api:

Saving Data API
===============

.. currentmodule:: ray.data

Parquet
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_parquet

CSV
---

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_csv

JSON
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_json

Images
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_images

TFRecords
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_tfrecords

Pandas
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_pandas
   Dataset.to_pandas_refs

NumPy
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_numpy
   Dataset.to_numpy_refs

Arrow
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_arrow_refs

MongoDB
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_mongo

BigQuery
--------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_bigquery

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

Iceberg
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_iceberg

Lance
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_lance

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

Spark
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_spark

Modin
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_modin

Mars
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.to_mars

Datasink API
------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_datasink
   Datasink
   datasource.RowBasedFileDatasink
   datasource.BlockBasedFileDatasink
   datasource.FileBasedDatasource
   datasource.WriteResult
   datasource.WriteReturnType
