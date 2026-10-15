.. _saving-data-api:

Saving Data API
===============

.. currentmodule:: ray.data

Public APIs
-----------

BigQuery
^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_bigquery

CSV
^^^

.. autosummary::
   :nosignatures:

   Dataset.write_csv

ClickHouse
^^^^^^^^^^

.. deprecated::
   ``write_clickhouse`` is deprecated. Use the `ray-clickhouse <https://github.com/jiangxt2/ray-clickhouse>`_ package instead.

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_clickhouse

Daft
^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_daft

Dask
^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_dask

Iceberg
^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_iceberg

Images
^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_images

JSON
^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_json

Lance
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_lance

Mars
^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_mars

Modin
^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_modin

MongoDB
^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_mongo

NumPy
^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_numpy

ORC
^^^

.. autosummary::
   :nosignatures:

   Dataset.write_orc

Pandas
^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_pandas

Parquet
^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_parquet

SQL Databases
^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_sql

Snowflake
^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Dataset.write_snowflake

Spark
^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_spark

TFRecords
^^^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.write_tfrecords

Developer APIs
--------------

Arrow refs
^^^^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_arrow_refs

Datasink API
^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Datasink
   Dataset.write_datasink
   datasource.RowBasedFileDatasink
   datasource.BlockBasedFileDatasink
   datasource.WriteResult
   datasource.WriteReturnType

FilenameProvider
^^^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.FilenameProvider

NumPy refs
^^^^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_numpy_refs

Pandas refs
^^^^^^^^^^^

.. autosummary::
   :nosignatures:

   Dataset.to_pandas_refs
