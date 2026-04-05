.. _loading-data-api:

Loading Data API
================

.. currentmodule:: ray.data

Public APIs
-----------

Arrow
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_arrow

Audio
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_audio

Avro
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_avro

BigQuery
^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_bigquery

Binary
^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_binary_files

CSV
^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_csv

ClickHouse
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_clickhouse

Daft
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_daft

Dask
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_dask

Databricks
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_databricks_tables

Delta Lake
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_delta

Delta Sharing
^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_delta_sharing_tables

Hudi
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_hudi

Hugging Face
^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_huggingface

Iceberg
^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_iceberg

Images
^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_images

JSON
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_json

Kafka
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_kafka

Lance
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_lance

MCAP (Message Capture)
^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_mcap

Mars
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_mars

Modin
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_modin

MongoDB
^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_mongo

NumPy
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_numpy
   read_numpy

Pandas
^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_pandas

Parquet
^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_parquet

Python Objects
^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_items

SQL Databases
^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_sql

Snowflake
^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_snowflake

Spark
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_spark

Synthetic Data
^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   range
   range_tensor

TFRecords
^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_tfrecords
   TFXReadOptions

TensorFlow
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_tf

Text
^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_text

Torch
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_torch

Unity Catalog
^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_unity_catalog

Video
^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_videos

WebDataset
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_webdataset

Partitioning API
^^^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.Partitioning
   datasource.PartitionStyle
   datasource.PathPartitionFilter
   datasource.PathPartitionParser

Shuffling API
^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   FileShuffleConfig

Developer APIs
--------------

Arrow refs
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_arrow_refs

Datasource API
^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   Datasource
   datasource.FileBasedDatasource
   read_datasource
   ReadTask

.. _metadata_provider:

MetadataProvider API
^^^^^^^^^^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.BaseFileMetadataProvider
   datasource.DefaultFileMetadataProvider
   datasource.FileMetadataProvider

NumPy refs
^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_numpy_refs

Pandas refs
^^^^^^^^^^^

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_pandas_refs
