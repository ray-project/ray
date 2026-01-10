.. _loading-data-api:

Loading Data API
================

.. currentmodule:: ray.data

Synthetic Data
--------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   range
   range_tensor

Python Objects
--------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_items

Parquet
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_parquet

CSV
---

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_csv

JSON
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_json

Text
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_text

Audio
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_audio

Avro
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_avro

Images
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_images

Binary
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_binary_files

TFRecords
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_tfrecords
   TFXReadOptions

Pandas
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_pandas
   from_pandas_refs

NumPy
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_numpy
   from_numpy
   from_numpy_refs

Arrow
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_arrow
   from_arrow_refs

MongoDB
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_mongo

BigQuery
--------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_bigquery

SQL Databases
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_sql

Databricks
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_databricks_tables

Snowflake
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_snowflake

Unity Catalog
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_unity_catalog

Delta Sharing
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_delta_sharing_tables

Hudi
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_hudi

Iceberg
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_iceberg

Delta Lake
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_delta

Lance
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_lance

MCAP (Message Capture)
----------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_mcap

ClickHouse
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_clickhouse

Daft
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_daft

Dask
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_dask

Spark
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_spark

Modin
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_modin

Mars
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_mars

Torch
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_torch

Hugging Face
------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_huggingface

TensorFlow
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_tf

Video
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_videos

WebDataset
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_webdataset

Kafka
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_kafka

.. _data_source_api:

Datasource API
--------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_datasource
   Datasource
   ReadTask
   datasource.FilenameProvider

Partitioning API
----------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.Partitioning
   datasource.PartitionStyle
   datasource.PathPartitionParser
   datasource.PathPartitionFilter

.. _metadata_provider:

MetadataProvider API
--------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.FileMetadataProvider
   datasource.BaseFileMetadataProvider
   datasource.DefaultFileMetadataProvider

Shuffling API
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   FileShuffleConfig
