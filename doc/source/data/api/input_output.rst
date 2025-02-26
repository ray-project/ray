.. _input-output:

Input/Output
============

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
   read_parquet_bulk
   Dataset.write_parquet

CSV
---

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_csv
   Dataset.write_csv

JSON
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_json
   Dataset.write_json

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
   Dataset.write_images

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
   Dataset.write_tfrecords
   TFXReadOptions

Pandas
------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_pandas
   from_pandas_refs
   Dataset.to_pandas
   Dataset.to_pandas_refs

NumPy
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_numpy
   from_numpy
   from_numpy_refs
   Dataset.write_numpy
   Dataset.to_numpy_refs

Arrow
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_arrow
   from_arrow_refs
   Dataset.to_arrow_refs

MongoDB
-------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_mongo
   Dataset.write_mongo

BigQuery
--------

.. autosummary::
   :toctree: doc/

   read_bigquery
   Dataset.write_bigquery

SQL Databases
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_sql
   Dataset.write_sql

Databricks
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_databricks_tables

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
   Dataset.write_iceberg

Lance
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_lance
   Dataset.write_lance

ClickHouse
----------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_clickhouse

Dask
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_dask
   Dataset.to_dask

Spark
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_spark
   Dataset.to_spark

Modin
-----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_modin
   Dataset.to_modin

Mars
----

.. autosummary::
   :nosignatures:
   :toctree: doc/

   from_mars
   Dataset.to_mars

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
   datasource.ParquetMetadataProvider
   datasource.FastFileMetadataProvider

Shuffling API
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   FileShuffleConfig
