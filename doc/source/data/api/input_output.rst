.. _input-output:

Input/Output
============

.. currentmodule:: ray.data

Synthetic Data
--------------

.. autosummary::
   :toctree: doc/

   range
   range_tensor

Python Objects
--------------

.. autosummary::
   :toctree: doc/

   from_items

Parquet
-------

.. autosummary::
   :toctree: doc/

   read_parquet
   read_parquet_bulk
   Datastream.write_parquet

CSV
---

.. autosummary::
   :toctree: doc/

   read_csv
   Datastream.write_csv

JSON
----

.. autosummary::
   :toctree: doc/

   read_json
   Datastream.write_json

Text
----

.. autosummary::
   :toctree: doc/

   read_text

Images
------

.. autosummary::
   :toctree: doc/

   read_images

Binary
------

.. autosummary::
   :toctree: doc/

   read_binary_files

TFRecords
---------

.. autosummary::
   :toctree: doc/

   read_tfrecords
   Datastream.write_tfrecords


Pandas
------

.. autosummary::
   :toctree: doc/

   from_pandas
   from_pandas_refs
   Datastream.to_pandas
   Datastream.to_pandas_refs

NumPy
-----

.. autosummary::
   :toctree: doc/

   read_numpy
   from_numpy
   from_numpy_refs
   Datastream.write_numpy
   Datastream.to_numpy_refs

Arrow
-----

.. autosummary::
   :toctree: doc/

   from_arrow
   from_arrow_refs
   Datastream.to_arrow_refs

MongoDB
-------

.. autosummary::
   :toctree: doc/

   read_mongo
   Datastream.write_mongo

SQL Databases
-------------

.. autosummary::
   :toctree: doc/

   read_sql
   
Dask
----

.. autosummary::
   :toctree: doc/

   from_dask
   Datastream.to_dask

Spark
-----

.. autosummary::
   :toctree: doc/

   from_spark
   Datastream.to_spark

Modin
-----

.. autosummary::
   :toctree: doc/

   from_modin
   Datastream.to_modin

Mars
----

.. autosummary::
   :toctree: doc/

   from_mars
   Datastream.to_mars

Torch
-----

.. autosummary::
   :toctree: doc/

   from_torch

Hugging Face
------------

.. autosummary::
   :toctree: doc/

   from_huggingface

TensorFlow
----------

.. autosummary::
   :toctree: doc/

   from_tf

WebDataset
----------

.. autosummary::
   :toctree: doc/

   read_webdataset

.. _data_source_api:

Datasource API
--------------

.. autosummary::
   :toctree: doc/

   read_datasource
   Datastream.write_datasource
   Datasource
   ReadTask
   datasource.Reader


Built-in Datasources
####################

.. autosummary::
   :toctree: doc/

   datasource.BinaryDatasource
   datasource.CSVDatasource
   datasource.FileBasedDatasource
   datasource.ImageDatasource
   datasource.JSONDatasource
   datasource.NumpyDatasource
   datasource.ParquetDatasource
   datasource.RangeDatasource
   datasource.TFRecordDatasource
   datasource.MongoDatasource
   datasource.WebDatasetDatasource

Partitioning API
----------------

.. autosummary::
   :toctree: doc/

   datasource.Partitioning
   datasource.PartitionStyle
   datasource.PathPartitionEncoder
   datasource.PathPartitionParser
   datasource.PathPartitionFilter

MetadataProvider API
--------------------

.. autosummary::
   :toctree: doc/

   datasource.FileMetadataProvider
   datasource.BaseFileMetadataProvider
   datasource.ParquetMetadataProvider
   datasource.DefaultFileMetadataProvider
   datasource.DefaultParquetMetadataProvider
   datasource.FastFileMetadataProvider
