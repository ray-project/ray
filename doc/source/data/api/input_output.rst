.. _input-output:

Input/Output
============

.. currentmodule:: ray.data

Synthetic Data
--------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   range
   range_table
   range_tensor

Python Objects
--------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_items

Parquet
-------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_parquet
   read_parquet_bulk
   Dataset.write_parquet

CSV
---

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_csv
   Dataset.write_csv

JSON
----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_json
   Dataset.write_json

Text
----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_text

Images
------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_images

Binary
------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_binary_files

TFRecords
---------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_tfrecords
   Dataset.write_tfrecords


Pandas
------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_pandas
   from_pandas_refs
   Dataset.to_pandas
   Dataset.to_pandas_refs

NumPy
-----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_numpy
   from_numpy
   from_numpy_refs
   Dataset.write_numpy
   Dataset.to_numpy_refs

Arrow
-----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_arrow
   from_arrow_refs
   Dataset.to_arrow_refs

MongoDB
-------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_mongo
   Dataset.write_mongo

Dask
----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_dask
   Dataset.to_dask

Spark
-----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_spark
   Dataset.to_spark

Modin
-----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_modin
   Dataset.to_modin

Mars
----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_mars
   Dataset.to_mars

Torch
-----

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_torch

HuggingFace
-----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_huggingface

TensorFlow
----------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   from_tf

.. _data_source_api:

Datasource API
--------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   read_datasource
   Dataset.write_datasource
   Datasource
   ReadTask
   datasource.Reader


Built-in Datasources
####################

.. autosummary::
   :toctree: doc/
   :nosignatures:

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

Partitioning API
----------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   datasource.Partitioning
   datasource.PartitionStyle
   datasource.PathPartitionEncoder
   datasource.PathPartitionParser
   datasource.PathPartitionFilter

MetadataProvider API
--------------------

.. autosummary::
   :toctree: doc/
   :nosignatures:

   datasource.FileMetadataProvider
   datasource.BaseFileMetadataProvider
   datasource.ParquetMetadataProvider
   datasource.DefaultFileMetadataProvider
   datasource.DefaultParquetMetadataProvider
   datasource.FastFileMetadataProvider
