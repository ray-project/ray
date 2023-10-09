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

SQL Databases
-------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   read_sql

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
   Dataset.write_datasource
   Datasource
   ReadTask
   datasource.Reader

Partitioning API
----------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.Partitioning
   datasource.PartitionStyle
   datasource.PathPartitionParser
   datasource.PathPartitionFilter
   datasource.FileExtensionFilter

.. _metadata_provider:

MetadataProvider API
--------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.FileMetadataProvider
   datasource.BaseFileMetadataProvider
   datasource.ParquetMetadataProvider
   datasource.DefaultFileMetadataProvider
   datasource.DefaultParquetMetadataProvider
   datasource.FastFileMetadataProvider


.. _block_write_path_provider:

BlockWritePathProvider API
--------------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   datasource.BlockWritePathProvider
   datasource.DefaultBlockWritePathProvider
   
