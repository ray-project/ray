.. _input-output:

Input/Output
============

Synthetic Data
--------------

.. autofunction:: ray.data.range

.. autofunction:: ray.data.range_table

.. autofunction:: ray.data.range_tensor

Python Objects
--------------

.. autofunction:: ray.data.from_items

Parquet
-------

.. autofunction:: ray.data.read_parquet

.. autofunction:: ray.data.read_parquet_bulk

.. automethod:: ray.data.Dataset.write_parquet
    :noindex:

CSV
---

.. autofunction:: ray.data.read_csv

.. automethod:: ray.data.Dataset.write_csv
    :noindex:

JSON
----

.. autofunction:: ray.data.read_json

.. automethod:: ray.data.Dataset.write_json
    :noindex:

Text
----

.. autofunction:: ray.data.read_text

Images (experimental)
---------------------

.. autofunction:: ray.data.read_images

Binary
------

.. autofunction:: ray.data.read_binary_files

TFRecords
---------

.. autofunction:: ray.data.read_tfrecords

.. autofunction:: ray.data.Dataset.write_tfrecords
    :noindex:


Pandas
------

.. autofunction:: ray.data.from_pandas

.. autofunction:: ray.data.from_pandas_refs

.. automethod:: ray.data.Dataset.to_pandas
    :noindex:

.. automethod:: ray.data.Dataset.to_pandas_refs
    :noindex:

NumPy
-----

.. autofunction:: ray.data.read_numpy

.. autofunction:: ray.data.from_numpy

.. autofunction:: ray.data.from_numpy_refs

.. automethod:: ray.data.Dataset.write_numpy
    :noindex:

.. automethod:: ray.data.Dataset.to_numpy_refs
    :noindex:

Arrow
-----

.. autofunction:: ray.data.from_arrow

.. autofunction:: ray.data.from_arrow_refs

.. automethod:: ray.data.Dataset.to_arrow_refs
    :noindex:

Dask
----

.. autofunction:: ray.data.from_dask

.. automethod:: ray.data.Dataset.to_dask
    :noindex:

Spark
-----

.. autofunction:: ray.data.from_spark

.. automethod:: ray.data.Dataset.to_spark
    :noindex:

Modin
-----

.. autofunction:: ray.data.from_modin

.. automethod:: ray.data.Dataset.to_modin
    :noindex:

Mars
----

.. autofunction:: ray.data.from_mars

.. automethod:: ray.data.Dataset.to_mars
    :noindex:

Torch
-----

.. autofunction:: ray.data.from_torch

HuggingFace
------------

.. autofunction:: ray.data.from_huggingface

TensorFlow
----------

.. autofunction:: ray.data.from_tf


.. _data_source_api:

Datasource API
--------------

.. autofunction:: ray.data.read_datasource

.. automethod:: ray.data.Dataset.write_datasource
    :noindex:

.. autoclass:: ray.data.Datasource
    :members:

.. autoclass:: ray.data.ReadTask
    :members:

.. autoclass:: ray.data.datasource.Reader
    :members:


Built-in Datasources
####################

.. autoclass:: ray.data.datasource.BinaryDatasource
    :members:

.. autoclass:: ray.data.datasource.CSVDatasource
    :members:

.. autoclass:: ray.data.datasource.FileBasedDatasource
    :members:

.. autoclass:: ray.data.datasource.ImageDatasource
    :members:

.. autoclass:: ray.data.datasource.JSONDatasource
    :members:

.. autoclass:: ray.data.datasource.NumpyDatasource
    :members:

.. autoclass:: ray.data.datasource.ParquetDatasource
    :members:

.. autoclass:: ray.data.datasource.RangeDatasource
    :members:

.. autoclass:: ray.data.datasource.TFRecordDatasource
    :members:

Partitioning API
----------------

.. autoclass:: ray.data.datasource.Partitioning
    :members:

.. autoclass:: ray.data.datasource.PartitionStyle
    :members:

.. autoclass:: ray.data.datasource.PathPartitionEncoder
    :members:

.. autoclass:: ray.data.datasource.PathPartitionParser
    :members:

.. autoclass:: ray.data.datasource.PathPartitionFilter


MetadataProvider API
--------------------

.. autoclass:: ray.data.datasource.FileMetadataProvider
    :members:


.. autoclass:: ray.data.datasource.BaseFileMetadataProvider
    :members:

.. autoclass:: ray.data.datasource.ParquetMetadataProvider
    :members:

.. autoclass:: ray.data.datasource.DefaultFileMetadataProvider
    :members:

.. autoclass:: ray.data.datasource.DefaultParquetMetadataProvider
    :members:

.. autoclass:: ray.data.datasource.FastFileMetadataProvider
    :members:
