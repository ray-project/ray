Ray Datasets API 
================

Creating Datasets
-----------------

.. autofunction:: ray.data.range
.. autofunction:: ray.data.range_table
.. autofunction:: ray.data.range_tensor
.. autofunction:: ray.data.read_csv
.. autofunction:: ray.data.read_json
.. autofunction:: ray.data.read_parquet
.. autofunction:: ray.data.read_parquet_bulk
.. autofunction:: ray.data.read_numpy
.. autofunction:: ray.data.read_text
.. autofunction:: ray.data.read_binary_files
.. autofunction:: ray.data.read_datasource
.. autofunction:: ray.data.from_items
.. autofunction:: ray.data.from_arrow
.. autofunction:: ray.data.from_arrow_refs
.. autofunction:: ray.data.from_huggingface
.. autofunction:: ray.data.from_spark
.. autofunction:: ray.data.from_dask
.. autofunction:: ray.data.from_modin
.. autofunction:: ray.data.from_mars
.. autofunction:: ray.data.from_pandas
.. autofunction:: ray.data.from_pandas_refs
.. autofunction:: ray.data.from_numpy
.. autofunction:: ray.data.from_numpy_refs


Built-in Datasources
--------------------

.. autoclass:: ray.data.datasource.BinaryDatasource
    :members:

.. autoclass:: ray.data.datasource.CSVDatasource
    :members:

.. autoclass:: ray.data.datasource.FileBasedDatasource
    :members:

.. autoclass:: ray.data.datasource.ImageFolderDatasource
    :members:

.. autoclass:: ray.data.datasource.JSONDatasource
    :members:

.. autoclass:: ray.data.datasource.NumpyDatasource
    :members:

.. autoclass:: ray.data.datasource.ParquetDatasource
    :members:

.. autoclass:: ray.data.datasource.RangeDatasource
    :members:

.. autoclass:: ray.data.datasource.SimpleTensorFlowDatasource
    :members:

.. autoclass:: ray.data.datasource.SimpleTorchDatasource
    :members: