Datasets API Reference
======================

Creating a Dataset
------------------
.. autofunction:: ray.data.range
.. autofunction:: ray.data.range_arrow
.. autofunction:: ray.data.read_csv
.. autofunction:: ray.data.read_json
.. autofunction:: ray.data.read_parquet
.. autofunction:: ray.data.read_binary_files
.. autofunction:: ray.data.read_datasource
.. autofunction:: ray.data.from_spark
.. autofunction:: ray.data.from_dask
.. autofunction:: ray.data.from_modin
.. autofunction:: ray.data.from_mars
.. autofunction:: ray.data.from_pandas

Dataset API
-----------

.. autoclass:: ray.data.Dataset
    :members:

Custom Datasource API
---------------------

.. autoclass:: ray.data.Datasource
    :members:

.. autoclass:: ray.data.ReadTask
    :members:

.. autoclass:: ray.data.WriteTask
    :members:
