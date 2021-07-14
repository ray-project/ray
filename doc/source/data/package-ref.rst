Ray Dataset API Reference
=========================

Creating a Dataset
------------------
.. autofunction:: ray.experimental.data.range
.. autofunction:: ray.experimental.data.range_arrow
.. autofunction:: ray.experimental.data.read_csv
.. autofunction:: ray.experimental.data.read_json
.. autofunction:: ray.experimental.data.read_parquet
.. autofunction:: ray.experimental.data.read_binary_files
.. autofunction:: ray.experimental.data.read_datasource
.. autofunction:: ray.experimental.data.from_spark
.. autofunction:: ray.experimental.data.from_dask
.. autofunction:: ray.experimental.data.from_modin
.. autofunction:: ray.experimental.data.from_mars
.. autofunction:: ray.experimental.data.from_pandas

Dataset API
-----------

.. autoclass:: ray.experimental.data.Dataset
    :members:

Custom Datasource API
---------------------

.. autoclass:: ray.experimental.data.Datasource
    :members:

.. autoclass:: ray.experimental.data.ReadTask
    :members:

.. autoclass:: ray.experimental.data.WriteTask
    :members:
