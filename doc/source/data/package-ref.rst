Dataset API Reference
=====================

Creating a Dataset
------------------
.. autofunction:: ray.data.range
.. autofunction:: ray.data.range_arrow
.. autofunction:: ray.data.range_tensor
.. autofunction:: ray.data.read_csv
.. autofunction:: ray.data.read_json
.. autofunction:: ray.data.read_parquet
.. autofunction:: ray.data.read_numpy
.. autofunction:: ray.data.read_text
.. autofunction:: ray.data.read_binary_files
.. autofunction:: ray.data.read_datasource
.. autofunction:: ray.data.from_items
.. autofunction:: ray.data.from_arrow
.. autofunction:: ray.data.from_spark
.. autofunction:: ray.data.from_dask
.. autofunction:: ray.data.from_modin
.. autofunction:: ray.data.from_mars
.. autofunction:: ray.data.from_pandas
.. autofunction:: ray.data.from_numpy

Dataset API
-----------

.. autoclass:: ray.data.Dataset
    :members:

DatasetPipeline API
-------------------

.. autoclass:: ray.data.dataset_pipeline.DatasetPipeline
    :members:

Custom Datasource API
---------------------

.. autoclass:: ray.data.Datasource
    :members:

.. autoclass:: ray.data.ReadTask
    :members:

Utility
-------
.. autofunction:: ray.data.set_progress_bars
