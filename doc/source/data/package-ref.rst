.. _data_api:

Ray Datasets API
================

Creating Datasets
-----------------

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
.. autofunction:: ray.data.from_arrow_refs
.. autofunction:: ray.data.from_spark
.. autofunction:: ray.data.from_dask
.. autofunction:: ray.data.from_modin
.. autofunction:: ray.data.from_mars
.. autofunction:: ray.data.from_pandas
.. autofunction:: ray.data.from_pandas_refs
.. autofunction:: ray.data.from_numpy

.. _dataset-api:

Dataset API
-----------

.. autoclass:: ray.data.Dataset
    :members:

.. _dataset-pipeline-api:

DatasetPipeline API
-------------------

.. autoclass:: ray.data.dataset_pipeline.DatasetPipeline
    :members:

GroupedDataset API
------------------

.. autoclass:: ray.data.grouped_dataset.GroupedDataset
    :members:

RandomAccessDataset API
-----------------------

.. autoclass:: ray.data.random_access_dataset.RandomAccessDataset
    :members:

Tensor Column Extension API
---------------------------

.. autoclass:: ray.data.extensions.tensor_extension.TensorDtype
    :members:

.. autoclass:: ray.data.extensions.tensor_extension.TensorArray
    :members:

.. autoclass:: ray.data.extensions.tensor_extension.ArrowTensorType
    :members:

.. autoclass:: ray.data.extensions.tensor_extension.ArrowTensorArray
    :members:

Custom Datasource API
---------------------

.. autoclass:: ray.data.Datasource
    :members:

.. autoclass:: ray.data.ReadTask
    :members:

Table Row API
---------------------

.. autoclass:: ray.data.row.TableRow
    :members:

Utility
-------
.. autofunction:: ray.data.set_progress_bars
