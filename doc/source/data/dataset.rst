Ray Datasets
============

.. tip::

  Ray Datasets is available in early preview at ``ray.experimental.data``.

Ray Datasets are the standard way to load and exchange data in Ray libraries and applications. Datasets provide basic distributed data transformations such as ``map``, ``filter``, and ``repartition``, and are compatible with a variety of file formats, datasources, and distributed frameworks.

.. image:: dataset.svg

..
  https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit

Concepts
--------
Ray Datasets implement `"Distributed Arrow" <https://arrow.apache.org/>`__. A dataset consists of a list of Ray object references to *blocks*. Each block holds a set of items in either Arrow table format or in a Python list (for Arrow incompatible objects). Splitting the dataset into blocks allows for parallel transformation and ingest of the data.

The following figure visualizes a dataset that has three Arrow table blocks, each block holding 1000 rows each:

.. image:: dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Since a Ray Dataset is just a list of Ray object references, it can be freely passed between Ray tasks, actors, and libraries like any other object reference. This flexibility is a unique characteristic of Ray Datasets.

Compared to `Spark RDDs <https://spark.apache.org/docs/latest/rdd-programming-guide.html>`__ and `Dask Bags <https://docs.dask.org/en/latest/bag.html>`__, Datasets offers a more basic set of features, and executes operations eagerly for simplicity. It is intended that users cast Datasets into more featureful dataframe types (e.g., ``ds.to_dask()``) for advanced operations.

Datasource Compatibility Matrices
---------------------------------


.. list-table:: Input compatibility matrix
   :header-rows: 1

   * - Input Type
     - Read API
     - Status
   * - CSV File Format
     - ``ray.experimental.data.read_csv()``
     - ✅
   * - JSON File Format
     - ``ray.experimental.data.read_json()``
     - ✅
   * - Parquet File Format
     - ``ray.experimental.data.read_parquet()``
     - ✅
   * - Binary Files
     - ``ray.experimental.data.read_binary_files()``
     - ✅
   * - Custom Datasource
     - ``ray.experimental.data.read_datasource()``
     - ✅
   * - Spark Dataframe
     - ``ray.experimental.data.from_spark()``
     - (todo)
   * - Dask Dataframe
     - ``ray.experimental.data.from_dask()``
     - ✅
   * - Modin Dataframe
     - ``ray.experimental.data.from_modin()``
     - (todo)
   * - MARS Dataframe
     - ``ray.experimental.data.from_mars()``
     - (todo)
   * - Pandas Dataframe Objects
     - ``ray.experimental.data.from_pandas()``
     - ✅


.. list-table:: Output compatibility matrix
   :header-rows: 1

   * - Output Type
     - Dataset API
     - Status
   * - CSV File Format
     - ``ds.write_csv()``
     - ✅
   * - JSON File Format
     - ``ds.write_json()``
     - ✅
   * - Parquet File Format
     - ``ds.write_parquet()``
     - ✅
   * - Custom Datasource
     - ``ds.write_datasource()``
     - ✅
   * - Spark Dataframe
     - ``ds.to_spark()``
     - (todo)
   * - Dask Dataframe
     - ``ds.to_dask()``
     - ✅
   * - Modin Dataframe
     - ``ds.to_modin()``
     - (todo)
   * - MARS Dataframe
     - ``ds.to_mars()``
     - (todo)
   * - Arrow Table Objects
     - ``ds.to_arrow()``
     - ✅
   * - Arrow Table Iterator
     - ``ds.iter_batches(batch_format="pyarrow")``
     - ✅
   * - Pandas Dataframe Objects
     - ``ds.to_pandas()``
     - ✅
   * - Pandas Dataframe Iterator
     - ``ds.iter_batches(batch_format="pandas")``
     - ✅
   * - PyTorch Iterable Dataset
     - ``ds.to_torch()``
     - ✅
   * - TensorFlow Iterable Dataset
     - ``ds.to_tf()``
     - ✅


Creating Datasets
-----------------

talk about ray.experimental.data.read_*, from_<df>, read_datasource, range, items for testing

Example: ray.experimental.data.range(100) -> str(), count, schema, etc. inspection

Example: read parquet, basic transform, saving data

Transforming Datasets
---------------------

Saving Datasets
---------------

Consuming datasets
------------------

talk about split(), iter_batch/rows, to_torch(), to_tf(), to_<df>

Example: passing datasets between Ray tasks / actors

Example: pseudocode read features, split, send to training actors

Example: converting to/from a dask-on-ray dataset

Parallel transforms
-------------------

talk about map fn, repartition

Example: read binary files, map_batches with actors / GPU

Example: map_batches using pandas batch udf for efficiency

Custom datasources
------------------

talk about custom datasource examples

Pipelining data processing and ML computations
----------------------------------------------

This feature is planned for development. Please provide your input on this `GitHub RFC <https://github.com/ray-project/ray/issues/16852>`__.

Contributing
------------

Contributions to Datasets are welcome! There are many potential improvements, including:

- Supporting more datasources and transforms.
- Improving compatibility with ecosystem libraries.
- Adding features that require partitioning such as groupby() and join().
- Performance optimizations.

Get started with Ray Python development `here <https://docs.ray.io/en/master/development.html#python-develop>`__.
