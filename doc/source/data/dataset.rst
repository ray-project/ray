Datasets: Distributed Arrow on Ray
==================================

.. tip::

  Ray Datasets is available in early preview at ``ray.experimental.data``.

Ray Datasets are the standard way to load and exchange data in Ray libraries and applications. Datasets provide basic distributed data transformations such as ``map``, ``filter``, and ``repartition``, and are compatible with a variety of file formats, datasources, and distributed frameworks.

.. image:: dataset.svg

..
  https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit

Concepts
--------
Ray Datasets implement `"Distributed Arrow" <https://arrow.apache.org/>`__. A Dataset consists of a list of Ray object references to *blocks*. Each block holds a set of items in either Arrow table format or in a Python list (for Arrow incompatible objects). Splitting the dataset into blocks allows for parallel transformation and ingest of the data.

The following figure visualizes a Dataset that has three Arrow table blocks, each block holding 1000 rows each:

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
     - ``ray.data.read_csv()``
     - ✅
   * - JSON File Format
     - ``ray.data.read_json()``
     - ✅
   * - Parquet File Format
     - ``ray.data.read_parquet()``
     - ✅
   * - Binary Files
     - ``ray.data.read_binary_files()``
     - ✅
   * - Custom Datasource
     - ``ray.data.read_datasource()``
     - ✅
   * - Spark Dataframe
     - ``ray.data.from_spark()``
     - (todo)
   * - Dask Dataframe
     - ``ray.data.from_dask()``
     - ✅
   * - Modin Dataframe
     - ``ray.data.from_modin()``
     - (todo)
   * - MARS Dataframe
     - ``ray.data.from_mars()``
     - (todo)
   * - Pandas Dataframe Objects
     - ``ray.data.from_pandas()``
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

Get started by creating Datasets from synthetic data using ``ray.data.range()`` and ``ray.data.from_items()``. Datasets can hold either plain Python objects (schema is a Python type), or Arrow records (schema is Arrow).

.. code-block:: python

    # Create a Dataset of Python objects.
    ds = ray.data.range(10000)
    # -> Dataset(num_rows=10000, num_blocks=200, schema=<class 'int'>)

    ds.take(5)
    # -> [0, 1, 2, 3, 4]

    ds.count())
    # -> 100

    # Create a Dataset of Arrow records.
    ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
    # -> Dataset(num_rows=10000, num_blocks=200, schema={col1: int64, col2: string})

    ds.show(2)
    # -> ArrowRow({'col1': 0, 'col2': '0'})
    # -> ArrowRow({'col1': 1, 'col2': '1'})

    ds.schema()
    # -> col1: int64
    # -> col2: string

Datasets can also be created from files on local disk or remote datasources such as S3. Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__ can be used to specify file locations:

.. code-block:: python

    # Read a directory of files in remote storage.
    ds = ray.data.read_csv("s3://bucket/path")

    # Read multiple local files.
    ds = ray.data.read_csv(["/path/to/file1", "/path/to/file2"])

    # Read multiple directories.
    ds = ray.data.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

Finally, you can create a Dataset from an existing data in the Ray object store or Ray compatible distributed DataFrames:

.. code-block:: python

    # Create a Dataset from a list of Pandas DataFrame objects.
    pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.experimental.data.from_pandas([ray.put(pdf)])

    # Create a Dataset from a Dask-on-Ray DataFrame.
    dask_df = dd.from_pandas(pdf, npartitions=10)
    ds = ray.data.from_dask(dask_df)


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
