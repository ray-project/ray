.. _datasets:

Datasets: Flexible Distributed Data Loading
===========================================

.. tip::

  Datasets is available as **alpha** in Ray 1.7+ (experimental release available in Ray 1.6). Please file feature requests and bug reports on GitHub Issues or join the discussion on the `Ray Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`__.

Ray Datasets are the standard way to load and exchange data in Ray libraries and applications. Datasets provide basic distributed data transformations such as ``map``, ``filter``, and ``repartition``, and are compatible with a variety of file formats, datasources, and distributed frameworks.

.. image:: dataset.svg

..
  https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit

Concepts
--------
Ray Datasets implement `Distributed Arrow <https://arrow.apache.org/>`__. A Dataset consists of a list of Ray object references to *blocks*. Each block holds a set of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__, `Arrow tensor <https://arrow.apache.org/docs/python/generated/pyarrow.Tensor.html>`__, or a Python list (for Arrow incompatible objects). Having multiple blocks in a dataset allows for parallel transformation and ingest of the data.

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
   * - Numpy File Format
     - ``ray.data.read_numpy()``
     - ✅
   * - Text Files
     - ``ray.data.read_text()``
     - ✅
   * - Binary Files
     - ``ray.data.read_binary_files()``
     - ✅
   * - Python Objects
     - ``ray.data.from_items()``
     - ✅
   * - Spark Dataframe
     - ``ray.data.from_spark()``
     - (todo)
   * - Dask Dataframe
     - ``ray.data.from_dask()``
     - ✅
   * - Modin Dataframe
     - ``ray.data.from_modin()``
     - ✅
   * - MARS Dataframe
     - ``ray.data.from_mars()``
     - (todo)
   * - Pandas Dataframe Objects
     - ``ray.data.from_pandas()``
     - ✅
   * - NumPy ndarray Objects
     - ``ray.data.from_numpy()``
     - ✅
   * - Arrow Table Objects
     - ``ray.data.from_arrow()``
     - ✅
   * - Custom Datasource
     - ``ray.data.read_datasource()``
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
   * - Numpy File Format
     - ``ds.write_numpy()``
     - ✅
   * - Spark Dataframe
     - ``ds.to_spark()``
     - (todo)
   * - Dask Dataframe
     - ``ds.to_dask()``
     - ✅
   * - Modin Dataframe
     - ``ds.to_modin()``
     - ✅
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
   * - NumPy ndarray Objects
     - ``ds.to_numpy()``
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
   * - Custom Datasource
     - ``ds.write_datasource()``
     - ✅


Creating Datasets
-----------------

Get started by creating Datasets from synthetic data using ``ray.data.range()`` and ``ray.data.from_items()``. Datasets can hold either plain Python objects (schema is a Python type), or Arrow records (schema is Arrow).

.. code-block:: python

    import ray
    
    # Create a Dataset of Python objects.
    ds = ray.data.range(10000)
    # -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

    ds.take(5)
    # -> [0, 1, 2, 3, 4]

    ds.count()
    # -> 10000

    # Create a Dataset of Arrow records.
    ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
    # -> Dataset(num_blocks=200, num_rows=10000, schema={col1: int64, col2: string})

    ds.show(5)
    # -> ArrowRow({'col1': 0, 'col2': '0'})
    # -> ArrowRow({'col1': 1, 'col2': '1'})
    # -> ArrowRow({'col1': 2, 'col2': '2'})
    # -> ArrowRow({'col1': 3, 'col2': '3'})
    # -> ArrowRow({'col1': 4, 'col2': '4'})

    ds.schema()
    # -> col1: int64
    # -> col2: string

Datasets can be created from files on local disk or remote datasources such as S3. Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__ can be used to specify file locations:

.. code-block:: python

    # Read a directory of files in remote storage.
    ds = ray.data.read_csv("s3://bucket/path")

    # Read multiple local files.
    ds = ray.data.read_csv(["/path/to/file1", "/path/to/file2"])

    # Read multiple directories.
    ds = ray.data.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

Finally, you can create a Dataset from existing data in the Ray object store or Ray compatible distributed DataFrames:

.. code-block:: python

    import pandas as pd
    import dask.dataframe as dd

    # Create a Dataset from a list of Pandas DataFrame objects.
    pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([ray.put(pdf)])

    # Create a Dataset from a Dask-on-Ray DataFrame.
    dask_df = dd.from_pandas(pdf, npartitions=10)
    ds = ray.data.from_dask(dask_df)

Saving Datasets
---------------

Datasets can be written to local or remote storage using ``.write_csv()``, ``.write_json()``, and ``.write_parquet()``.

.. code-block:: python

    # Write to csv files in /tmp/output.
    ray.data.range(10000).write_csv("/tmp/output")
    # -> /tmp/output/data0.csv, /tmp/output/data1.csv, ...

    # Use repartition to control the number of output files:
    ray.data.range(10000).repartition(1).write_csv("/tmp/output2")
    # -> /tmp/output2/data0.csv

Transforming Datasets
---------------------

Datasets can be transformed in parallel using ``.map()``. Transformations are executed *eagerly* and block until the operation is finished. Datasets also supports ``.filter()`` and ``.flat_map()``.

.. code-block:: python

    ds = ray.data.range(10000)
    ds = ds.map(lambda x: x * 2)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]
    # -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)
    ds.take(5)
    # -> [0, 2, 4, 6, 8]

    ds.filter(lambda x: x > 5).take(5)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1859.63it/s]
    # -> [6, 8, 10, 12, 14]

    ds.flat_map(lambda x: [x, -x]).take(5)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1568.10it/s]
    # -> [0, 0, 2, -2, 4]

To take advantage of vectorized functions, use ``.map_batches()``. Note that you can also implement ``filter`` and ``flat_map`` using ``.map_batches()``, since your map function can return an output batch of any size.

.. code-block:: python

    ds = ray.data.range_arrow(10000)
    ds = ds.map_batches(
        lambda df: df.applymap(lambda x: x * 2), batch_format="pandas")
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1927.62it/s]
    ds.take(5)
    # -> [ArrowRow({'value': 0}), ArrowRow({'value': 2}), ...]

By default, transformations are executed using Ray tasks. For transformations that require setup, specify ``compute="actors"`` and Ray will use an autoscaling actor pool to execute your transforms instead. The following is an end-to-end example of reading, transforming, and saving batch inference results using Datasets:

.. code-block:: python

    # Example of GPU batch inference on an ImageNet model.
    def preprocess(image: bytes) -> bytes:
        return image

    class BatchInferModel:
        def __init__(self):
            self.model = ImageNetModel()
        def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
            return self.model(batch)

    ds = ray.data.read_binary_files("s3://bucket/image-dir")

    # Preprocess the data.
    ds = ds.map(preprocess)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]

    # Apply GPU batch inference with actors, and assign each actor a GPU using
    # ``num_gpus=1`` (any Ray remote decorator argument can be used here).
    ds = ds.map_batches(BatchInferModel, compute="actors", batch_size=256, num_gpus=1)
    # -> Map Progress (16 actors 4 pending): 100%|██████| 200/200 [00:07, 27.60it/s]

    # Save the results.
    ds.repartition(1).write_json("s3://bucket/inference-results")

Exchanging datasets
-------------------

Datasets can be passed to Ray tasks or actors and read with ``.iter_batches()`` or ``.iter_rows()``. This does not incur a copy, since the blocks of the Dataset are passed by reference as Ray objects:

.. code-block:: python

    @ray.remote
    def consume(data: Dataset[int]) -> int:
        num_batches = 0
        for batch in data.iter_batches():
            num_batches += 1
        return num_batches

    ds = ray.data.range(10000)
    ray.get(consume.remote(ds))
    # -> 200

Datasets can be split up into disjoint sub-datasets. Locality-aware splitting is supported if you pass in a list of actor handles to the ``split()`` function along with the number of desired splits. This is a common pattern useful for loading and splitting data between distributed training actors:

.. code-block:: python

    @ray.remote(num_gpus=1)
    class Worker:
        def __init__(self, rank: int):
            pass

        def train(self, shard: ray.data.Dataset[int]) -> int:
            for batch in shard.iter_batches(batch_size=256):
                pass
            return shard.count()

    workers = [Worker.remote(i) for i in range(16)]
    # -> [Actor(Worker, ...), Actor(Worker, ...), ...]

    ds = ray.data.range(10000)
    # -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

    shards = ds.split(n=16, locality_hints=workers)
    # -> [Dataset(num_blocks=13, num_rows=650, schema=<class 'int'>),
    #     Dataset(num_blocks=13, num_rows=650, schema=<class 'int'>), ...]

    ray.get([w.train.remote(s) for s in shards])
    # -> [650, 650, ...]

Tensor-typed values
-------------------

Datasets support tensor-typed values, which are represented in-memory as Arrow tensors (i.e., np.ndarray format). Tensor datasets can be read from and written to ``.npy`` files. Here are some examples:

.. code-block:: python

    # Create a Dataset of tensor-typed values.
    ds = ray.data.range_tensor(10000, shape=(3, 5))
    # -> Dataset(num_blocks=200, num_rows=10000,
    #            schema=<Tensor: shape=(None, 3, 5), dtype=int64>)

    ds.map_batches(lambda t: t + 2).show(2)
    # -> [[2 2 2 2 2]
    #     [2 2 2 2 2]
    #     [2 2 2 2 2]]
    #    [[3 3 3 3 3]
    #     [3 3 3 3 3]
    #     [3 3 3 3 3]]

    # Save to storage.
    ds.write_numpy("/tmp/tensor_out")

    # Read from storage.
    ray.data.read_numpy("/tmp/tensor_out")
    # -> Dataset(num_blocks=200, num_rows=?,
    #            schema=<Tensor: shape=(None, 3, 5), dtype=int64>)

Tensor datasets are also created whenever an array type is returned from a map function:

.. code-block:: python

    # Create a dataset of Python integers.
    ds = ray.data.range(10)
    # -> Dataset(num_blocks=10, num_rows=10, schema=<class 'int'>)

    # It is now converted into a Tensor dataset.
    ds = ds.map_batches(lambda x: np.array(x))
    # -> Dataset(num_blocks=10, num_rows=10,
    #            schema=<Tensor: shape=(None,), dtype=int64>)

Tensor datasets can also be created from NumPy ndarrays that are already stored in the Ray object store:

.. code-block:: python

    import numpy as np

    # Create a Dataset from a list of NumPy ndarray objects.
    arr1 = np.arange(0, 10)
    arr2 = np.arange(10, 20)
    ds = ray.data.from_numpy([ray.put(arr1), ray.put(arr2)])

Limitations: currently tensor-typed values cannot be nested in tabular records (e.g., as in TFRecord / Petastorm format). This is planned for development.

Custom datasources
------------------

Datasets can read and write in parallel to `custom datasources <package-ref.html#custom-datasource-api>`__ defined in Python.

.. code-block:: python

    # Read from a custom datasource.
    ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

    # Write to a custom datasource.
    ds.write_datasource(YourCustomDatasource(), **write_args)

Contributing
------------

Contributions to Datasets are `welcome <https://docs.ray.io/en/master/development.html#python-develop>`__! There are many potential improvements, including:

- Supporting more datasources and transforms.
- Integration with more ecosystem libraries.
- Adding features that require partitioning such as groupby() and join().
- Performance optimizations.
