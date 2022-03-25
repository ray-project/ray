.. _datasets_getting_started:

===============
Getting Started
===============

In this tutorial you will learn how to:

- Create and save a Ray ``Dataset``.
- How to transform a ``Dataset`` and pass it into other Ray Tasks.
- How to create a Ray ``DatasetPipeline`` and run transformations on it.

.. _ray_datasets_quick_start:

-------------------
Dataset Quick Start
-------------------

Ray Datasets implements `Distributed Arrow <https://arrow.apache.org/>`__.
A Dataset consists of a list of Ray object references to *blocks*.
Each block holds a set of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
or a Python list (for Arrow incompatible objects).
Let's start by creating a Dataset.

Creating Datasets
=================

.. tip::

   Run ``pip install "ray[data]"`` to get started!

You can get started by creating Datasets from synthetic data using ``ray.data.range()`` and ``ray.data.from_items()``.
Datasets can hold either plain Python objects (i.e. their schema is a Python type), or Arrow records
(in which case their schema is Arrow).

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
    # -> {'col1': 0, 'col2': '0'}
    # -> {'col1': 1, 'col2': '1'}
    # -> {'col1': 2, 'col2': '2'}
    # -> {'col1': 3, 'col2': '3'}
    # -> {'col1': 4, 'col2': '4'}

    ds.schema()
    # -> col1: int64
    # -> col2: string

Datasets can be created from files on local disk or remote datasources such as S3.
Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations:

.. code-block:: python

    # Read a directory of files in remote storage.
    ds = ray.data.read_csv("s3://bucket/path")

    # Read multiple local files.
    ds = ray.data.read_csv(["/path/to/file1", "/path/to/file2"])

    # Read multiple directories.
    ds = ray.data.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

Finally, you can create a ``Dataset`` from existing data in the Ray object store or Ray-compatible distributed DataFrames:

.. code-block:: python

    import pandas as pd
    import dask.dataframe as dd

    # Create a Dataset from a list of Pandas DataFrame objects.
    pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([pdf])

    # Create a Dataset from a Dask-on-Ray DataFrame.
    dask_df = dd.from_pandas(pdf, npartitions=10)
    ds = ray.data.from_dask(dask_df)

Saving Datasets
===============

Datasets can be written to local or remote storage using ``.write_csv()``, ``.write_json()``, and ``.write_parquet()``.

.. code-block:: python

    # Write to csv files in /tmp/output.
    ray.data.range(10000).write_csv("/tmp/output")
    # -> /tmp/output/data0.csv, /tmp/output/data1.csv, ...

    # Use repartition to control the number of output files:
    ray.data.range(10000).repartition(1).write_csv("/tmp/output2")
    # -> /tmp/output2/data0.csv

You can also convert a ``Dataset`` to Ray-compatible distributed DataFrames:

.. code-block:: python

    # Convert a Ray Dataset into a Dask-on-Ray DataFrame.
    dask_df = ds.to_dask()

Transforming Datasets
=====================

Datasets can be transformed in parallel using ``.map()``.
Transformations are executed *eagerly* and block until the operation is finished.
Datasets also supports ``.filter()`` and ``.flat_map()``.

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

To take advantage of vectorized functions, use ``.map_batches()``.
Note that you can also implement ``filter`` and ``flat_map`` using ``.map_batches()``,
since your map function can return an output batch of any size.

.. code-block:: python

    ds = ray.data.range_arrow(10000)
    ds = ds.map_batches(
        lambda df: df.applymap(lambda x: x * 2), batch_format="pandas")
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1927.62it/s]
    ds.take(5)
    # -> [{'value': 0}, {'value': 2}, ...]

By default, transformations are executed using Ray tasks.
For transformations that require setup, specify ``compute=ray.data.ActorPoolStrategy(min, max)`` and Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your transforms.
For a fixed-size actor pool, specify ``ActorPoolStrategy(n, n)``.
The following is an end-to-end example of reading, transforming, and saving batch inference results using Ray Data:

.. code-block:: python

    from ray.data import ActorPoolStrategy

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
    ds = ds.map_batches(
        BatchInferModel, compute=ActorPoolStrategy(10, 20),
        batch_size=256, num_gpus=1)
    # -> Map Progress (16 actors 4 pending): 100%|██████| 200/200 [00:07, 27.60it/s]

    # Save the results.
    ds.repartition(1).write_json("s3://bucket/inference-results")

Exchanging datasets
===================

Datasets can be passed to Ray tasks or actors and read with ``.iter_batches()`` or ``.iter_rows()``.
This does not incur a copy, since the blocks of the Dataset are passed by reference as Ray objects:

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

Datasets can be split up into disjoint sub-datasets.
Locality-aware splitting is supported if you pass in a list of actor handles to the ``split()`` function along with the number of desired splits.
This is a common pattern useful for loading and splitting data between distributed training actors:

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

    ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
    # -> [650, 650, ...]


.. _dataset_pipelines_quick_start:

-----------------------------
Dataset Pipelines Quick Start
-----------------------------

Creating a DatasetPipeline
==========================

A `DatasetPipeline <package-ref.html#datasetpipeline-api>`__ can be constructed in two ways: either by pipelining the execution of an existing Dataset (via ``Dataset.window``), or generating repeats of an existing Dataset (via ``Dataset.repeat``). Similar to Datasets, you can freely pass DatasetPipelines between Ray tasks, actors, and libraries. Get started with this synthetic data example:

.. code-block:: python

    import ray

    def func1(i: int) -> int:
        return i + 1

    def func2(i: int) -> int:
        return i * 2

    def func3(i: int) -> int:
        return i % 3

    # Create a dataset and then create a pipeline from it.
    base = ray.data.range(1000000)
    print(base)
    # -> Dataset(num_blocks=200, num_rows=1000000, schema=<class 'int'>)
    pipe = base.window(blocks_per_window=10)
    print(pipe)
    # -> DatasetPipeline(num_windows=20, num_stages=1)

    # Applying transforms to pipelines adds more pipeline stages.
    pipe = pipe.map(func1)
    pipe = pipe.map(func2)
    pipe = pipe.map(func3)
    print(pipe)
    # -> DatasetPipeline(num_windows=20, num_stages=4)

    # Output can be pulled from the pipeline concurrently with its execution.
    num_rows = 0
    for row in pipe.iter_rows():
        num_rows += 1
    # ->
    # Stage 0:  55%|█████████████████████████                |11/20 [00:02<00:00,  9.86it/s]
    # Stage 1:  50%|██████████████████████                   |10/20 [00:02<00:01,  9.45it/s]
    # Stage 2:  45%|███████████████████                      | 9/20 [00:02<00:01,  8.27it/s]
    # Stage 3:  35%|████████████████                         | 8/20 [00:02<00:02,  5.33it/s]
    print("Total num rows", num_rows)
    # -> Total num rows 1000000

You can also create a DatasetPipeline from a custom iterator over dataset creators using ``DatasetPipeline.from_iterable``. For example, this is how you would implement ``Dataset.repeat`` and ``Dataset.window`` using ``from_iterable``:

.. code-block:: python

    import ray
    from ray.data.dataset_pipeline import DatasetPipeline

    # Equivalent to ray.data.range(1000).repeat(times=4)
    source = ray.data.range(1000)
    pipe = DatasetPipeline.from_iterable(
        [lambda: source, lambda: source, lambda: source, lambda: source])

    # Equivalent to ray.data.range(1000).window(blocks_per_window=10)
    splits = ray.data.range(1000, parallelism=200).split(20)
    pipe = DatasetPipeline.from_iterable([lambda s=s: s for s in splits])


Per-Window Transformations
==========================

While most Dataset operations are per-row (e.g., map, filter), some operations apply to the Dataset as a whole (e.g., sort, shuffle). When applied to a pipeline, holistic transforms like shuffle are applied separately to each window in the pipeline:

.. code-block:: python

    # Example of randomly shuffling each window of a pipeline.
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(2) \
        .random_shuffle_each_window() \
        .show_windows()
    # ->
    # ----- Epoch 0 ------
    # === Window 0 ===
    # 4
    # 3
    # 1
    # 0
    # 2
    # ----- Epoch 1 ------
    # === Window 1 ===
    # 2
    # 1
    # 4
    # 0
    # 3

You can also apply arbitrary transformations to each window using ``DatasetPipeline.foreach_window()``:

.. code-block:: python

    # Equivalent transformation using .foreach_window()
    ray.data.from_items([0, 1, 2, 3, 4]) \
        .repeat(2) \
        .foreach_window(lambda w: w.random_shuffle()) \
        .show_windows()
    # ->
    # ----- Epoch 0 ------
    # === Window 0 ===
    # 1
    # 0
    # 4
    # 2
    # 3
    # ----- Epoch 1 ------
    # === Window 1 ===
    # 4
    # 2
    # 0
    # 3
    # 1
