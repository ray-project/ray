.. _datasets_getting_started:

===============
Getting Started
===============

In this tutorial you will learn how to:

- Create and save a Ray ``Dataset``.
- Transform a ``Dataset``.
- Pass a ``Dataset`` to Ray tasks/actors and access the data inside.

.. _ray_datasets_quick_start:

-------------------
Dataset Quick Start
-------------------

Ray Datasets implements Distributed `Arrow <https://arrow.apache.org/>`__.
A Dataset consists of a list of Ray object references to *blocks*.
Each block holds a set of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
(when creating from or transforming to tabular or tensor data), a `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
(when creating from or transforming to Pandas data), or a Python list (otherwise).
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

Datasets can be transformed in parallel using ``.map_batches()``. Ray will transform
batches of records in the Dataset using the given function. The function must return
a batch of records. You are allowed to filter or add additional records to the batch,
which will change the size of the Dataset.

Transformations are executed *eagerly* and block until the operation is finished.

.. code-block:: python

    def transform_batch(df: pandas.DataFrame) -> pd.DataFrame:
        return df.applymap(lambda x: x * 2)

    ds = ray.data.range_table(10000)
    ds = ds.map_batches(transform_batch, batch_format="pandas")
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1927.62it/s]
    ds.take(5)
    # -> [{'value': 0}, {'value': 2}, ...]

The batch format can be specified using ``batch_format`` option, which defaults to "native",
meaning pandas format for Arrow-compatible batches, and Python lists for other types. You
can also specify explicitly "arrow" or "pandas" to force a conversion to that batch format.
The batch size can also be chosen. If not given, the batch size will default to entire blocks.

.. tip::

    Datasets also provides the convenience methods ``map``, ``flat_map``, and ``filter``, which are not vectorized (slower than ``map_batches``), but may be useful for development.

By default, transformations are executed using Ray tasks.
For transformations that require setup, specify ``compute=ray.data.ActorPoolStrategy(min, max)`` and Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your transforms.
For a fixed-size actor pool, specify ``ActorPoolStrategy(n, n)``.
The following is an end-to-end example of reading, transforming, and saving batch inference results using Ray Data:

.. code-block:: python

    from ray.data import ActorPoolStrategy

    # Example of GPU batch inference on an ImageNet model.
    def preprocess(images: List[bytes]) -> List[bytes]:
        return images

    class BatchInferModel:
        def __init__(self):
            self.model = ImageNetModel()
        def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
            return self.model(batch)

    ds = ray.data.read_binary_files("s3://bucket/image-dir")

    # Preprocess the data.
    ds = ds.map_batches(preprocess)
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]

    # Apply GPU batch inference with actors, and assign each actor a GPU using
    # ``num_gpus=1`` (any Ray remote decorator argument can be used here).
    ds = ds.map_batches(
        BatchInferModel, compute=ActorPoolStrategy(10, 20),
        batch_size=256, num_gpus=1)
    # -> Map Progress (16 actors 4 pending): 100%|██████| 200/200 [00:07, 27.60it/s]

    # Save the results.
    ds.repartition(1).write_json("s3://bucket/inference-results")

Passing and accessing datasets
==============================

Datasets can be passed to Ray tasks or actors and accessed with ``.iter_batches()`` or ``.iter_rows()``.
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
