.. _datasets_getting_started:

Getting Started
===============

A Ray :class:`Dataset <ray.data.Dataset>` is a distributed data collection. It holds
references to distributed data *blocks*, and exposes APIs for loading and processing
data.

Install Ray Data
----------------

To install Ray Data, run:

.. code-block:: console

    $ pip install 'ray[data]'

To learn more about installing Ray and its libraries, read
:ref:`Installing Ray <installation>`.

Create a dataset
----------------

Create datasets from on-disk files, Python objects, and cloud storage services like S3.
Ray reads from any `filesystem supported by Arrow
<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__.

.. testcode::

    import ray

    dataset = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    dataset.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}


To learn more about creating datasets, read
:ref:`Creating datasets <creating_datasets>`.

Transform the dataset
---------------------

Apply :ref:`user-defined functions <transform_datasets_writing_udfs>` (UDFs) to
transform datasets. Ray executes transformations in parallel for performance at scale.

.. testcode::

    import pandas as pd

    # Find rows with spepal length < 5.5 and petal length > 3.5.
    def transform_batch(df: pd.DataFrame) -> pd.DataFrame:
        return df[(df["sepal length (cm)"] < 5.5) & (df["petal length (cm)"] > 3.5)]

    transformed_dataset = dataset.map_batches(transform_batch)
    print(transformed_dataset)

.. testoutput::

    MapBatches(transform_batch)
    +- Dataset(
          num_blocks=...,
          num_rows=150,
          schema={
             sepal length (cm): double,
             sepal width (cm): double,
             petal length (cm): double,
             petal width (cm): double,
             target: int64
          }
    )

To learn more about transforming datasets, read
:ref:`Transforming datasets <transforming_datasets>`.

Consume the dataset
-------------------

Pass datasets to Ray tasks or actors, and access records with methods like
:meth:`~ray.data.Dataset.iter_batches`.

.. tabbed:: Local

    .. testcode::

        batches = transformed_dataset.iter_batches(batch_size=8)
        print(next(iter(batches)))

    .. testoutput::
        :options: +NORMALIZE_WHITESPACE

           sepal length (cm)  ...  target
        0                5.2  ...       1
        1                5.4  ...       1
        2                4.9  ...       2

        [3 rows x 5 columns]

.. tabbed:: Tasks

   .. testcode::

        @ray.remote
        def consume(dataset: ray.data.Dataset) -> int:
            num_batches = 0
            for batch in dataset.iter_batches(batch_size=8):
                num_batches += 1
            return num_batches

        ray.get(consume.remote(transformed_dataset))

.. tabbed:: Actors

    .. testcode::

        @ray.remote
        class Worker:

            def train(self, shard) -> int:
                for batch in shard.iter_batches(batch_size=8):
                    pass
                return shard.count()

        workers = [Worker.remote() for _ in range(4)]
        shards = transformed_dataset.split(n=4, locality_hints=workers)
        ray.get([w.train.remote(s) for w, s in zip(workers, shards)])


To learn more about consuming datasets, read
:ref:`Consuming datasets <consuming_datasets>`.

Save the dataset
----------------

Call methods like :meth:`~ray.data.Dataset.write_parquet` to save datasets to local
or remote filesystems.

.. testcode::

    import os

    transformed_dataset.write_parquet("iris")

    print(os.listdir("iris"))

.. testoutput::
    :options: +ELLIPSIS

    ['..._000000.parquet']


To learn more about saving datasets, read :ref:`Saving datasets <saving_datasets>`.

Next Steps
----------

* To check how your application is doing, you can use the :ref:`Ray dashboard<ray-dashboard>`. 