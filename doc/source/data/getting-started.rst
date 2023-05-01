.. _data_getting_started:

Getting Started
===============

Ray Data's main abstraction is a :class:`Datastream <ray.data.Datastream>`, which
is a distributed data transformation pipeline. Datastream provides APIs for loading
external data into Ray in *blocks*, and it exposes APIs for streaming
processing of these data blocks in the cluster.

.. tip::

    Ray Data is for processing of *finite* datasets for ML training and
    batch inference. This is in contrast to frameworks such as Apache Flink that
    process infinite data streams.

Install Ray Data
----------------

To install Ray Data, run:

.. code-block:: console

    $ pip install 'ray[data]'

To learn more about installing Ray and its libraries, read
:ref:`Installing Ray <installation>`.

Create a datastream
-------------------

Create datastreams from on-disk files, Python objects, and cloud storage services like S3.
Ray reads from any `filesystem supported by Arrow
<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__.

.. testcode::

    import ray

    datastream = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    datastream.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}


To learn more about creating datastreams, read
:ref:`Loading data <creating_datastreams>`.

Transform the datastream
------------------------

Apply :ref:`user-defined functions <transform_datastreams_writing_udfs>` (UDFs) to
transform datastreams. Ray executes transformations in parallel for performance.

.. testcode::

    import pandas as pd

    # Find rows with sepal length < 5.5 and petal length > 3.5.
    def transform_batch(df: pd.DataFrame) -> pd.DataFrame:
        return df[(df["sepal length (cm)"] < 5.5) & (df["petal length (cm)"] > 3.5)]

    transformed_ds = datastream.map_batches(transform_batch, batch_format="pandas")
    print(transformed_ds)

.. testoutput::

    MapBatches(transform_batch)
    +- Datastream(
          num_blocks=1,
          num_rows=150,
          schema={
             sepal length (cm): double,
             sepal width (cm): double,
             petal length (cm): double,
             petal width (cm): double,
             target: int64
          }
       )


To learn more about transforming datastreams, read
:ref:`Transforming data <transforming_datastreams>`.

Consume the datastream
----------------------

Pass datastreams to Ray tasks or actors, and access records with methods like
:meth:`~ray.data.Datastream.iter_batches`.

.. tab-set::

    .. tab-item:: Local

        .. testcode::

            batches = transformed_ds.iter_batches(batch_size=8)
            print(next(iter(batches)))

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

            {'sepal length (cm)': array([5.2, 5.4, 4.9]),
             'sepal width (cm)': array([2.7, 3. , 2.5]),
             'petal length (cm)': array([3.9, 4.5, 4.5]),
             'petal width (cm)': array([1.4, 1.5, 1.7]),
             'target': array([1, 1, 2])}

    .. tab-item:: Tasks

       .. testcode::

            @ray.remote
            def consume(ds: ray.data.Datastream) -> int:
                num_batches = 0
                for batch in ds.iter_batches(batch_size=8):
                    num_batches += 1
                return num_batches

            ray.get(consume.remote(transformed_ds))

    .. tab-item:: Actors

        .. testcode::

            @ray.remote
            class Worker:

                def train(self, data_iterator):
                    for batch in data_iterator.iter_batches(batch_size=8):
                        pass

            workers = [Worker.remote() for _ in range(4)]
            shards = transformed_ds.streaming_split(n=4, equal=True)
            ray.get([w.train.remote(s) for w, s in zip(workers, shards)])


To learn more about consuming datastreams, read
:ref:`Consuming data <consuming_datastreams>`.

Save the datastream
-------------------

Call methods like :meth:`~ray.data.Datastream.write_parquet` to save datastream contents to local
or remote filesystems.

.. testcode::

    import os

    transformed_ds.write_parquet("iris")

    print(os.listdir("iris"))

.. testoutput::
    :options: +ELLIPSIS

    ['..._000000.parquet']


To learn more about saving datastream contents, read :ref:`Saving data <saving_datastreams>`.

Next Steps
----------

* To check how your application is doing, you can use the :ref:`Ray dashboard<ray-dashboard>`. 
