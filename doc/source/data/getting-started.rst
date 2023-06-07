.. _data_getting_started:

Getting Started
===============

Ray Data's main abstraction is a :class:`Dataset <ray.data.Dataset>`, which
is a distributed data transformation pipeline. Dataset provides APIs for loading
external data into Ray in *blocks*, and it exposes APIs for streaming
processing of these data blocks in the cluster.

Install Ray Data
----------------

To install Ray Data, run:

.. code-block:: console

    $ pip install 'ray[data]'

To learn more about installing Ray and its libraries, read
:ref:`Installing Ray <installation>`.

Create a dataset
-------------------

Create datasets from on-disk files, Python objects, and cloud storage services like S3.
Ray Data can read from any `filesystem supported by Arrow
<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    ds.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}


To learn more about creating datasets, read
:ref:`Loading data <loading_data>`.

Transform the dataset
------------------------

Apply :ref:`user-defined functions <transform_datasets_writing_udfs>` (UDFs) to
transform datasets. Ray executes transformations in parallel for performance.

.. testcode::

    from typing import Dict
    import numpy as np

    # Compute a "petal area" attribute.
    def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        vec_a = batch["petal length (cm)"]
        vec_b = batch["petal width (cm)"]
        batch["petal area (cm^2)"] = vec_a * vec_b
        return batch

    transformed_ds = ds.map_batches(transform_batch)
    print(transformed_ds.materialize())

.. testoutput::

    MaterializedDataset(
       num_blocks=1,
       num_rows=150,
       schema={
          sepal length (cm): double,
          sepal width (cm): double,
          petal length (cm): double,
          petal width (cm): double,
          target: int64,
          petal area (cm^2): double
       }
    )

To learn more about transforming datasets, read
:ref:`Transforming data <transforming_data>`.

Consume the dataset
----------------------

Pass datasets to Ray tasks or actors, and access records with methods like
:meth:`~ray.data.Dataset.take_batch` and :meth:`~ray.data.Dataset.iter_batches`.

.. tab-set::

    .. tab-item:: Local

        .. testcode::

            print(transformed_ds.take_batch(batch_size=3))

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

            {'sepal length (cm)': array([5.1, 4.9, 4.7]),
             'sepal width (cm)': array([3.5, 3. , 3.2]),
             'petal length (cm)': array([1.4, 1.4, 1.3]),
             'petal width (cm)': array([0.2, 0.2, 0.2]),
             'target': array([0, 0, 0]),
             'petal area (cm^2)': array([0.28, 0.28, 0.26])}

    .. tab-item:: Tasks

       .. testcode::

            @ray.remote
            def consume(ds: ray.data.Dataset) -> int:
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


To learn more about consuming datasets, read
:ref:`Consuming data <consuming_data>`.

Save the dataset
-------------------

Call methods like :meth:`~ray.data.Dataset.write_parquet` to save dataset contents to local
or remote filesystems.

.. testcode::

    import os

    transformed_ds.write_parquet("/tmp/iris")

    print(os.listdir("/tmp/iris"))

.. testoutput::

    ['..._000000.parquet']


To learn more about saving dataset contents, read :ref:`Saving data <saving_data>`.
