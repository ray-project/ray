.. _data_quickstart:

Quickstart
==========

This page introduces the :class:`Dataset <ray.data.Dataset>` concept and the capabilities it provides.

This guide provides a lightweight introduction to:

* :ref:`Key Concepts: Datasets and Blocks <data_key_concepts>`
* :ref:`Loading data <loading_key_concept>`
* :ref:`Transforming data <transforming_key_concept>`
* :ref:`Consuming data <consuming_key_concept>`
* :ref:`Saving data <saving_key_concept>`
* :ref:`Streaming Execution Model <streaming_execution_model>`

.. _data_key_concepts:

Key Concepts: Datasets and Blocks
---------------------------------

There are two main concepts in Ray Data:

* :class:`Dataset <ray.data.Dataset>`
* :ref:`Blocks <blocks_key_concept>`

:class:`Dataset <ray.data.Dataset>` is the main user-facing Python API. It represents a
distributed data collection and defines data loading and processing operations. You
typically use the API in this way:

1. Create a Dataset from external storage or in-memory data.
2. Apply transformations to the data.
3. Write the outputs to external storage or feed the outputs to training workers.

The Dataset API is lazy, meaning that operations are not executed until you call an action
like :meth:`~ray.data.Dataset.show`. This allows Ray Data to optimize the execution plan
and execute operations in parallel.

A *block* is the basic unit of data that Ray Data operates on. A block is a contiguous
subset of rows from a dataset.

The following figure visualizes a dataset with three blocks, each holding 1000 rows.
Ray Data holds the :class:`~ray.data.Dataset` on the process that triggers execution
(which is usually the driver) and stores the blocks as objects in Ray's shared-memory
:ref:`object store <objects-in-ray>`.

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit


.. _loading_key_concept:

Loading data
------------

Create datasets from on-disk files, Python objects, and cloud storage services like S3.
Ray Data can read from any `filesystem supported by Arrow
<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    ds.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}

To learn more about creating datasets, read :ref:`Loading data <loading_data>`.

.. _transforming_key_concept:

Transforming data
-----------------

Apply user-defined functions (UDFs) to transform datasets. Ray executes transformations
in parallel for performance.

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
       num_blocks=...,
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

.. _consuming_key_concept:

Consuming data
--------------

Pass datasets to Ray Tasks or Actors, and access records with methods like
:meth:`~ray.data.Dataset.take_batch` and :meth:`~ray.data.Dataset.iter_batches`.

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

To learn more about consuming datasets, see
:ref:`Iterating over Data <iterating-over-data>` and :ref:`Saving Data <saving-data>`.

.. _saving_key_concept:

Saving data
-----------

Call methods like :meth:`~ray.data.Dataset.write_parquet` to save dataset contents to local
or remote filesystems.

.. testcode::
    :hide:

    # The number of blocks can be non-determinstic. Repartition the dataset beforehand
    # so that the number of written files is consistent.
    transformed_ds = transformed_ds.repartition(2)

.. testcode::

    import os

    transformed_ds.write_parquet("/tmp/iris")

    print(os.listdir("/tmp/iris"))

.. testoutput::
    :options: +MOCK

    ['..._000000.parquet', '..._000001.parquet']


To learn more about saving dataset contents, see :ref:`Saving data <saving-data>`.

.. _streaming_execution_model:

Streaming Execution Model
-------------------------

Ray Data uses a streaming execution model to efficiently process large datasets.
Rather than materializing the entire dataset in memory at once,
Ray Data can process data in a streaming fashion through a pipeline of operations.

Here's how it works:

.. code-block:: python

    import ray

    # Create a dataset with 1K rows
    ds = ray.data.range(1_000)

    # Define a pipeline of operations
    ds = ds.map(lambda x: x * 2)
    ds = ds.filter(lambda x: x % 4 == 0)

    # Data starts flowing when you call an action like show()
    ds.show(5)

Key benefits of streaming execution include:

* **Memory Efficient**: Processes data in chunks rather than loading everything into memory
* **Pipeline Parallelism**: Different stages of the pipeline can execute concurrently
* **Automatic Memory Management**: Ray Data automatically spills data to disk if memory pressure is high
* **Lazy Evaluation**: Transformations are not executed until an action triggers the pipeline

The streaming model allows Ray Data to efficiently handle datasets much larger than memory while maintaining high performance through parallel execution.

.. note::
   Operations like :meth:`ds.sort() <ray.data.Dataset.sort>` and :meth:`ds.groupby() <ray.data.Dataset.groupby>` require materializing data, which may impact memory usage for very large datasets.


Deep Dive into Ray Data
-----------------------

To learn more about Ray Data, see :ref:`Ray Data Internals <data_internals>`.
