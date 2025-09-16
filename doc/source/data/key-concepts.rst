.. _data_key_concepts:

Key Concepts
============


Datasets and blocks
-------------------

There are two main concepts in Ray Data:

* Datasets
* Blocks

`Dataset` is the main user-facing Python API. It represents a distributed data collection and defines data loading and processing operations. Users typically use the API by:

1. Create a :class:`Dataset <ray.data.Dataset>` from external storage or in-memory data.
2. Apply transformations to the data.
3. Write the outputs to external storage or feed the outputs to training workers.

The Dataset API is lazy, meaning that operations aren't executed until you materialize or consume the dataset,
like :meth:`~ray.data.Dataset.show`. This allows Ray Data to optimize the execution plan
and execute operations in a pipelined, streaming fashion.

*Block* is a set of rows representing single partition of the dataset. Blocks, as a collection of rows represented by columnar formats (like Arrow)
 are the basic unit of data processing in Ray Data:

 1. Every dataset is partitioned into a number of blocks, then
 2. Processing of the whole dataset is distributed and parallelized at the block level (blocks are processed in parallel and for the most part independently)

Block is the basic unit of data that every Ray Data dataset is partitioned into and stored in the object store. Data processing is parallelized at the block level.

The following figure visualizes a dataset with three blocks, each holding 1000 rows.
Ray Data holds the :class:`~ray.data.Dataset` on the process that triggers execution
(which is usually the entrypoint of the program, referred to as the :term:`driver`)
and stores the blocks as objects in Ray's shared-memory :ref:`object store <objects-in-ray>`. Internally, Ray Data can natively handle blocks either
as Pandas ``DataFrame`` or PyArrow ``Table``.

.. image:: images/dataset-arch-with-blocks.svg
..
  https://docs.google.com/drawings/d/1kOYQqHdMrBp2XorDIn0u0G_MvFj-uSA4qm6xf9tsFLM/edit

Operators and Plans
-------------------

Ray Data uses a two-phase planning process to execute operations efficiently. When you write a program using the Dataset API, Ray Data first builds a *logical plan* - a high-level description of what operations to perform. When execution begins, it converts this into a *physical plan* that specifies exactly how to execute those operations.

This diagram illustrates the complete planning process:

.. https://docs.google.com/drawings/d/1WrVAg3LwjPo44vjLsn17WLgc3ta2LeQGgRfE8UHrDA0/edit

.. image:: images/get_execution_plan.svg
   :width: 600
   :align: center

The building blocks of these plans are operators:

* Logical plans consist of *logical operators* that describe *what* operation to perform. For example, ``ReadOp`` specifies what data to read.
* Physical plans consist of *physical operators* that describe *how* to execute the operation. For example, ``TaskPoolMapOperator`` launches Ray tasks to actually read the data.

Here is a simple example of how Ray Data builds a logical plan. As you chain operations together, Ray Data constructs the logical plan behind the scenes:

.. testcode::
    import ray

    dataset = ray.data.range(100)
    dataset = dataset.add_column("test", lambda x: x["id"] + 1)
    dataset = dataset.select_columns("test")

You can inspect the resulting logical plan by printing the dataset:

.. code-block::

    Project
    +- MapBatches(add_column)
       +- Dataset(schema={...})

When execution begins, Ray Data optimizes the logical plan, then translates it into a physical plan - a series of operators that implement the actual data transformations. During this translation:

1. A single logical operator may become multiple physical operators. For example, ``ReadOp`` becomes both ``InputDataBuffer`` and ``TaskPoolMapOperator``.
2. Both logical and physical plans go through optimization passes. For example, ``OperatorFusionRule`` combines map operators to reduce serialization overhead.

Physical operators work by:

* Taking in a stream of block references
* Performing their operation (either transforming data with Ray Tasks/Actors or manipulating references)
* Outputting another stream of block references

For more details on Ray Tasks and Actors, see :ref:`Ray Core Concepts <core-key-concepts>`.

.. note:: A dataset's execution plan only runs when you materialize or consume the dataset through operations like :meth:`~ray.data.Dataset.show`.

.. _streaming-execution:

Streaming execution model
-------------------------

Ray Data uses a *streaming execution model* to efficiently process large datasets.

Rather than materializing the entire dataset in memory at once,
Ray Data can process data in a streaming fashion through a pipeline of operations.

This is useful for inference and training workloads where the dataset can be too large to fit in memory and the workload doesn't require the entire dataset to be in memory at once.

Here is an example of how the streaming execution model works. The below code creates a dataset with 1K rows, applies a map and filter transformation, and then calls the ``show`` action to trigger the pipeline:

.. testcode::

    import ray

    # Create a dataset with 1K rows
    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    # Define a pipeline of operations
    ds = ds.map(lambda x: {"target1": x["target"] * 2})
    ds = ds.map(lambda x: {"target2": x["target1"] * 2})
    ds = ds.map(lambda x: {"target3": x["target2"] * 2})
    ds = ds.filter(lambda x: x["target3"] % 4 == 0)

    # Data starts flowing when you call a method like show()
    ds.show(5)

This creates a logical plan like the following:

.. code-block::

    Filter(<lambda>)
    +- Map(<lambda>)
       +- Map(<lambda>)
          +- Map(<lambda>)
             +- Dataset(schema={...})


The streaming topology looks like the following:

.. https://docs.google.com/drawings/d/10myFIVtpI_ZNdvTSxsaHlOhA_gHRdUde_aHRC9zlfOw/edit

.. image:: images/streaming-topology.svg
   :width: 1000
   :align: center

In the streaming execution model, operators are connected in a pipeline, with each operator's output queue feeding directly into the input queue of the next downstream operator. This creates an efficient flow of data through the execution plan.

The streaming execution model provides significant advantages for data processing.

In particular, the pipeline architecture enables multiple stages to execute concurrently, improving overall performance and resource utilization. For example, if the map operator requires GPU resources, the streaming execution model can execute the map operator concurrently with the filter operator (which may run on CPUs), effectively utilizing the GPU through the entire duration of the pipeline.

To summarize, Ray Data's streaming execution model can efficiently process datasets that are much larger than available memory while maintaining high performance through parallel execution across the cluster.

.. note::
   Operations like :meth:`ds.sort() <ray.data.Dataset.sort>` and :meth:`ds.groupby() <ray.data.Dataset.groupby>` require materializing data, which may impact memory usage for very large datasets.

You can read more about the streaming execution model in this `blog post <https://www.anyscale.com/blog/streaming-distributed-execution-across-cpus-and-gpus>`__.
