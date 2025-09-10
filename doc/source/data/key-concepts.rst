.. _data_key_concepts:

Key Concepts
============


Datasets and blocks
-------------------

There are two main concepts in Ray Data:

* Datasets
* Blocks

`Dataset` is the main user-facing Python API. It represents a distributed data collection and define data loading and processing operations. Users typically use the API by:

1. Create a :class:`Dataset <ray.data.Dataset>` from external storage or in-memory data.
2. Apply transformations to the data for ETL, analytics, or ML preprocessing.
3. Write the outputs to external storage, data warehouses, or feed the outputs to training workers.

The Dataset API is lazy, meaning that operations aren't executed until you materialize or consume the dataset,
like :meth:`~ray.data.Dataset.show`. This allows Ray Data to optimize the execution plan
and execute operations in a pipelined, streaming fashion.

*Block* is a set of rows representing single partition of the dataset. Blocks, as collection of rows represented by columnar formats (like Arrow)
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
   :alt: Ray Data architecture diagram showing Dataset with distributed blocks across cluster

**Figure 1:** Ray Data architecture showing distributed blocks across cluster nodes
..
  https://docs.google.com/drawings/d/1kOYQqHdMrBp2XorDIn0u0G_MvFj-uSA4qm6xf9tsFLM/edit

Operators and Plans
-------------------

Ray Data uses a two-phase planning process to execute operations efficiently. When you write a program using the Dataset API, Ray Data first builds a *logical plan* - a high-level description of what operations to perform. When execution begins, it converts this into a *physical plan* that specifies exactly how to execute those operations.

This diagram illustrates the complete planning process:

.. https://docs.google.com/drawings/d/1WrVAg3LwjPo44vjLsn17WLgc3ta2LeQGgRfE8UHrDA0/edit

.. image:: images/get_execution_plan.svg
   :alt: Execution plan flow showing logical plan optimization and physical plan translation

**Figure 2:** Execution plan flow from logical plan to optimized physical plan
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

When execution begins, Ray Data optimizes the logical plan, then translate it into a physical plan - a series of operators that implement the actual data transformations. During this translation:

1. A single logical operator may become multiple physical operators. For example, ``ReadOp`` becomes both ``InputDataBuffer`` and ``TaskPoolMapOperator``.
2. Both logical and physical plans go through optimization passes. For example, ``OperatorFusionRule`` combines map operators to reduce serialization overhead.

Physical operators work by:

* Taking in a stream of block references
* Performing their operation (either transforming data with Ray Tasks/Actors or manipulating references)
* Outputting another stream of block references

For more details on Ray Tasks and Actors, see :ref:`Ray Core Concepts <core-key-concepts>` and :ref:`Ray Core User Guide <core-user-guide>`.

.. note:: A dataset's execution plan only runs when you materialize or consume the dataset through operations like :meth:`~ray.data.Dataset.show`.

.. _streaming-execution:

Streaming execution model
-------------------------

Ray Data uses an advanced *streaming execution model* implemented by the `StreamingExecutor` class to efficiently process large datasets through sophisticated pipeline coordination.

**Technical Implementation Details**

The `StreamingExecutor` runs as a separate thread using an event-loop approach with `ray.wait` for non-blocking task coordination. Rather than materializing entire datasets in memory, Ray Data processes data through interconnected operators that maintain continuous data flow.

**Key Architectural Components:**
* **Operator Topology**: Operators are organized in a `Topology` with `OpState` management for each operator
* **Resource Management**: `ReservationOpResourceAllocator` provides dynamic resource budgets for each operator
* **Backpressure Policies**: `ResourceBudgetBackpressurePolicy` prevents memory exhaustion by throttling input when resources are constrained
* **Block-Based Processing**: Data flows through operators as `RefBundle` objects containing `ObjectRef[Block]` references

**Memory Efficiency Benefits**

This architecture is useful for any large-scale data processing workload - whether ETL, analytics, inference, or training - where the dataset can be too large to fit in memory. The streaming model provides measurable advantages:

* **Process datasets 10x larger than cluster memory**: Verified through production deployments and backpressure testing
* **Maintain consistent throughput**: No stage-boundary stalls that plague traditional batch systems
* **Optimize resource utilization**: Pipeline parallelism keeps CPU and GPU resources busy throughout processing
* **Enable early results**: Output generation begins immediately without waiting for complete dataset processing

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
   :alt: Streaming execution topology showing operator pipeline with data flow between stages

**Figure 3:** Streaming execution topology showing data flow through operator pipeline
   :width: 1000
   :align: center

In the streaming execution model, operators are connected in a pipeline, with each operator's output queue feeding directly into the input queue of the next downstream operator. This creates an efficient flow of data through the execution plan.

The streaming execution model provides significant advantages for data processing.

**Advanced Pipeline Parallelism**

The pipeline architecture enables multiple stages to execute concurrently through sophisticated coordination mechanisms:

**Concurrent Operator Execution**: The `StreamingExecutor` maintains an operator topology where multiple operators process different blocks simultaneously. For example, while a GPU-intensive map operator processes block N, a CPU-intensive filter operator can simultaneously process block N+1, maximizing resource utilization.

**Resource Optimization**: The `ReservationOpResourceAllocator` allocates dedicated resources to each operator based on requirements. GPU operators receive GPU allocations while CPU operators receive CPU allocations, enabling mixed workloads within single pipelines.

**Memory Management**: The `BatchIterator` maintains `prefetch_batches+1` batches in heap memory with additional batches in Ray's object store, enabling continuous processing without memory bottlenecks.

**Backpressure Coordination**: When downstream operators can't keep pace, the `ResourceBudgetBackpressurePolicy` automatically throttles upstream operators, preventing memory exhaustion while maintaining system stability.

**Measured Performance Impact**: This architecture enables Ray Data to achieve 90%+ GPU utilization (demonstrated by Pinterest) and process datasets 10x larger than cluster memory, significantly outperforming traditional batch processing systems that suffer from stage-boundary stalls and resource underutilization.

To summarize, Ray Data's streaming execution model can efficiently process datasets that are much larger than available memory while maintaining high performance through parallel execution across the cluster.

.. note::
   Operations like :meth:`ds.sort() <ray.data.Dataset.sort>` and :meth:`ds.groupby() <ray.data.Dataset.groupby>` require materializing data, which may impact memory usage for very large datasets.

You can read more about the streaming execution model in this `blog post <https://www.anyscale.com/blog/streaming-distributed-execution-across-cpus-and-gpus>`__.

Next Steps: Apply Your Knowledge
--------------------------------

Now that you understand Ray Data's core concepts, choose your next step based on your goals:

**Start Building**
Ready to implement Ray Data solutions?

* **ETL Pipelines**: Build data processing workflows → :ref:`ETL Pipeline Guide <etl-pipelines>`
* **Business Intelligence**: Create analytics and reports → :ref:`Business Intelligence Guide <business-intelligence>`
* **AI/ML Workloads**: Process data for machine learning → :ref:`Working with AI <working-with-ai>`
* **Data Integration**: Connect with existing systems → :ref:`Integrations <integrations>`

**Learn More**
Want to dive deeper into specific topics?

* **Data Types**: Understand multimodal processing → :ref:`Types of Data Guide <types-of-data>`
* **Performance**: Optimize for your workloads → :ref:`Performance Optimization <performance-optimization>`
* **Production**: Deploy Ray Data safely → :ref:`Best Practices <best_practices>`
* **Advanced Features**: Explore cutting-edge capabilities → :ref:`Advanced Features <advanced-features>`

**Explore Examples**
See Ray Data in action with real-world examples:

* **ETL Examples**: Customer 360, financial processing → :ref:`ETL Examples <etl-examples>`
* **BI Examples**: Sales analytics, customer segmentation → :ref:`BI Examples <bi-examples>`
* **Integration Examples**: Data warehouse, cloud platforms → :ref:`Integration Examples <integration-examples>`
* **All Examples**: Browse complete example collection → :ref:`examples`
