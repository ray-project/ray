.. _datasets_scheduling:

==================
Ray Data Internals
==================

Ray Data internals explain how Ray Data processes data at scale using distributed computing principles. This comprehensive guide helps data engineers, performance engineers, and system architects understand Ray Data's sophisticated internal mechanisms to optimize workflows and troubleshoot complex performance issues.

What are Ray Data internals?
----------------------------

Ray Data internals provide deep technical understanding of:

* **Streaming execution model**: How Ray Data processes datasets larger than memory through continuous pipeline execution
* **Distributed architecture**: How data flows across cluster nodes and workers with intelligent coordination
* **Block-based processing**: The fundamental unit of data parallelism that enables scalable distributed processing
* **Operator planning and optimization**: How Ray Data optimizes execution plans for performance and resource efficiency
* **Memory management**: Advanced memory allocation, spilling, and garbage collection strategies
* **Resource coordination**: How CPU, GPU, and memory resources are allocated, managed, and optimized

Why understand Ray Data internals?
----------------------------------

**Performance Engineering Benefits:**
* **Performance optimization**: Understand bottlenecks to tune your workloads effectively and achieve optimal throughput
* **Resource efficiency**: Optimize CPU, GPU, and memory utilization based on internal execution patterns
* **Scaling strategies**: Design workloads that scale linearly by understanding distributed execution mechanisms
* **Cost optimization**: Reduce infrastructure costs through intelligent resource allocation and usage optimization

**Operational Excellence Benefits:**
* **Advanced troubleshooting**: Diagnose complex issues with distributed data processing pipelines
* **Capacity planning**: Size clusters appropriately based on internal resource requirements and utilization patterns
* **Monitoring optimization**: Set up meaningful metrics and alerts based on internal execution characteristics
* **Architecture decisions**: Make informed choices about data processing patterns and deployment strategies

**Expert Development Benefits:**
* **Advanced usage**: Leverage Ray Data's full capabilities for complex, specialized workflows
* **Custom optimization**: Implement workload-specific optimizations based on internal behavior understanding
* **Extension development**: Build custom functionality that integrates properly with Ray Data's internal architecture
* **Contribution readiness**: Understand internals sufficiently to contribute meaningfully to Ray Data development

How Ray Data processes data
---------------------------

Ray Data uses a sophisticated three-phase approach that distinguishes it from traditional batch processing systems:

**Ray Data Processing Pipeline Visualization**

```mermaid
graph TD
    subgraph "Phase 1: Logical Planning"
        LP_Input["User Operations<br/>• ds.map()<br/>• ds.filter()<br/>• ds.groupby()"]
        LP_Graph["Logical Plan Graph<br/>• Operation dependencies<br/>• Data flow analysis<br/>• Optimization opportunities"]
        LP_Optimize["Logical Optimization<br/>• Predicate pushdown<br/>• Column pruning<br/>• Operation fusion"]
    end
    
    subgraph "Phase 2: Physical Planning"
        PP_Resource["Resource Analysis<br/>• Cluster topology<br/>• Resource availability<br/>• Workload characteristics"]
        PP_Strategy["Execution Strategy<br/>• Task distribution<br/>• Memory allocation<br/>• Parallelism planning"]
        PP_Schedule["Task Scheduling<br/>• Node assignment<br/>• Resource allocation<br/>• Execution ordering"]
    end
    
    subgraph "Phase 3: Streaming Execution"
        SE_Blocks["Block Processing<br/>• RefBundle management<br/>• Memory allocation<br/>• Garbage collection"]
        SE_Pipeline["Pipeline Execution<br/>• Streaming coordination<br/>• Backpressure control<br/>• Fault tolerance"]
        SE_Output["Output Generation<br/>• Result materialization<br/>• Data persistence<br/>• Completion tracking"]
    end
    
    LP_Input --> LP_Graph
    LP_Graph --> LP_Optimize
    LP_Optimize --> PP_Resource
    PP_Resource --> PP_Strategy
    PP_Strategy --> PP_Schedule
    PP_Schedule --> SE_Blocks
    SE_Blocks --> SE_Pipeline
    SE_Pipeline --> SE_Output
    
    style LP_Graph fill:#e8f5e8
    style PP_Strategy fill:#e3f2fd
    style SE_Pipeline fill:#fff3e0
```

**Three-Phase Processing Approach:**

1. **Logical planning**: Define what operations to perform on the data with optimization opportunities
2. **Physical planning**: Determine how to execute operations across the cluster with resource optimization
3. **Streaming execution**: Process data in a pipelined, memory-efficient manner with fault tolerance

This sophisticated approach enables Ray Data to handle datasets larger than available cluster memory while maintaining high performance and resource utilization through intelligent coordination and optimization.

For a gentler introduction to Ray Data, see :ref:`Quickstart <data_quickstart>`.

**Internals-Based Performance Optimization**

Understanding Ray Data's internals enables sophisticated performance optimization that goes beyond basic tuning:

.. code-block:: python

    # Advanced performance optimization using internals knowledge
    def optimize_using_internals():
        """Apply internals knowledge for advanced optimization."""
        from ray.data.context import DataContext
        
        ctx = DataContext.get_current()
        
        # Optimize block management based on workload characteristics
        def configure_for_workload_type(workload_type):
            """Configure Ray Data based on workload internals understanding."""
            
            if workload_type == "cpu_intensive_transforms":
                # Optimize for CPU-bound transformations
                ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
                ctx.execution_options.preserve_order = False  # Enable reordering
                
            elif workload_type == "gpu_accelerated_processing":
                # Optimize for GPU memory constraints
                ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
                ctx.execution_options.actor_locality_enabled = True
                
            elif workload_type == "memory_constrained":
                # Optimize for limited memory environments
                ctx.target_max_block_size = 64 * 1024 * 1024   # 64MB blocks
                ctx.execution_options.enable_auto_log_stats = True
                
            elif workload_type == "shuffle_heavy":
                # Optimize for operations requiring data redistribution
                ctx.target_shuffle_max_block_size = 1024 * 1024 * 1024  # 1GB shuffle blocks
                ctx.execution_options.preserve_order = False
        
        return configure_for_workload_type

**Internals-Based Troubleshooting Guide**

Common issues and their internals-based solutions:

**Issue: Out of Memory Errors**
* **Internals cause**: Block sizes exceed available heap memory
* **Diagnosis**: Check `target_max_block_size` vs available memory per worker
* **Solution**: Reduce block sizes or increase cluster memory allocation

.. code-block:: python

    # Diagnose memory issues using internals knowledge
    def diagnose_memory_issues():
        """Use internals knowledge to diagnose memory problems."""
        import ray
        
        # Check current memory configuration
        ctx = ray.data.DataContext.get_current()
        cluster_resources = ray.cluster_resources()
        
        # Calculate expected memory usage
        num_cpus = int(cluster_resources.get("CPU", 0))
        max_block_size_mb = ctx.target_max_block_size / (1024 * 1024)
        expected_memory_mb = num_cpus * max_block_size_mb
        
        print(f"Expected heap memory usage: {expected_memory_mb:.1f} MB")
        print(f"Cluster CPUs: {num_cpus}")
        print(f"Max block size: {max_block_size_mb:.1f} MB")
        
        # Recommend optimization
        if expected_memory_mb > 8000:  # > 8GB
            recommended_block_size = (8000 / num_cpus) * 1024 * 1024
            print(f"Recommended max block size: {recommended_block_size / (1024*1024):.1f} MB")

**Issue: Poor Performance with Large Datasets**
* **Internals cause**: Inefficient streaming execution or materialization
* **Diagnosis**: Check for materialization triggers in pipeline
* **Solution**: Redesign pipeline to maintain streaming execution

.. code-block:: python

    # Identify materialization issues using internals knowledge
    def identify_materialization_issues():
        """Identify operations that break streaming execution."""
        
        # Operations that force materialization (avoid when possible)
        materializing_operations = [
            "ds.sort()",           # AllToAllOperator - requires all data
            "ds.repartition()",    # AllToAllOperator - redistributes data
            "ds.random_shuffle()", # AllToAllOperator - global shuffling
            "ds.materialize()",    # Explicit materialization
            "ds.take_all()",       # Consumes entire dataset
            "ds.count()",          # Requires processing all blocks
        ]
        
        # Streaming-friendly operations (prefer these)
        streaming_operations = [
            "ds.map()",
            "ds.map_batches()",
            "ds.filter()",
            "ds.select_columns()",
            "ds.write_*()",        # Streaming output
        ]
        
        print("Design pipelines using streaming operations for best performance")
        return streaming_operations

.. _dataset_concept:

Key concepts
============

Datasets and blocks
-------------------

Dataset fundamentals
~~~~~~~~

:class:`Dataset <ray.data.Dataset>` is the main user-facing Python API. It represents a
distributed data collection, and defines data loading and processing operations. You
typically use the API in this way:

1. Create a Ray Dataset from external storage or in-memory data.
2. Apply transformations to the data for ETL, analytics, or ML workloads.
3. Write the outputs to external storage, data warehouses, or feed the outputs to training workers.

Block structure
~~~~~~

A *block* is the basic unit of data bulk that Ray Data stores in the object store and
transfers over the network. Each block contains a disjoint subset of rows, and Ray Data
loads and transforms these blocks in parallel.

The following figure visualizes a dataset with three blocks, each holding 1000 rows.
Ray Data holds the :class:`~ray.data.Dataset` on the process that triggers execution
(which is usually the driver) and stores the blocks as objects in Ray's shared-memory
:ref:`object store <objects-in-ray>`.

.. image:: images/dataset-arch.svg
   :alt: Ray Data architecture showing Dataset abstraction with blocks stored in object store

**Figure 1:** Ray Data architecture showing how datasets are distributed across blocks in the object store

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Block formats
~~~~~~~~~~~~~

Blocks are Arrow tables or `pandas` DataFrames. Generally, blocks are Arrow tables
unless Arrow can’t represent your data.

The block format doesn’t affect the type of data returned by APIs like
:meth:`~ray.data.Dataset.iter_batches`.

Block size limiting
~~~~~~~~~~~~~~~~~~~

Ray Data bounds block sizes to avoid excessive communication overhead and prevent
out-of-memory errors. Small blocks are good for latency and more streamed execution,
while large blocks reduce scheduler and communication overhead. The default range
attempts to make a good tradeoff for most jobs.

Ray Data attempts to bound block sizes between 1 MiB and 128 MiB. To change the block
size range, configure the ``target_min_block_size`` and  ``target_max_block_size``
attributes of :class:`~ray.data.context.DataContext`.

.. testcode::

    import ray

    ctx = ray.data.DataContext.get_current()
    ctx.target_min_block_size = 1 * 1024 * 1024
    ctx.target_max_block_size = 128 * 1024 * 1024

Dynamic block splitting
~~~~~~~~~~~~~~~~~~~~~~~

If a block is larger than 192 MiB (50% more than the target max size), Ray Data
dynamically splits the block into smaller blocks.

To change the size at which Ray Data splits blocks, configure
``MAX_SAFE_BLOCK_SIZE_FACTOR``. The default value is 1.5.

.. testcode::

    import ray

    ray.data.context.MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5

Ray Data can’t split rows. So, if your dataset contains large rows (for example, large
images), then Ray Data can’t bound the block size.


Shuffle Algorithms
------------------

In data processing shuffling refers to the process of redistributing individual dataset's partitions (that in Ray Data are
called :ref:`blocks <data_key_concepts>`).

Ray Data implements two main shuffle algorithms:

.. _hash-shuffle:

Hash-shuffling
~~~~~~~~~~~~~~

.. note:: Hash-shuffling is available in Ray 2.46

Hash-shuffling is a classical hash-partitioning based shuffling where:

1. **Partition phase:** rows in every block are hash-partitioned based on values in the *key columns* into a specified number of partitions, following a simple residual formula of ``hash(key-values) % N`` (used in hash-tables and pretty much everywhere).
2. **Push phase:** partition's shards from individual blocks are then pushed into corresponding aggregating actors (called ``HashShuffleAggregator``) handling respective partitions.
3. **Reduce phase:** aggregators combine received individual partition's shards back into blocks optionally applying additional transformations before producing the resulting blocks.

Hash-shuffling is particularly useful for operations that require deterministic partitioning based on keys, such as joins, group-by operations, and key-based repartitioning, by
ensuring that rows with the same key-values are being placed into the same partition.

.. note:: To use hash-shuffling in your aggregations and repartitioning operations, you need to currently specify
    ``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE`` before creating a ``Dataset``.

.. _range-partitioning-shuffle:

Range-partitioning shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~

Range-partitioning based shuffle also is a classical algorithm, based on the dataset being split into target number of ranges as determined by boundaries approximating
the real ranges of the totally ordered (sorted) dataset.

1. **Sampling phase:** every input block is randomly sampled for (10) rows. Samples are combined into a single dataset, which is then sorted and split into
   target number of partitions defining approximate *range boundaries*.
2. **Partition phase:** every block is sorted and split into partitions based on the *range boundaries* derived in the previous step.
3. **Reduce phase:** individual partitions within the same range are then recombined to produce the resulting block.

.. note:: Range-partitioning shuffle is a default shuffling strategy. To set it explicitly specify
    ``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED`` before creating a ``Dataset``.


Operators, plans, and planning
------------------------------

Operator system
~~~~~~~~~

There are two types of operators: *logical operators* and *physical operators*. Logical
operators are stateless objects that describe “what” to do. Physical operators are
stateful objects that describe “how” to do it. An example of a logical operator is
``ReadOp``, and an example of a physical operator is ``TaskPoolMapOperator``.

Execution plans
~~~~~

A *logical plan* is a series of logical operators, and a *physical plan* is a series of
physical operators. When you call APIs like :func:`ray.data.read_images` and
:meth:`ray.data.Dataset.map_batches`, Ray Data produces a logical plan. When execution
starts, the planner generates a corresponding physical plan.

The planner
~~~~~~~~~~~

The Ray Data planner translates logical operators to one or more physical operators. For
example, the planner translates the ``ReadOp`` logical operator into two physical
operators: an ``InputDataBuffer`` and ``TaskPoolMapOperator``. Whereas the ``ReadOp``
logical operator only describes the input data, the ``TaskPoolMapOperator`` physical
operator actually launches tasks to read the data.

Plan optimization
~~~~~~~~~~~~~~~~~

Ray Data applies optimizations to both logical and physical plans. For example, the
``OperatorFusionRule`` combines a chain of physical map operators into a single map
operator. This prevents unnecessary serialization between map operators.

To add custom optimization rules, implement a class that extends ``Rule`` and configure
``DEFAULT_LOGICAL_RULES`` or ``DEFAULT_PHYSICAL_RULES``.

.. testcode::

    import ray
    from ray.data._internal.logical.interfaces import Rule

    class CustomRule(Rule):
        def apply(self, plan):
            ...

    ray.data._internal.logical.optimizers.DEFAULT_LOGICAL_RULES.append(CustomRule)

Types of physical operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Physical operators take in a stream of block references and output another stream of
block references. Some physical operators launch Ray Tasks and Actors to transform
the blocks, and others only manipulate the references.

``MapOperator`` is the most common operator. All read, transform, and write operations
are implemented with it. To process data, ``MapOperator`` implementations use either Ray
Tasks or Ray Actors.

Non-map operators include ``OutputSplitter`` and ``LimitOperator``. These two operators
manipulate references to data, but don’t launch tasks or modify the underlying data.

Execution model
---------

The executor
~~~~~~~~~~~~

The *executor* schedules tasks and moves data between physical operators.

The executor and operators are located on the process where dataset execution starts.
For batch inference jobs, this process is usually the driver. For training jobs, the
executor runs on a special actor called ``SplitCoordinator`` which handles
:meth:`~ray.data.Dataset.streaming_split`.

Tasks and actors launched by operators are scheduled across the cluster, and outputs are
stored in Ray’s distributed object store. The executor manipulates references to
objects, and doesn’t fetch the underlying data itself to the executor.

Out queues
~~~~~~~~~~

Each physical operator has an associated *out queue*. When a physical operator produces
outputs, the executor moves the outputs to the operator’s out queue.

.. _streaming_execution:

Streaming execution
~~~~~~~~~~~~~~~~~~~

In contrast to bulk synchronous execution, Ray Data’s streaming execution doesn’t wait
for one operator to complete to start the next. Each operator takes in and outputs a
stream of blocks. This approach allows you to process datasets that are too large to fit
in your cluster’s memory.

The scheduling loop
~~~~~~~~~~~~~~~~~~~

The executor runs a loop. Each step works like this:

1. Wait until running tasks and actors have new outputs.
2. Move new outputs into the appropriate operator out queues.
3. Choose some operators and assign new inputs to them. These operator process the new
   inputs either by launching new tasks or manipulating metadata.

Choosing the best operator to assign inputs is one of the most important decisions in
Ray Data. This decision is critical to the performance, stability, and scalability of a
Ray Data job. The executor can schedule an operator if the operator satisfies the
following conditions:

* The operator has inputs.
* There are adequate resources available.
* The operator isn’t backpressured.

If there are multiple viable operators, the executor chooses the operator with the
smallest out queue.

Scheduling strategies
==========

Ray Data uses Ray Core for execution. Below is a summary of the :ref:`scheduling strategy <ray-scheduling-strategies>` for Ray Data:

* The ``SPREAD`` scheduling strategy ensures that data blocks and map tasks are evenly balanced across the cluster.
* Dataset tasks ignore placement groups by default, see :ref:`Ray Data and Placement Groups <datasets_pg>`.
* Map operations use the ``SPREAD`` scheduling strategy if the total argument size is less than 50 MB; otherwise, they use the ``DEFAULT`` scheduling strategy.
* Read operations use the ``SPREAD`` scheduling strategy.
* All other operations, such as split, sort, and shuffle, use the ``DEFAULT`` scheduling strategy.

.. _datasets_pg:

Ray Data and placement groups
-----------------------------

By default, Ray Data configures its tasks and actors to use the cluster-default scheduling strategy (``"DEFAULT"``). You can inspect this configuration variable here:
:class:`ray.data.DataContext.get_current().scheduling_strategy <ray.data.DataContext>`. This scheduling strategy schedules these Tasks and Actors outside any present
placement group. To use current placement group resources specifically for Ray Data, set ``ray.data.DataContext.get_current().scheduling_strategy = None``.

Consider this override only for advanced use cases to improve performance predictability. The general recommendation is to let Ray Data run outside placement groups.

.. _datasets_tune:

Ray Data and Tune
-----------------

When using Ray Data in conjunction with :ref:`Ray Tune <tune-main>`, it's important to ensure there are enough free CPUs for Ray Data to run on. By default, Tune tries to fully utilize cluster CPUs. This can prevent Ray Data from scheduling tasks, reducing performance or causing workloads to hang.

To ensure CPU resources are always available for Ray Data execution, limit the number of concurrent Tune trials with the ``max_concurrent_trials`` Tune option.

.. literalinclude:: ./doc_code/key_concepts.py
  :language: python
  :start-after: __resource_allocation_1_begin__
  :end-before: __resource_allocation_1_end__

Memory Management
=================

This section describes how Ray Data manages execution and object store memory.

Execution Memory
----------------

During execution, a task can read multiple input blocks, and write multiple output blocks. Input and output blocks consume both worker heap memory and shared memory through Ray's object store.
Ray caps object store memory usage by spilling to disk, but excessive worker heap memory usage can cause out-of-memory errors.

For more information on tuning memory usage and preventing out-of-memory errors, see the :ref:`performance guide <data_memory>`.

Object Store Memory
-------------------

Ray Data uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

* Object Spilling: Since Ray Data uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:
* Locality Scheduling: Ray preferentially schedules compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.
* Reference Counting: Dataset blocks are kept alive by object store reference counting as long as there is any Dataset that references them. To free memory, delete any Python references to the Dataset object.
