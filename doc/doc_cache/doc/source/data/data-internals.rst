.. _datasets_scheduling:

==================
Ray Data Internals
==================

This guide describes the implementation of Ray Data. The intended audience is advanced
users and Ray Data developers.

For a gentler introduction to Ray Data, see :ref:`Quickstart <data_quickstart>`.

.. _dataset_concept:

Key concepts
============

Datasets and blocks
-------------------

Datasets
~~~~~~~~

:class:`Dataset <ray.data.Dataset>` is the main user-facing Python API. It represents a 
distributed data collection, and defines data loading and processing operations. You 
typically use the API in this way:

1. Create a Ray Dataset from external storage or in-memory data.
2. Apply transformations to the data. 
3. Write the outputs to external storage or feed the outputs to training workers. 

Blocks
~~~~~~

A *block* is the basic unit of data bulk that Ray Data stores in the object store and 
transfers over the network. Each block contains a disjoint subset of rows, and Ray Data 
loads and transforms these blocks in parallel. 

The following figure visualizes a dataset with three blocks, each holding 1000 rows.
Ray Data holds the :class:`~ray.data.Dataset` on the process that triggers execution 
(which is usually the driver) and stores the blocks as objects in Ray's shared-memory 
:ref:`object store <objects-in-ray>`.

.. image:: images/dataset-arch.svg

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

Operators, plans, and planning
------------------------------

Operators
~~~~~~~~~

There are two types of operators: *logical operators* and *physical operators*. Logical 
operators are stateless objects that describe “what” to do. Physical operators are 
stateful objects that describe “how” to do it. An example of a logical operator is 
``ReadOp``, and an example of a physical operator is ``TaskPoolMapOperator``.

Plans
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

Execution
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

Scheduling
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
