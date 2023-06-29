.. _datasets_scheduling:

==================
Ray Data Internals
==================

This guide describes the implementation of Ray Data. The intended audience is advanced
users and Ray Data developers.

For a gentler introduction to Ray Data, see :ref:`Key concepts <data_key_concepts>`.

.. _dataset_concept:

Datasets and blocks
===================

A :class:`Dataset <ray.data.Dataset>` operates over a sequence of Ray object references
to :term:`blocks <Block>`. Each block contains a disjoint subset of rows, and Ray Data
loads and transforms these blocks in parallel.

The following figure visualizes a dataset with three blocks, each holding 1000 rows.

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Operations
==========

Reading files
-------------

Ray Data uses :ref:`Ray tasks <task-key-concept>` to read files in parallel. Each read
task reads one or more files and produces an output block:

.. image:: images/dataset-read.svg
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

To handle transient errors from remote datasources, Ray Data retries application-level
exceptions.

For more information on loading data, see :ref:`Loading data <loading_data>`.

Transforming data
-----------------

Ray Data uses either :ref:`Ray tasks <task-key-concept>` or
:ref:`Ray actors <actor-key-concept>` to transform blocks. By default, it uses tasks.

.. image:: images/dataset-map.svg
   :align: center
..
  https://docs.google.com/drawings/d/12STHGV0meGWfdWyBlJMUgw7a-JcFPu9BwSOn5BjRw9k/edit

For more information on transforming data, see
:ref:`Transforming data <transforming_data>`.

Shuffling data
--------------

When you call :meth:`~ray.data.Dataset.random_shuffle`,
:meth:`~ray.data.Dataset.sort`, or :meth:`~ray.data.Dataset.groupby`, Ray Data shuffles
blocks in a map-reduce style: map tasks partition blocks by value and then reduce tasks
merge co-partitioned blocks.

.. note::

    Shuffles materialize :class:`Datasets <ray.data.Dataset>` in memory. In other
    words, shuffle execution isn't streamed through memory.

For an in-depth guide on shuffle performance, see :ref:`Performance Tips and Tuning <shuffle_performance_tips>`.

Scheduling
==========

Ray Data uses Ray Core for execution, and is subject to the same scheduling considerations as normal Ray Tasks and Actors. Ray Data uses the following custom scheduling settings by default for improved performance:

* The ``SPREAD`` scheduling strategy ensures that data blocks and map tasks are evenly balanced across the cluster.
* Dataset tasks ignore placement groups by default, see :ref:`Ray Data and Placement Groups <datasets_pg>`.

.. _datasets_pg:

Ray Data and placement groups
-----------------------------

By default, Ray Data configures its tasks and actors to use the cluster-default scheduling strategy ("DEFAULT"). You can inspect this configuration variable here:
:class:`ray.data.DataContext.get_current().scheduling_strategy <ray.data.DataContext>`. This scheduling strategy schedules these Tasks and Actors outside any present
placement group. To force Ray Data to schedule tasks within the current placement group (i.e., to use current placement group resources specifically for Ray Data), set ``ray.data.DataContext.get_current().scheduling_strategy = None``.

Consider this override only for advanced use cases to improve performance predictability. The general recommendation is to let Ray Data run outside placement groups.

.. _datasets_tune:

Ray Data and Tune
-----------------

When using Ray Data in conjunction with :ref:`Ray Tune <tune-main>`, it is important to ensure there are enough free CPUs for Ray Data to run on. By default, Tune will try to fully utilize cluster CPUs. This can prevent Ray Data from scheduling tasks, reducing performance or causing workloads to hang.

To ensure CPU resources are always available for Ray Data execution, limit the number of concurrent Tune trials. This can be done using the ``max_concurrent_trials`` Tune option.

.. literalinclude:: ./doc_code/key_concepts.py
  :language: python
  :start-after: __resource_allocation_1_begin__
  :end-before: __resource_allocation_1_end__

.. _dataset_execution:

Execution
=========

Ray Data execution by default is:

- **Lazy**: This means that transformations on Dataset are not executed until a
  consumption operation (e.g. :meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`)
  or :meth:`Dataset.materialize() <ray.data.Dataset.materialize>` is called. This creates
  opportunities for optimizing the execution plan (e.g. :ref:`stage fusion <datasets_stage_fusion>`).
- **Streaming**: This means that Dataset transformations will be executed in a
  streaming way, incrementally on the base data, instead of on all of the data
  at once, and overlapping the execution of operations. This can be used for streaming
  data loading into ML training to overlap the data preprocessing and model training,
  or to execute batch transformations on large datasets without needing to load the
  entire dataset into cluster memory.

.. _datasets_lazy_execution:

Lazy Execution
--------------

Lazy execution offers opportunities for improved performance and memory stability due
to stage fusion optimizations and aggressive garbage collection of intermediate results.

Dataset creation and transformation APIs are lazy, with execution only triggered via "sink"
APIs, such as consuming (:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`),
writing (:meth:`ds.write_parquet() <ray.data.Dataset.write_parquet>`), or manually triggering via
:meth:`ds.materialize() <ray.data.Dataset.materialize>`. There are a few
exceptions to this rule, where transformations such as :meth:`ds.union()
<ray.data.Dataset.union>` and
:meth:`ds.limit() <ray.data.Dataset.limit>` trigger execution; we plan to make these
operations lazy in the future.

Check the API docs for Ray Data methods to see if they
trigger execution. Those that do trigger execution will have a ``Note`` indicating as
much.

.. _streaming_execution:

Streaming Execution
-------------------

The following code is a hello world example which invokes the execution with
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` consumption. We will also enable verbose progress reporting, which shows per-operator progress in addition to overall progress.

.. code-block::

   import ray
   import time

   # Enable verbose reporting. This can also be toggled on by setting
   # the environment variable RAY_DATA_VERBOSE_PROGRESS=1.
   ctx = ray.data.DataContext.get_current()
   ctx.execution_options.verbose_progress = True

   def sleep(x):
       time.sleep(0.1)
       return x

   for _ in (
       ray.data.range_tensor(5000, shape=(80, 80, 3), parallelism=200)
       .map_batches(sleep, num_cpus=2)
       .map_batches(sleep, compute=ray.data.ActorPoolStrategy(2, 4))
       .map_batches(sleep, num_cpus=1)
       .iter_batches()
   ):
       pass

This launches a simple 4-stage pipeline. We use different compute args for each stage, which forces them to be run as separate operators instead of getting fused together. You should see a log message indicating streaming execution is being used:

.. code-block::

   2023-03-30 16:40:10,076	INFO streaming_executor.py:83 -- Executing DAG InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange] -> TaskPoolMapOperator[MapBatches(sleep)] -> ActorPoolMapOperator[MapBatches(sleep)] -> TaskPoolMapOperator[MapBatches(sleep)]

The next few lines will show execution progress. Here is how to interpret the output:

.. code-block::

   Running: 7.0/16.0 CPU, 0.0/0.0 GPU, 76.91 MiB/2.25 GiB object_store_memory 65%|██▊ | 130/200 [00:08<00:02, 22.52it/s]

This line tells you how many resources are currently being used by the streaming executor out of the limits, as well as the number of completed output blocks. The streaming executor will attempt to keep resource usage under the printed limits by throttling task executions.

.. code-block::

   ReadRange: 2 active, 37 queued, 7.32 MiB objects 1:  80%|████████▊  | 161/200 [00:08<00:02, 17.81it/s]
   MapBatches(sleep): 5 active, 5 queued, 18.31 MiB objects 2:  76%|██▎| 151/200 [00:08<00:02, 19.93it/s]
   MapBatches(sleep): 7 active, 2 queued, 25.64 MiB objects, 2 actors [all objects local] 3:  71%|▋| 142/
   MapBatches(sleep): 2 active, 0 queued, 7.32 MiB objects 4:  70%|██▊ | 139/200 [00:08<00:02, 23.16it/s]

These lines are only shown when verbose progress reporting is enabled. The `active` count indicates the number of running tasks for the operator. The `queued` count is the number of input blocks for the operator that are computed but are not yet submitted for execution. For operators that use actor-pool execution, the number of running actors is shown as `actors`.

.. tip::

    Avoid returning large outputs from the final operation of a pipeline you are iterating over, since the consumer process will be a serial bottleneck.

Fault tolerance
---------------

Ray Data performs *lineage reconstruction* to recover data. If an application error or
system failure occurs, Ray Data recreates blocks by re-executing tasks.

.. note::

    Fault tolerance isn't supported if the process that created the
    :class:`~ray.data.Dataset` dies.


.. _datasets_stage_fusion:

Stage Fusion Optimization
-------------------------

In order to reduce memory usage and task overheads, Ray Data will automatically fuse together
lazy operations that are compatible:

* Same compute pattern: embarrassingly parallel map vs. all-to-all shuffle
* Same compute strategy: Ray tasks vs Ray actors
* Same resource specification, e.g. ``num_cpus`` or ``num_gpus`` requests

Read stages and subsequent map-like transformations will usually be fused together.
All-to-all transformations such as
:meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>` can be fused with earlier
map-like stages, but not later stages.

You can tell if stage fusion is enabled by checking the :ref:`Dataset stats <data_performance_tips>` and looking for fused stages (e.g., ``read->map_batches``).

.. code-block::

    Stage N read->map_batches->shuffle_map: N/N blocks executed in T
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Output num rows: N min, N max, N mean, N total

Memory Management
=================

This section describes how Ray Data manages execution and object store memory.

Execution Memory
----------------

During execution, a task can read multiple input blocks, and write multiple output blocks. Input and output blocks consume both worker heap memory and shared memory via Ray's object store.

Ray Data attempts to bound its heap memory usage to `num_execution_slots * max_block_size`. The number of execution slots is by default equal to the number of CPUs, unless custom resources are specified. The maximum block size is set by the configuration parameter `ray.data.DataContext.target_max_block_size` and is set to 512MiB by default. When a task's output is larger than this value, the worker will automatically split the output into multiple smaller blocks to avoid running out of heap memory.

Large block size can lead to potential out-of-memory situations. To avoid these issues, make sure no single item in your Ray Data is too large, and always call :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` with batch size small enough such that the output batch can comfortably fit into memory.

Object Store Memory
-------------------

Ray Data uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

* Object Spilling: Since Ray Data uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:
* Locality Scheduling: Ray will preferentially schedule compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.
* Reference Counting: Dataset blocks are kept alive by object store reference counting as long as there is any Dataset that references them. To free memory, delete any Python references to the Dataset object.
