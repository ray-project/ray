.. _datastreams_scheduling:

============================================
Scheduling, Execution, and Memory Management
============================================

Scheduling
==========

Ray Data uses Ray core for execution, and hence is subject to the same scheduling considerations as normal Ray tasks and actors. Ray Data uses the following custom scheduling settings by default for improved performance:

* The ``SPREAD`` scheduling strategy is used to ensure data blocks are evenly balanced across the cluster.
* Retries of application-level exceptions are enabled to handle transient errors from remote datasources.
* Datastream tasks ignore placement groups by default, see :ref:`Ray Data and Placement Groups <datastreams_pg>`.

.. _datastreams_tune:

Ray Data and Tune
~~~~~~~~~~~~~~~~~

When using Ray Data in conjunction with :ref:`Ray Tune <tune-main>`, it is important to ensure there are enough free CPUs for Ray Data to run on. By default, Tune will try to fully utilize cluster CPUs. This can prevent Ray Data from scheduling tasks, reducing performance or causing workloads to hang.

As an example, the following shows two ways to use Ray Data together with Tune:

.. tab-set::

    .. tab-item:: Limiting Tune Concurrency

        By limiting the number of concurrent Tune trials, we ensure CPU resources are always available for Ray Data execution.
        This can be done using the ``max_concurrent_trials`` Tune option.

        .. literalinclude:: ./doc_code/key_concepts.py
          :language: python
          :start-after: __resource_allocation_1_begin__
          :end-before: __resource_allocation_1_end__

    .. tab-item:: Reserving CPUs (Experimental)

        Alternatively, we can tell Tune to set aside CPU resources for other libraries.
        This can be done by setting ``_max_cpu_fraction_per_node=0.8``, which reserves
        20% of node CPUs for Datastream execution.

        .. literalinclude:: ./doc_code/key_concepts.py
          :language: python
          :start-after: __resource_allocation_2_begin__
          :end-before: __resource_allocation_2_end__

        .. warning::

            This option is experimental and not currently recommended for use with
            autoscaling clusters (scale-up will not trigger properly).

.. _datastreams_pg:

Ray Data and Placement Groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray Data configures its tasks and actors to use the cluster-default scheduling strategy ("DEFAULT"). You can inspect this configuration variable here:
:class:`ray.data.DataContext.get_current().scheduling_strategy <ray.data.DataContext>`. This scheduling strategy will schedule these tasks and actors outside any present
placement group. If you want to force Ray Data to schedule tasks within the current placement group (i.e., to use current placement group resources specifically for Ray Data), you can set ``ray.data.DataContext.get_current().scheduling_strategy = None``.

This should be considered for advanced use cases to improve performance predictability only. We generally recommend letting Ray Data run outside placement groups as documented in the :ref:`Ray Data and Other Libraries <datastreams_tune>` section.

.. _datastream_execution:

Execution
=========

Ray Data execution by default is:

- **Lazy**: This means that transformations on Datastream are not executed until a
  consumption operation (e.g. :meth:`ds.iter_batches() <ray.data.Datastream.iter_batches>`)
  or :meth:`Datastream.materialize() <ray.data.Datastream.materialize>` is called. This creates
  opportunities for optimizing the execution plan (e.g. :ref:`stage fusion <datastreams_stage_fusion>`).
- **Streaming**: This means that Datastream transformations will be executed in a
  streaming way, incrementally on the base data, instead of on all of the data
  at once, and overlapping the execution of operations. This can be used for streaming
  data loading into ML training to overlap the data preprocessing and model training,
  or to execute batch transformations on large datastreams without needing to load the
  entire datastream into cluster memory.

.. _datastreams_lazy_execution:

Lazy Execution
~~~~~~~~~~~~~~

Lazy execution offers opportunities for improved performance and memory stability due
to stage fusion optimizations and aggressive garbage collection of intermediate results.

Datastream creation and transformation APIs are lazy, with execution only triggered via "sink"
APIs, such as consuming (:meth:`ds.iter_batches() <ray.data.Datastream.iter_batches>`),
writing (:meth:`ds.write_parquet() <ray.data.Datastream.write_parquet>`), or manually triggering via
:meth:`ds.materialize() <ray.data.Datastream.materialize>`. There are a few
exceptions to this rule, where transformations such as :meth:`ds.union()
<ray.data.Datastream.union>` and
:meth:`ds.limit() <ray.data.Datastream.limit>` trigger execution; we plan to make these
operations lazy in the future.

Check the API docs for Ray Data methods to see if they
trigger execution. Those that do trigger execution will have a ``Note`` indicating as
much.

.. _streaming_execution:

Streaming Execution
~~~~~~~~~~~~~~~~~~~

The following code is a hello world example which invokes the execution with
:meth:`ds.iter_batches() <ray.data.Datastream.iter_batches>` consumption. We will also enable verbose progress reporting, which shows per-operator progress in addition to overall progress.

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

Configuring Resources and Locality
----------------------------------

By default, the CPU and GPU limits are set to the cluster size, and the object store memory limit conservatively to 1/4 of the total object store size to avoid the possibility of disk spilling.

You may want to customize these limits in the following scenarios:
- If running multiple concurrent jobs on the cluster, setting lower limits can avoid resource contention between the jobs.
- If you want to fine-tune the memory limit to maximize performance.
- For data loading into training jobs, you may want to set the object store memory to a low value (e.g., 2GB) to limit resource usage.

Execution options can be configured via the global DataContext. The options will be applied for future jobs launched in the process:

.. code-block::

   ctx = ray.data.DataContext.get_current()
   ctx.execution_options.resource_limits.cpu = 10
   ctx.execution_options.resource_limits.gpu = 5
   ctx.execution_options.resource_limits.object_store_memory = 10e9

Deterministic Execution
-----------------------

.. code-block::

   # By default, this is set to False.
   ctx.execution_options.preserve_order = True

To enable deterministic execution, set the above to True. This may decrease performance, but will ensure block ordering is preserved through execution. This flag defaults to False.

Actor Locality Optimization (ML inference use case)
---------------------------------------------------

.. code-block::

   # By default, this is set to True already.
   ctx.execution_options.actor_locality_enabled = True

The actor locality optimization (if you're using actor pools) tries to schedule objects that are already local to an actor's node to the same actor. This reduces network traffic across nodes. When actor locality is enabled, you'll see a report in the progress output of the hit rate:

.. code-block::

   MapBatches(Model): 0 active, 0 queued, 0 actors [992 locality hits, 8 misses]: 100%|██████████| 1000/1000 [00:59<00:00, 16.84it/s]

Locality with Output (ML ingest use case)
-----------------------------------------

.. code-block::

   ctx.execution_options.locality_with_output = True

Setting this to True tells Ray Data to prefer placing operator tasks onto the consumer node in the cluster, rather than spreading them evenly across the cluster. This can be useful if you know you'll be consuming the output data directly on the consumer node (i.e., for ML training ingest). However, this may incur a performance penalty for other use cases.

Scalability
-----------
We expect the data streaming backend to scale to tens of thousands of files / blocks and up to hundreds of terabytes of data. Please report if you experience performance degradation at these scales, we would be very interested to investigate!

.. _datastreams_stage_fusion:

Stage Fusion Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~

In order to reduce memory usage and task overheads, Ray Data will automatically fuse together
lazy operations that are compatible:

* Same compute pattern: embarrassingly parallel map vs. all-to-all shuffle
* Same compute strategy: Ray tasks vs Ray actors
* Same resource specification, e.g. ``num_cpus`` or ``num_gpus`` requests

Read stages and subsequent map-like transformations will usually be fused together.
All-to-all transformations such as
:meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>` can be fused with earlier
map-like stages, but not later stages.

You can tell if stage fusion is enabled by checking the :ref:`Datastream stats <data_performance_tips>` and looking for fused stages (e.g., ``read->map_batches``).

.. code-block::

    Stage N read->map_batches->shuffle_map: N/N blocks executed in T
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Output num rows: N min, N max, N mean, N total

Memory Management
=================

This section describes how Ray Data manages execution and object store memory.

Execution Memory
~~~~~~~~~~~~~~~~

During execution, a task can read multiple input blocks, and write multiple output blocks. Input and output blocks consume both worker heap memory and shared memory via Ray's object store.

Ray Data attempts to bound its heap memory usage to `num_execution_slots * max_block_size`. The number of execution slots is by default equal to the number of CPUs, unless custom resources are specified. The maximum block size is set by the configuration parameter `ray.data.DataContext.target_max_block_size` and is set to 512MiB by default. When a task's output is larger than this value, the worker will automatically split the output into multiple smaller blocks to avoid running out of heap memory.

Large block size can lead to potential out-of-memory situations. To avoid these issues, make sure no single item in your Ray Data is too large, and always call :meth:`ds.map_batches() <ray.data.Datastream.map_batches>` with batch size small enough such that the output batch can comfortably fit into memory.

Object Store Memory
~~~~~~~~~~~~~~~~~~~

Ray Data uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

* Object Spilling: Since Ray Data uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:
* Locality Scheduling: Ray will preferentially schedule compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.
* Reference Counting: Datastream blocks are kept alive by object store reference counting as long as there is any Datastream that references them. To free memory, delete any Python references to the Datastream object.

Block Data Formats
~~~~~~~~~~~~~~~~~~

In order to optimize conversion costs, Ray Data can hold tabular data in-memory
as either `Arrow Tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
or `Pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.

Different ways of creating Ray Data leads to a different starting internal format:

* Reading tabular files (Parquet, CSV, JSON) creates Arrow blocks initially.
* Converting from Pandas, Dask, Modin, and Mars creates Pandas blocks initially.
* Reading NumPy files or converting from NumPy ndarrays creates Arrow blocks.
* Reading TFRecord file creates Arrow blocks.
* Reading MongoDB creates Arrow blocks.

However, this internal format is not exposed to the user. Ray Data converts between formats
as needed internally depending on the specified ``batch_format`` of transformations.
