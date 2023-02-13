.. _datasets_scheduling:

============================================
Scheduling, Execution, and Memory Management
============================================

Scheduling
==========

Datasets uses Ray core for execution, and hence is subject to the same scheduling considerations as normal Ray tasks and actors. Datasets uses the following custom scheduling settings by default for improved performance:

* The ``SPREAD`` scheduling strategy is used to ensure data blocks are evenly balanced across the cluster.
* Retries of application-level exceptions are enabled to handle transient errors from remote datasources.
* Dataset tasks ignore placement groups by default, see :ref:`Datasets and Placement Groups <datasets_pg>`.

.. _datasets_tune:

Datasets and Tune
~~~~~~~~~~~~~~~~~

When using Datasets in conjunction with :ref:`Ray Tune <tune-main>`, it is important to ensure there are enough free CPUs for Datasets to run on. By default, Tune will try to fully utilize cluster CPUs. This can prevent Datasets from scheduling tasks, reducing performance or causing workloads to hang.

As an example, the following shows two ways to use Datasets together with Tune:

.. tabbed:: Limiting Tune Concurrency

    By limiting the number of concurrent Tune trials, we ensure CPU resources are always available for Datasets execution.
    This can be done using the ``max_concurrent_trials`` Tune option.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_1_begin__
      :end-before: __resource_allocation_1_end__

.. tabbed:: Reserving CPUs (Experimental)

    Alternatively, we can tell Tune to set aside CPU resources for other libraries.
    This can be done by setting ``_max_cpu_fraction_per_node=0.8``, which reserves
    20% of node CPUs for Dataset execution.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_2_begin__
      :end-before: __resource_allocation_2_end__

    .. warning::

        This option is experimental and not currently recommended for use with
        autoscaling clusters (scale-up will not trigger properly).

.. _datasets_pg:

Datasets and Placement Groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Datasets configures its tasks and actors to use the cluster-default scheduling strategy ("DEFAULT"). You can inspect this configuration variable here:
:class:`ray.data.context.DatasetContext.get_current().scheduling_strategy <ray.data.context.DatasetContext>`. This scheduling strategy will schedule these tasks and actors outside any present
placement group. If you want to force Datasets to schedule tasks within the current placement group (i.e., to use current placement group resources specifically for Datasets), you can set ``ray.data.context.DatasetContext.get_current().scheduling_strategy = None``.

This should be considered for advanced use cases to improve performance predictability only. We generally recommend letting Datasets run outside placement groups as documented in the :ref:`Datasets and Other Libraries <datasets_tune>` section.

.. _datasets_execution:

Execution
=========

This section covers the Datasets execution model and performance considerations.

Lazy Execution
~~~~~~~~~~~~~~

Lazy execution offers opportunities for improved performance and memory stability due
to stage fusion optimizations and aggressive garbage collection of intermediate results.

Dataset creation and transformation APIs are lazy, with execution only triggered via "sink"
APIs, such as consuming (:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`),
writing (:meth:`ds.write_parquet() <ray.data.Dataset.write_parquet>`), or manually triggering via
:meth:`ds.fully_executed() <ray.data.Dataset.fully_executed>`. There are a few
exceptions to this rule, where transformations such as :meth:`ds.union()
<ray.data.Dataset.union>` and
:meth:`ds.limit() <ray.data.Dataset.limit>` trigger execution; we plan to make these
operations lazy in the future.

Check the API docs for Datasets methods to see if they
trigger execution. Those that do trigger execution will have a ``Note`` indicating as
much.

Stage Fusion Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~

In order to reduce memory usage and task overheads, Datasets will automatically fuse together
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

This section describes how Datasets manages execution and object store memory.

Execution Memory
~~~~~~~~~~~~~~~~

During execution, a task can read multiple input blocks, and write multiple output blocks. Input and output blocks consume both worker heap memory and shared memory via Ray's object store.

Datasets attempts to bound its heap memory usage to `num_execution_slots * max_block_size`. The number of execution slots is by default equal to the number of CPUs, unless custom resources are specified. The maximum block size is set by the configuration parameter `ray.data.context.DatasetContext.target_max_block_size` and is set to 512MiB by default. When a task's output is larger than this value, the worker will automatically split the output into multiple smaller blocks to avoid running out of heap memory.

Large block size can lead to potential out-of-memory situations. To avoid these issues, make sure no single item in your Datasets is too large, and always call :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` with batch size small enough such that the output batch can comfortably fit into memory.

Object Store Memory
~~~~~~~~~~~~~~~~~~~

Datasets uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

* Object Spilling: Since Datasets uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:
* Locality Scheduling: Ray will preferentially schedule compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.
* Reference Counting: Dataset blocks are kept alive by object store reference counting as long as there is any Dataset that references them. To free memory, delete any Python references to the Dataset object.

Block Data Formats
~~~~~~~~~~~~~~~~~~

In order to optimize conversion costs, Datasets can hold tabular data in-memory
as either `Arrow Tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
or `Pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.

Different ways of creating Datasets leads to a different starting internal format:

* Reading tabular files (Parquet, CSV, JSON) creates Arrow blocks initially.
* Converting from Pandas, Dask, Modin, and Mars creates Pandas blocks initially.
* Reading NumPy files or converting from NumPy ndarrays creates Arrow blocks.
* Reading TFRecord file creates Arrow blocks.
* Reading MongoDB creates Arrow blocks.

However, this internal format is not exposed to the user. Datasets converts between formats
as needed internally depending on the specified ``batch_format`` of transformations.
