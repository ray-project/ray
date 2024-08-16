.. _data_performance_tips:

Advanced: Performance Tips and Tuning
=====================================

Optimizing transforms
---------------------

Batching transforms
~~~~~~~~~~~~~~~~~~~

If your transformation is vectorized like most NumPy or pandas operations, use
:meth:`~ray.data.Dataset.map_batches` rather than :meth:`~ray.data.Dataset.map`. It's
faster.

If your transformation isn't vectorized, there's no performance benefit.

Optimizing reads
----------------

.. _read_output_blocks:

Tuning output blocks for read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray Data automatically selects the number of output blocks for read according to the following procedure:

- The ``override_num_blocks`` parameter passed to Ray Data's :ref:`read APIs <input-output>` specifies the number of output blocks, which is equivalent to the number of read tasks to create.
- Usually, if the read is followed by a :func:`~ray.data.Dataset.map` or :func:`~ray.data.Dataset.map_batches`, the map is fused with the read; therefore ``override_num_blocks`` also determines the number of map tasks.

Ray Data decides the default value for number of output blocks based on the following heuristics, applied in order:

1. Start with the default value of 200. You can overwrite this by setting :class:`DataContext.read_op_min_num_blocks <ray.data.context.DataContext>`.
2. Min block size (default=1 MiB). If number of blocks would make blocks smaller than this threshold, reduce number of blocks to avoid the overhead of tiny blocks. You can override by setting :class:`DataContext.target_min_block_size <ray.data.context.DataContext>` (bytes).
3. Max block size (default=128 MiB). If number of blocks would make blocks larger than this threshold, increase number of blocks to avoid out-of-memory errors during processing. You can override by setting :class:`DataContext.target_max_block_size <ray.data.context.DataContext>` (bytes).
4. Available CPUs. Increase number of blocks to utilize all of the available CPUs in the cluster. Ray Data chooses the number of read tasks to be at least 2x the number of available CPUs.

Occasionally, it's advantageous to manually tune the number of blocks to optimize the application.
For example, the following code batches multiple files into the same read task to avoid creating blocks that are too large.

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    # Repeat the iris.csv file 16 times.
    ds = ray.data.read_csv(["example://iris.csv"] * 16)
    print(ds.materialize())

.. testoutput::
    :options: +MOCK

    MaterializedDataset(
       num_blocks=4,
       num_rows=2400,
       ...
    )

But suppose that you knew that you wanted to read all 16 files in parallel.
This could be, for example, because you know that additional CPUs should get added to the cluster by the autoscaler or because you want the downstream operator to transform each file's contents in parallel.
You can get this behavior by setting the ``override_num_blocks`` parameter.
Notice how the number of output blocks is equal to ``override_num_blocks`` in the following code:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    # Repeat the iris.csv file 16 times.
    ds = ray.data.read_csv(["example://iris.csv"] * 16, override_num_blocks=16)
    print(ds.materialize())

.. testoutput::
    :options: +MOCK

    MaterializedDataset(
       num_blocks=16,
       num_rows=2400,
       ...
    )


When using the default auto-detected number of blocks, Ray Data attempts to cap each task's output to :class:`DataContext.target_max_block_size <ray.data.context.DataContext>` many bytes.
Note however that Ray Data can't perfectly predict the size of each task's output, so it's possible that each task produces one or more output blocks.
Thus, the total blocks in the final :class:`~ray.data.Dataset` may differ from the specified ``override_num_blocks``.
Here's an example where we manually specify ``override_num_blocks=1``, but the one task still produces multiple blocks in the materialized Dataset:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    # Generate ~400MB of data.
    ds = ray.data.range_tensor(5_000, shape=(10_000, ), override_num_blocks=1)
    print(ds.materialize())

.. testoutput::
    :options: +MOCK

    MaterializedDataset(
       num_blocks=3,
       num_rows=5000,
       schema={data: numpy.ndarray(shape=(10000,), dtype=int64)}
    )


Currently, Ray Data can assign at most one read task per input file.
Thus, if the number of input files is smaller than ``override_num_blocks``, the number of read tasks is capped to the number of input files.
To ensure that downstream transforms can still execute with the desired number of blocks, Ray Data splits the read tasks' outputs into a total of ``override_num_blocks`` blocks and prevents fusion with the downstream transform.
In other words, each read task's output blocks are materialized to Ray's object store before the consuming map task executes.
For example, the following code executes :func:`~ray.data.read_csv` with only one task, but its output is split into 4 blocks before executing the :func:`~ray.data.Dataset.map`:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    ds = ray.data.read_csv("example://iris.csv").map(lambda row: row)
    print(ds.materialize().stats())

.. testoutput::
    :options: +MOCK

    ...
    Operator 1 ReadCSV->SplitBlocks(4): 1 tasks executed, 4 blocks produced in 0.01s
    ...
    
    Operator 2 Map(<lambda>): 4 tasks executed, 4 blocks produced in 0.3s
    ...

To turn off this behavior and allow the read and map operators to be fused, set ``override_num_blocks`` manually.
For example, this code sets the number of files equal to ``override_num_blocks``:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    ds = ray.data.read_csv("example://iris.csv", override_num_blocks=1).map(lambda row: row)
    print(ds.materialize().stats())

.. testoutput::
    :options: +MOCK

    ...
    Operator 1 ReadCSV->Map(<lambda>): 1 tasks executed, 1 blocks produced in 0.01s
    ...


.. _tuning_read_resources:

Tuning read resources
~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 1 CPU per read task, which means one read task per CPU can execute concurrently.
For datasources that benefit from more IO parallelism, you can specify a lower ``num_cpus`` value for the read function with the ``ray_remote_args`` parameter.
For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

Parquet column pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Dataset reads all Parquet columns into memory.
If you only need a subset of the columns, make sure to specify the list of columns
explicitly when calling :func:`ray.data.read_parquet` to
avoid loading unnecessary data (projection pushdown).
For example, use ``ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet", columns=["sepal.length", "variety"])`` to read
just two of the five columns of Iris dataset.

.. _parquet_row_pruning:

Parquet row pruning
~~~~~~~~~~~~~~~~~~~

Similarly, you can pass in a filter to :func:`ray.data.read_parquet` (filter pushdown)
which is applied at the file scan so only rows that match the filter predicate
are returned.
For example, use ``ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet", filter=pyarrow.dataset.field("sepal.length") > 5.0)``
(where ``pyarrow`` has to be imported)
to read rows with sepal.length greater than 5.0.
This can be used in conjunction with column pruning when appropriate to get the benefits of both.


.. _data_memory:

Reducing memory usage
---------------------

.. _data_out_of_memory:

Troubleshooting out-of-memory errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During execution, a task can read multiple input blocks, and write multiple output blocks. Input and output blocks consume both worker heap memory and shared memory through Ray's object store.
Ray caps object store memory usage by spilling to disk, but excessive worker heap memory usage can cause out-of-memory situations.

Ray Data attempts to bound its heap memory usage to ``num_execution_slots * max_block_size``. The number of execution slots is by default equal to the number of CPUs, unless custom resources are specified.
The maximum block size is set by the configuration parameter :class:`DataContext.target_max_block_size <ray.data.context.DataContext>` and is set to 128MiB by default.
If the Dataset includes an :ref:`all-to-all shuffle operation <optimizing_shuffles>` (such as :func:`~ray.data.Dataset.random_shuffle`), then the default maximum block size is controlled by :class:`DataContext.target_shuffle_max_block_size <ray.data.context.DataContext>`, set to 1GiB by default to avoid creating too many tiny blocks.

.. note::
    It's **not** recommended to modify :class:`DataContext.target_max_block_size <ray.data.context.DataContext>`. The default is already chosen to balance between high overheads from too many tiny blocks vs. excessive heap memory usage from too-large blocks.

When a task's output is larger than the maximum block size, the worker automatically splits the output into multiple smaller blocks to avoid running out of heap memory.
However, too-large blocks are still possible, and they can lead to out-of-memory situations.
To avoid these issues:

1. Make sure no single item in your dataset is too large. Aim for rows that are <10 MB each.
2. Always call :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` with a batch size small enough such that the output batch can comfortably fit into heap memory. Or, if vectorized execution is not necessary, use :meth:`ds.map() <ray.data.Dataset.map>`.
3. If neither of these is sufficient, manually increase the :ref:`read output blocks <read_output_blocks>` or modify your application code to ensure that each task reads a smaller amount of data.

As an example of tuning batch size, the following code uses one task to load a 1 GB :class:`~ray.data.Dataset` with 1000 1 MB rows and applies an identity function using :func:`~ray.data.Dataset.map_batches`.
Because the default ``batch_size`` for :func:`~ray.data.Dataset.map_batches` is 1024 rows, this code produces only one very large batch, causing the heap memory usage to increase to 4 GB.

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    # Force Ray Data to use one task to show the memory issue.
    ds = ray.data.range_tensor(1000, shape=(125_000, ), override_num_blocks=1)
    # The default batch size is 1024 rows.
    ds = ds.map_batches(lambda batch: batch)
    print(ds.materialize().stats())

.. testoutput::
    :options: +MOCK

    Operator 1 ReadRange->MapBatches(<lambda>): 1 tasks executed, 7 blocks produced in 1.33s
      ...
    * Peak heap memory usage (MiB): 3302.17 min, 4233.51 max, 4100 mean
    * Output num rows: 125 min, 125 max, 125 mean, 1000 total
    * Output size bytes: 134000536 min, 196000784 max, 142857714 mean, 1000004000 total
      ...

Setting a lower batch size produces lower peak heap memory usage:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    ds = ray.data.range_tensor(1000, shape=(125_000, ), override_num_blocks=1)
    ds = ds.map_batches(lambda batch: batch, batch_size=32)
    print(ds.materialize().stats())

.. testoutput::
    :options: +MOCK

    Operator 1 ReadRange->MapBatches(<lambda>): 1 tasks executed, 7 blocks produced in 0.51s
    ...
    * Peak heap memory usage (MiB): 587.09 min, 1569.57 max, 1207 mean
    * Output num rows: 40 min, 160 max, 142 mean, 1000 total
    * Output size bytes: 40000160 min, 160000640 max, 142857714 mean, 1000004000 total
    ...

Improving heap memory usage in Ray Data is an active area of development.
Here are the current known cases in which heap memory usage may be very high:

1. Reading large (1 GiB or more) binary files.
2. Transforming a Dataset where individual rows are large (100 MiB or more).

In these cases, the last resort is to reduce the number of concurrent execution slots.
This can be done with custom resources.
For example, use :meth:`ds.map_batches(fn, num_cpus=2) <ray.data.Dataset.map_batches>` to halve the number of execution slots for the ``map_batches`` tasks.

If these strategies are still insufficient, `file a Ray Data issue on GitHub`_.


Avoiding object spilling
~~~~~~~~~~~~~~~~~~~~~~~~

A Dataset's intermediate and output blocks are stored in Ray's object store.
Although Ray Data attempts to minimize object store usage with :ref:`streaming execution <streaming_execution>`, it's still possible that the working set exceeds the object store capacity.
In this case, Ray begins spilling blocks to disk, which can slow down execution significantly or even cause out-of-disk errors.

There are some cases where spilling is expected. In particular, if the total Dataset's size is larger than object store capacity, and one of the following is true:

1. An :ref:`all-to-all shuffle operation <optimizing_shuffles>` is used. Or,
2. There is a call to :meth:`ds.materialize() <ray.data.Dataset.materialize>`.

Otherwise, it's best to tune your application to avoid spilling.
The recommended strategy is to manually increase the :ref:`read output blocks <read_output_blocks>` or modify your application code to ensure that each task reads a smaller amount of data.

.. note:: This is an active area of development. If your Dataset is causing spilling and you don't know why, `file a Ray Data issue on GitHub`_.

Handling too-small blocks
~~~~~~~~~~~~~~~~~~~~~~~~~

When different operators of your Dataset produce different-sized outputs, you may end up with very small blocks, which can hurt performance and even cause crashes from excessive metadata.
Use :meth:`ds.stats() <ray.data.Dataset.stats>` to check that each operator's output blocks are each at least 1 MB and ideally 100 MB.

If your blocks are smaller than this, consider repartitioning into larger blocks.
There are two ways to do this:

1. If you need control over the exact number of output blocks, use :meth:`ds.repartition(num_partitions) <ray.data.Dataset.repartition>`. Note that this is an :ref:`all-to-all operation <optimizing_shuffles>` and it materializes all blocks into memory before performing the repartition.
2. If you don't need control over the exact number of output blocks and just want to produce larger blocks, use :meth:`ds.map_batches(lambda batch: batch, batch_size=batch_size) <ray.data.Dataset.map_batches>` and set ``batch_size`` to the desired number of rows per block. This is executed in a streaming fashion and avoids materialization.

When :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` is used, Ray Data coalesces blocks so that each map task can process at least this many rows.
Note that the chosen ``batch_size`` is a lower bound on the task's input block size but it does not necessarily determine the task's final *output* block size; see :ref:`the section <data_out_of_memory>` on block memory usage for more information on how block size is determined.

To illustrate these, the following code uses both strategies to coalesce the 10 tiny blocks with 1 row each into 1 larger block with 10 rows:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray
    # Pretend there are two CPUs.
    ray.init(num_cpus=2)

    # 1. Use ds.repartition().
    ds = ray.data.range(10, override_num_blocks=10).repartition(1)
    print(ds.materialize().stats())

    # 2. Use ds.map_batches().
    ds = ray.data.range(10, override_num_blocks=10).map_batches(lambda batch: batch, batch_size=10)
    print(ds.materialize().stats())

.. testoutput::
    :options: +MOCK

    # 1. ds.repartition() output.
    Operator 1 ReadRange: 10 tasks executed, 10 blocks produced in 0.33s
    ...
    * Output num rows: 1 min, 1 max, 1 mean, 10 total
    ...
    Operator 2 Repartition: executed in 0.36s

            Suboperator 0 RepartitionSplit: 10 tasks executed, 10 blocks produced
            ...

            Suboperator 1 RepartitionReduce: 1 tasks executed, 1 blocks produced
            ...
            * Output num rows: 10 min, 10 max, 10 mean, 10 total
            ...


    # 2. ds.map_batches() output.
    Operator 1 ReadRange->MapBatches(<lambda>): 1 tasks executed, 1 blocks produced in 0s
    ...
    * Output num rows: 10 min, 10 max, 10 mean, 10 total

Configuring execution
---------------------

Configuring resources and locality
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the CPU and GPU limits are set to the cluster size, and the object store memory limit conservatively to 1/4 of the total object store size to avoid the possibility of disk spilling.

You may want to customize these limits in the following scenarios:
- If running multiple concurrent jobs on the cluster, setting lower limits can avoid resource contention between the jobs.
- If you want to fine-tune the memory limit to maximize performance.
- For data loading into training jobs, you may want to set the object store memory to a low value (for example, 2 GB) to limit resource usage.

You can configure execution options with the global DataContext. The options are applied for future jobs launched in the process:

.. code-block::

   ctx = ray.data.DataContext.get_current()
   ctx.execution_options.resource_limits.cpu = 10
   ctx.execution_options.resource_limits.gpu = 5
   ctx.execution_options.resource_limits.object_store_memory = 10e9

.. note::
    It's **not** recommended to modify the Ray Core object store memory limit, as this can reduce available memory for task execution. The one exception to this is if you are using machines with a very large amount of RAM (1 TB or more each); then it's recommended to set the object store to ~30-40%.

Locality with output (ML ingest use case)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   ctx.execution_options.locality_with_output = True

Setting this parameter to True tells Ray Data to prefer placing operator tasks onto the consumer node in the cluster, rather than spreading them evenly across the cluster. This setting can be useful if you know you are consuming the output data directly on the consumer node (such as, for ML training ingest). However, other use cases may incur a performance penalty with this setting.

Reproducibility
---------------

Deterministic execution
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

   # By default, this is set to False.
   ctx.execution_options.preserve_order = True

To enable deterministic execution, set the preceding to True. This setting may decrease performance, but ensures block ordering is preserved through execution. This flag defaults to False.


.. _`file a Ray Data issue on GitHub`: https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdata&projects=&template=bug-report.yml&title=[data]+
