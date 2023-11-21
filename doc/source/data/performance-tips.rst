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

.. _read_parallelism:

Tuning read parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray Data automatically selects the read ``parallelism`` according to the following procedure:

The ``parallelism`` parameter passed to Ray Data's :ref:`read APIs <input-output>` specifies the number of read tasks that should be used.
Usually, if the read is followed by a :func:`~ray.data.Dataset.map` or :func:`~ray.data.Dataset.map_batches`, the map is fused with the read; therefore ``parallelism`` also determines the parallelism used during map.

The default value for ``parallelism`` is decided based on the following heuristics, applied in order:

1. We start with the default parallelism of 200. This can be overridden by setting :class:`DataContext.min_parallelism <ray.data.context.DataContext>`.
2. Min block size (default=1MiB). If the parallelism would make blocks smaller than this threshold, the parallelism is reduced to avoid the overhead of tiny blocks. This can be overridden by setting :class:`DataContext.target_min_block_size <ray.data.context.DataContext>` (bytes).
3. Max block size (default=128MiB). If the parallelism would make blocks larger than this
threshold, the parallelism is increased to avoid OOMs during processing. This can be overridden by setting :class:`DataContext.target_max_block_size <ray.data.context.DataContext>` (bytes).
4. Available CPUs. If the parallelism cannot make use of all the available
CPUs in the cluster, the parallelism is increased until it can. To ensure utilization, Ray Data will attempt to produce at least 2x the available CPUs.

Occasionally, it's advantageous to manually tune the parallelism to optimize the application.
For example, the following code will batch multiple files into the same read task to avoid creating blocks that are too large, so the total number of output blocks will be less than the number of input files.

    .. testcode::

        import ray
        # Pretend we have two CPUs.
        ray.init(num_cpus=2)

        # Repeat the iris.csv file 16 times.
        ds = ray.data.read_images(["example://iris.csv"] * 16)
        print(ds.materialize())
        # 2023-11-20 14:28:47,597 INFO plan.py:760 -- Using autodetected parallelism=4 for stage ReadCSV to satisfy parallelism at least twice the available number of CPUs (2).
        # MaterializedDataset(
        #    num_blocks=4,
        #    num_rows=2400,
        #    ...
        # )

But suppose that you knew that you wanted to read all 16 files in parallel, for example because you know that additional CPUs should get added to the cluster by the autoscaler or because the downstream stage should transform each file's contents in parallel.
Then, you can do this by setting the ``parallelism`` parameter.
Notice how the number of output blocks is equal to ``parallelism`` in the following code:

    .. testcode::

        import ray
        # Pretend we have two CPUs.
        ray.init(num_cpus=2)

        # Repeat the iris.csv file 16 times.
        ds = ray.data.read_images(["example://iris.csv"] * 16, parallelism=16)
        print(ds.materialize())
        # MaterializedDataset(
        #    num_blocks=16,
        #    num_rows=2400,
        #    ...
        # )


Note that because Ray Data cannot perfectly predict the size of each task's output, it is possible that each task produces one or more output blocks, based on :class:`DataContext.target_max_block_size <ray.data.context.DataContext>`.
Thus, the total blocks produced in the final :class:`~ray.data.Dataset` may differ from the specified ``parallelism``.
Here's an example where we'll manually specify ``parallelism=1``, but the one task will still produce multiple blocks in the materialized Dataset:

    .. testcode::

        import ray
        # Pretend we have two CPUs.
        ray.init(num_cpus=2)

        # Generate ~400MB of data.
        ds = ray.data.range_tensor(5_000, shape=(10_000, ), parallelism=1)
        print(ds.materialize())
        # MaterializedDataset(
        #    num_blocks=3,
        #    num_rows=5000,
        #    schema={data: numpy.ndarray(shape=(10000,), dtype=int64)}
        # )


Currently, Ray Data can assign at most one read task per input file.
Thus, if the number of input files is smaller than the autodetected or specified ``parallelism``, Ray Data will cap the number of read tasks to the number of input files.
To ensure that downstream transforms can still execute with the desired parallelism, Ray Data will split the read tasks' outputs into a total of ``parallelism`` blocks and disable fusing with the downstream transform, i.e. the read tasks' output blocks will be materialized to Ray's object store before any map stage executes.
For example, in the following code, we will execute :func:`~ray.data.read_csv` with only one task, but its output will get split into 4 blocks before executing the :func:`~ray.data.Dataset.map`:

    .. testcode::

        import ray
        # Pretend we have two CPUs.
        ray.init(num_cpus=2)

        ds = ray.data.read_csv("example://iris.csv").map(lambda row: row)
        print(ds.materialize().stats())
        # 2023-11-20 15:47:02,404 INFO split_read_output_blocks.py:101 -- Using autodetected parallelism=4 for stage ReadCSV to satisfy parallelism at least twice the available number of CPUs (2).
        # 2023-11-20 15:47:02,405 INFO split_read_output_blocks.py:106 -- To satisfy the requested parallelism of 4, each read task output is split into 4 smaller blocks.
        # ...
        # Stage 1 ReadCSV->SplitBlocks(4): 4/4 blocks executed in 0.01s
        # ...
        # 
        # Stage 2 Map(<lambda>): 4/4 blocks executed in 0.03s
        # ...

To disable this behavior and allow the read and map stages to be fused, manually set ``parallelism``.
For example, here we set it to equal the number of files:

    .. testcode::

        import ray
        # Pretend we have two CPUs.
        ray.init(num_cpus=2)

        ds = ray.data.read_csv("example://iris.csv", parallelism=1).map(lambda row: row)
        print(ds.materialize().stats())
        # ...
        # Stage 1 ReadCSV->Map(<lambda>): 1/1 blocks executed in 0.03s
        # ...


Tuning read resources
~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 1 CPU per read task, which means one read task per CPU can execute concurrently.
For datasources that benefit from more IO parallelism, you can specify a lower ``num_cpus`` value for the read function with the ``ray_remote_args`` parameter.
For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

Parquet column pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Dataset reads all Parquet columns into memory.
If you only need a subset of the columns, make sure to specify the list of columns
explicitly when calling :meth:`ray.data.read_parquet() <ray.data.read_parquet>` to
avoid loading unnecessary data (projection pushdown).
For example, use ``ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet", columns=["sepal.length", "variety"])`` to read
just two of the five columns of Iris dataset.

.. _parquet_row_pruning:

Parquet row pruning
~~~~~~~~~~~~~~~~~~~

Similarly, you can pass in a filter to :meth:`ray.data.read_parquet() <ray.data.Dataset.read_parquet>` (filter pushdown)
which is applied at the file scan so only rows that match the filter predicate
are returned.
For example, use ``ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet", filter=pyarrow.dataset.field("sepal.length") > 5.0)``
(where ``pyarrow`` has to be imported)
to read rows with sepal.length greater than 5.0.
This can be used in conjunction with column pruning when appropriate to get the benefits of both.

.. _optimizing_shuffles:

Optimizing shuffles
-------------------

When should you use global per-epoch shuffling?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use global per-epoch shuffling only if your model is sensitive to the
randomness of the training data. Based on a
`theoretical foundation <https://arxiv.org/abs/1709.10432>`__ all
gradient-descent-based model trainers benefit from improved (global) shuffle quality.
In practice, the benefit is particularly pronounced for tabular data/models.
However, the more global the shuffle is, the more expensive the shuffling operation.
The increase compounds with distributed data-parallel training on a multi-node cluster due
to data transfer costs. This cost can be prohibitive when using very large datasets.

The best route for determining the best tradeoff between preprocessing time and cost and
per-epoch shuffle quality is to measure the precision gain per training step for your
particular model under different shuffling policies:

* no shuffling,
* local (per-shard) limited-memory shuffle buffer,
* local (per-shard) shuffling,
* windowed (pseudo-global) shuffling, and
* fully global shuffling.

As long as your data loading and shuffling throughput is higher than your training throughput, your GPU should
be saturated. If you have shuffle-sensitive models, push the
shuffle quality higher until this threshold is hit.

.. _shuffle_performance_tips:

Enabling push-based shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Dataset operations require a *shuffle* operation, meaning that data is shuffled from all of the input partitions to all of the output partitions.
These operations include :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`,
:meth:`Dataset.sort <ray.data.Dataset.sort>` and :meth:`Dataset.groupby <ray.data.Dataset.groupby>`.
Shuffle can be challenging to scale to large data sizes and clusters, especially when the total dataset size can't fit into memory.

Datasets provides an alternative shuffle implementation known as push-based shuffle for improving large-scale performance.
Try this out if your dataset has more than 1000 blocks or is larger than 1 TB in size.

To try this out locally or on a cluster, you can start with the `nightly release test <https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/sort.py>`_ that Ray runs for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` and :meth:`Dataset.sort <ray.data.Dataset.sort>`.
To get an idea of the performance you can expect, here are some run time results for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` on 1-10 TB of data on 20 machines (m5.4xlarge instances on AWS EC2, each with 16 vCPUs, 64 GB RAM).

.. image:: https://docs.google.com/spreadsheets/d/e/2PACX-1vQvBWpdxHsW0-loasJsBpdarAixb7rjoo-lTgikghfCeKPQtjQDDo2fY51Yc1B6k_S4bnYEoChmFrH2/pubchart?oid=598567373&format=image
   :align: center

To try out push-based shuffle, set the environment variable ``RAY_DATA_PUSH_BASED_SHUFFLE=1`` when running your application:

.. code-block:: bash

    $ wget https://raw.githubusercontent.com/ray-project/ray/master/release/nightly_tests/dataset/sort.py
    $ RAY_DATA_PUSH_BASED_SHUFFLE=1 python sort.py --num-partitions=10 --partition-size=1e7
    # Dataset size: 10 partitions, 0.01GB partition size, 0.1GB total
    # [dataset]: Run `pip install tqdm` to enable progress reporting.
    # 2022-05-04 17:30:28,806	INFO push_based_shuffle.py:118 -- Using experimental push-based shuffle.
    # Finished in 9.571171760559082
    # ...

You can also specify the shuffle implementation during program execution by
setting the ``DataContext.use_push_based_shuffle`` flag:

.. testcode::

    import ray

    ctx = ray.data.DataContext.get_current()
    ctx.use_push_based_shuffle = True

    ds = (
        ray.data.range(1000)
        .random_shuffle()
    )

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
    It is *not* recommended to modify the Ray Core object store memory limit, as this can reduce available memory for task execution. The one exception to this is if you are using machines with a very large amount of RAM (1TB or more each); then it is recommended to set the object store to ~30-40%.

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

Monitoring your application
---------------------------

View the Ray Data dashboard located in the :ref:`Metrics tab <dash-metrics-view>` of the Ray Dashboard to monitor your application and troubleshoot issues. Ray Data emits Prometheus metrics in real-time while a Dataset is executing, and the Ray Data dashboard displays these metrics grouped by Dataset. Datasets can also be assigned a name using :meth:`Dataset._set_name`, which prefixes the dataset ID for a more identifiable label.

The metrics recorded are:

* Bytes spilled by objects from object store to disk
* Bytes of objects allocated in object store
* Bytes of objects freed in object store
* Current total bytes of objects in object store
* Logical CPUs allocated to dataset operators
* Logical GPUs allocated to dataset operators
* Bytes outputted by dataset operators

.. image:: images/data-dashboard.png
   :align: center

To learn more about the Ray dashboard, including detailed setup instructions, see :ref:`Ray Dashboard <observability-getting-started>`.
