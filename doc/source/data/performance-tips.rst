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

1. The number of available CPUs is estimated. If in a placement group, the number of CPUs in the cluster is scaled by the size of the placement group compared to the cluster size. If not in a placement group, this is the number of CPUs in the cluster.
2. The parallelism is set to the estimated number of CPUs multiplied by 2. If the parallelism is less than 8, it's set to 8.
3. The in-memory data size is estimated. If the parallelism would create in-memory blocks that are larger on average than the target block size (512MiB), the parallelism is increased until the blocks are < 512MiB in size.

Occasionally, it's advantageous to manually tune the parallelism to optimize the application. This can be done when loading data via the ``parallelism`` parameter.
For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to force up to 1000 read tasks to be created.

Tuning read resources
~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 1 CPU per read task, which means one read tasks per CPU can execute concurrently.
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

View the Ray Dashboard to monitor your application and troubleshoot issues. To learn
more about the Ray dashboard, see :ref:`Ray Dashboard <observability-getting-started>`.
