.. _data_performance_tips:

Performance Tips and Tuning
===========================

Debugging Statistics
~~~~~~~~~~~~~~~~~~~~

You can view debug stats for your Datastream executions via :meth:`ds.stats() <ray.data.Datastream.stats>`.
These stats can be used to understand the performance of your Datastream workload and can help you debug problematic bottlenecks. Note that both execution and iterator statistics are available:

.. code-block:: python

    import ray
    import time

    def pause(x):
        time.sleep(.0001)
        return x

    ds = ray.data.range(10000)
    ds = ds.map(lambda x: str(x + 1))
    ds = ds.map(pause)

    for x in ds.iter_batches():
        pass

    print(ds.stats())

.. code-block::

    Stage 1 ReadRange->Map->Map: 16/16 blocks executed in 0.37s                                                                                                                                                
    * Remote wall time: 101.55ms min, 331.39ms max, 135.24ms mean, 2.16s total
    * Remote cpu time: 7.42ms min, 15.88ms max, 11.01ms mean, 176.15ms total
    * Peak heap memory usage (MiB): 157.18 min, 157.73 max, 157 mean
    * Output num rows: 625 min, 625 max, 625 mean, 10000 total
    * Output size bytes: 3658 min, 4392 max, 4321 mean, 69150 total
    * Tasks per node: 16 min, 16 max, 16 mean; 1 nodes used
    * Extra metrics: {'obj_store_mem_alloc': 3658, 'obj_store_mem_freed': 5000, 'obj_store_mem_peak': 40000}

    Datastream iterator time breakdown:
    * Total time user code is blocked: 551.67ms
    * Total time in user code: 144.97us
    * Total time overall: 1.01s
    * Num blocks local: 0
    * Num blocks remote: 0
    * Num blocks unknown location: 16
    * Batch iteration time breakdown (summed across prefetch threads):
        * In ray.get(): 75.68us min, 220.26us max, 131.89us avg, 2.11ms total
        * In batch creation: 326.58us min, 1.37ms max, 644.86us avg, 25.79ms total
        * In batch formatting: 101.81us min, 898.73us max, 172.38us avg, 6.9ms total

Batching Transforms
~~~~~~~~~~~~~~~~~~~

Mapping individual records using :meth:`.map(fn) <ray.data.Datastream.map>` can be quite slow.
Instead, consider using :meth:`.map_batches(batch_fn, batch_format="pandas") <ray.data.Datastream.map_batches>` and writing your ``batch_fn`` to
perform vectorized pandas operations.

.. _data_format_overheads:

Format Overheads
~~~~~~~~~~~~~~~~

Converting between the internal block types (Arrow, Pandas)
and the requested batch format (``"numpy"``, ``"pandas"``, ``"pyarrow"``)
may incur data copies; which conversions cause data copying is given in the below table:


.. list-table:: Data Format Conversion Costs
   :header-rows: 1
   :stub-columns: 1

   * - Block Type x Batch Format
     - ``"pandas"``
     - ``"numpy"``
     - ``"pyarrow"``
     - ``None``
   * - Pandas Block
     - Zero-copy
     - Copy*
     - Copy*
     - Zero-copy
   * - Arrow Block
     - Copy*
     - Zero-copy*
     - Zero-copy
     - Zero-copy

.. note::
  \* No copies occur when converting between Arrow, Pandas, and NumPy formats for columns
  represented in the Ray Data tensor extension type (except for bool arrays).


Parquet Column Pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Datastream will read all Parquet columns into memory.
If you only need a subset of the columns, make sure to specify the list of columns
explicitly when calling :meth:`ray.data.read_parquet() <ray.data.read_parquet>` to
avoid loading unnecessary data (projection pushdown).
For example, use ``ray.data.read_parquet("example://iris.parquet", columns=["sepal.length", "variety"])`` to read
just two of the five columns of Iris datastream.

Parquet Row Pruning
~~~~~~~~~~~~~~~~~~~

Similarly, you can pass in a filter to :meth:`ray.data.read_parquet() <ray.data.Datastream.read_parquet>` (filter pushdown)
which will be applied at the file scan so only rows that match the filter predicate
will be returned.
For example, use ``ray.data.read_parquet("example://iris.parquet", filter=pyarrow.datastream.field("sepal.length") > 5.0)``
(where ``pyarrow`` has to be imported)
to read rows with sepal.length greater than 5.0.
This can be used in conjunction with column pruning when appropriate to get the benefits of both.

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray Data automatically selects the read ``parallelism`` according to the following procedure:

1. The number of available CPUs is estimated. If in a placement group, the number of CPUs in the cluster is scaled by the size of the placement group compared to the cluster size. If not in a placement group, this is the number of CPUs in the cluster.
2. The parallelism is set to the estimated number of CPUs multiplied by 2. If the parallelism is less than 8, it is set to 8.
3. The in-memory data size is estimated. If the parallelism would create in-memory blocks that are larger on average than the target block size (512MiB), the parallelism is increased until the blocks are < 512MiB in size.
4. The parallelism is truncated to ``min(num_files, parallelism)``.

Occasionally, it is advantageous to manually tune the parallelism to optimize the application. This can be done when loading data via the ``parallelism`` parameter.
For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to force up to 1000 read tasks to be created.

Tuning Read Resources
~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 1 CPU per read task, which means one read tasks per CPU can execute concurrently.
For data sources that can benefit from higher degress of I/O parallelism, you can specify a lower ``num_cpus`` value for the read function via the ``ray_remote_args`` parameter.
For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

.. _shuffle_performance_tips:

Enabling Push-Based Shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Datastream operations require a *shuffle* operation, meaning that data is shuffled from all of the input partitions to all of the output partitions.
These operations include :meth:`Datastream.random_shuffle <ray.data.Datastream.random_shuffle>`,
:meth:`Datastream.sort <ray.data.Datastream.sort>` and :meth:`Datastream.groupby <ray.data.Datastream.groupby>`.
Shuffle can be challenging to scale to large data sizes and clusters, especially when the total datastream size cannot fit into memory.

Datastreams provides an alternative shuffle implementation known as push-based shuffle for improving large-scale performance.
We recommend trying this out if your datastream has more than 1000 blocks or is larger than 1 TB in size.

To try this out locally or on a cluster, you can start with the `nightly release test <https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/sort.py>`_ that Ray runs for :meth:`Datastream.random_shuffle <ray.data.Datastream.random_shuffle>` and :meth:`Datastream.sort <ray.data.Datastream.sort>`.
To get an idea of the performance you can expect, here are some run time results for :meth:`Datastream.random_shuffle <ray.data.Datastream.random_shuffle>` on 1-10TB of data on 20 machines (m5.4xlarge instances on AWS EC2, each with 16 vCPUs, 64GB RAM).

.. image:: https://docs.google.com/spreadsheets/d/e/2PACX-1vQvBWpdxHsW0-loasJsBpdarAixb7rjoo-lTgikghfCeKPQtjQDDo2fY51Yc1B6k_S4bnYEoChmFrH2/pubchart?oid=598567373&format=image
   :align: center

To try out push-based shuffle, set the environment variable ``RAY_DATA_PUSH_BASED_SHUFFLE=1`` when running your application:

.. code-block:: bash

    $ wget https://raw.githubusercontent.com/ray-project/ray/master/release/nightly_tests/datastream/sort.py
    $ RAY_DATA_PUSH_BASED_SHUFFLE=1 python sort.py --num-partitions=10 --partition-size=1e7
    # Datastream size: 10 partitions, 0.01GB partition size, 0.1GB total
    # [datastream]: Run `pip install tqdm` to enable progress reporting.
    # 2022-05-04 17:30:28,806	INFO push_based_shuffle.py:118 -- Using experimental push-based shuffle.
    # Finished in 9.571171760559082
    # ...

You can also specify the shuffle implementation during program execution by
setting the ``DataContext.use_push_based_shuffle`` flag:

.. code-block:: python

    import ray.data

    ctx = ray.data.DataContext.get_current()
    ctx.use_push_based_shuffle = True

    n = 1000
    parallelism=10
    ds = ray.data.range(n, parallelism=parallelism)
    print(ds.random_shuffle().take(10))
    # [954, 405, 434, 501, 956, 762, 488, 920, 657, 834]
