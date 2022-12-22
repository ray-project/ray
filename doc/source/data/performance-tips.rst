.. _data_performance_tips:

Performance Tips and Tuning
===========================

Debugging Statistics
~~~~~~~~~~~~~~~~~~~~

You can view debug stats for your Dataset and DatasetPipeline executions via :meth:`ds.stats() <ray.data.Dataset.stats>`.
These stats can be used to understand the performance of your Dataset workload and can help you debug problematic bottlenecks. Note that both execution and iterator statistics are available:

.. code-block:: python

    import ray
    import time

    def pause(x):
        time.sleep(.0001)
        return x

    ds = ray.data.range(10000)
    ds = ds.map(lambda x: str(x + 1))

    pipe = ds.repeat(5).map(pause).random_shuffle_each_window()

    @ray.remote
    def consume(p, stats=False):
        for x in p.iter_batches():
            pass
        if stats:
            print(p.stats())

    a, b = pipe.split(2)
    ray.get([consume.remote(a), consume.remote(b, True)])

.. code-block::

    == Pipeline Window 4 ==
    Stage 0 read: [execution cached]
    Stage 1 map: [execution cached]
    Stage 2 map: 200/200 blocks executed in 0.37s
    * Remote wall time: 8.08ms min, 15.82ms max, 9.36ms mean, 1.87s total
    * Remote cpu time: 688.79us min, 3.63ms max, 977.38us mean, 195.48ms total
    * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used

    Stage 3 random_shuffle_map: 200/200 blocks executed in 0.63s
    * Remote wall time: 550.98us min, 5.2ms max, 900.66us mean, 180.13ms total
    * Remote cpu time: 550.79us min, 1.13ms max, 870.82us mean, 174.16ms total
    * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used

    Stage 3 random_shuffle_reduce: 200/200 blocks executed in 0.63s
    * Remote wall time: 152.37us min, 322.96us max, 218.32us mean, 43.66ms total
    * Remote cpu time: 151.9us min, 321.53us max, 217.96us mean, 43.59ms total
    * Output num rows: 32 min, 69 max, 50 mean, 10000 total
    * Output size bytes: 312 min, 608 max, 456 mean, 91200 total
    * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used

    Dataset iterator time breakdown:
    * In ray.wait(): 1.15ms
    * In ray.get(): 3.51ms
    * In format_batch(): 6.83ms
    * In user code: 441.53us
    * Total time: 12.92ms

    ##### Overall Pipeline Time Breakdown #####
    * Time stalled waiting for next dataset: 3.48ms min, 758.48ms max, 486.78ms mean, 1.95s total
    * Time in dataset iterator: 270.66ms
    * Time in user code: 1.38ms
    * Total time: 4.47s

Batching Transforms
~~~~~~~~~~~~~~~~~~~

Mapping individual records using :meth:`.map(fn) <ray.data.Dataset.map>` can be quite slow.
Instead, consider using :meth:`.map_batches(batch_fn, batch_format="pandas") <ray.data.Dataset.map_batches>` and writing your ``batch_fn`` to
perform vectorized pandas operations.

Parquet Column Pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Datasets will read all Parquet columns into memory.
If you only need a subset of the columns, make sure to specify the list of columns
explicitly when calling :meth:`ray.data.read_parquet() <ray.data.read_parquet>` to
avoid loading unnecessary data (projection pushdown).
For example, use ``ray.data.read_parquet("example://iris.parquet", columns=["sepal.length", "variety"]`` to read
just two of the five columns of Iris dataset.

Parquet Row Pruning
~~~~~~~~~~~~~~~~~~~

Similarly, you can pass in a filter to :meth:`ray.data.read_parquet() <ray.data.Dataset.read_parquet>` (filter pushdown)
which will be applied at the file scan so only rows that match the filter predicate
will be returned.
For example, use ``ray.data.read_parquet("example://iris.parquet", filter=pa.dataset.field("sepal.length") > 5.0``
to read rows with sepal.length greater than 5.0.
This can be used in conjunction with column pruning when appropriate to get the benefits of both.

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 1 CPU per read task, which means one read tasks per CPU can execute concurrently.
For data sources that can benefit from higher degress of I/O parallelism, you can specify a lower ``num_cpus`` value for the read function via the ``ray_remote_args`` parameter.
For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

By default, Datasets automatically selects the read parallelism based on the current cluster size and dataset size.
However, the number of read tasks can also be increased manually via the ``parallelism`` parameter.
For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to force up to 1000 read tasks to be created.

.. _shuffle_performance_tips:

Enabling Push-Based Shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Dataset operations require a *shuffle* operation, meaning that data is shuffled from all of the input partitions to all of the output partitions.
These operations include :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`,
:meth:`Dataset.sort <ray.data.Dataset.sort>` and :meth:`Dataset.groupby <ray.data.Dataset.groupby>`.
Shuffle can be challenging to scale to large data sizes and clusters, especially when the total dataset size cannot fit into memory.

Datasets provides an alternative shuffle implementation known as push-based shuffle for improving large-scale performance.
We recommend trying this out if your dataset has more than 1000 blocks or is larger than 1 TB in size.

To try this out locally or on a cluster, you can start with the `nightly release test <https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/sort.py>`_ that Ray runs for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` and :meth:`Dataset.sort <ray.data.Dataset.sort>`.
To get an idea of the performance you can expect, here are some run time results for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` on 1-10TB of data on 20 machines (m5.4xlarge instances on AWS EC2, each with 16 vCPUs, 64GB RAM).

.. image:: https://docs.google.com/spreadsheets/d/e/2PACX-1vQvBWpdxHsW0-loasJsBpdarAixb7rjoo-lTgikghfCeKPQtjQDDo2fY51Yc1B6k_S4bnYEoChmFrH2/pubchart?oid=598567373&format=image
   :align: center

To try out push-based shuffle, set the environment variable ``RAY_DATASET_PUSH_BASED_SHUFFLE=1`` when running your application:

.. code-block:: bash

    $ wget https://raw.githubusercontent.com/ray-project/ray/master/release/nightly_tests/dataset/sort.py
    $ RAY_DATASET_PUSH_BASED_SHUFFLE=1 python sort.py --num-partitions=10 --partition-size=1e7
    # Dataset size: 10 partitions, 0.01GB partition size, 0.1GB total
    # [dataset]: Run `pip install tqdm` to enable progress reporting.
    # 2022-05-04 17:30:28,806	INFO push_based_shuffle.py:118 -- Using experimental push-based shuffle.
    # Finished in 9.571171760559082
    # ...

You can also specify the shuffle implementation during program execution by
setting the ``DatasetContext.use_push_based_shuffle`` flag:

.. code-block:: python

    import ray.data

    ctx = ray.data.context.DatasetContext.get_current()
    ctx.use_push_based_shuffle = True

    n = 1000
    parallelism=10
    ds = ray.data.range(n, parallelism=parallelism)
    print(ds.random_shuffle().take(10))
    # [954, 405, 434, 501, 956, 762, 488, 920, 657, 834]
