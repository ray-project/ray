.. _data_performance_tips:

Performance Tips and Tuning
===========================

Debugging Statistics
~~~~~~~~~~~~~~~~~~~~

You can view debug stats for your Dataset and DatasetPipeline executions via ``ds.stats()``.
These stats can be used to understand the performance of your Datasets workload and can help you debug problematic bottlenecks.

At a high level, execution stats for tasks (e.g., CPU time) are attached to block metadata objects.
Datasets have stats objects that hold references to these stats and parent dataset stats (this avoids stats holding references to parent datasets, allowing them to be garbage collected).
Similarly, DatasetPipelines hold stats from recently computed datasets.
In addition, we also collect statistics about iterator timings (time spent waiting / processing / in user code).
Here's a sample output of getting stats in one of the most advanced use cases,
namely iterating over a split of a dataset pipeline in a remote task:

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

Mapping individual records using ``.map(fn)`` can be quite slow.
Instead, consider using ``.map_batches(batch_fn, batch_format="pandas")`` and writing your ``batch_fn`` to
perform vectorized pandas operations.

Parquet Column Pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Datasets will read all Parquet columns into memory.
If you only need a subset of the columns, make sure to specify the list of columns explicitly when
calling ``ray.data.read_parquet()`` to avoid loading unnecessary data.

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 0.5 CPUs per read task, which means two read tasks can concurrently execute per CPU.
For data sources that can benefit from higher degress of I/O parallelism, you can specify a lower ``num_cpus`` value for the read function via the ``ray_remote_args`` parameter.
For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

The number of read tasks can also be increased by increasing the ``parallelism`` parameter.
For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to create up to 1000 read tasks.
Typically, increasing the number of read tasks only helps if you have more cluster CPUs than the default parallelism.

Tuning Max Block Size
~~~~~~~~~~~~~~~~~~~~~

Block splitting is off by default. To enable block splitting (beta), run ``ray.data.context.DatasetContext.get_current().block_splitting_enabled = True``.

Once enabled, the max target block size can be adjusted via the Dataset context API.
For example, to configure a max target block size of 8GiB, run ``ray.data.context.DatasetContext.get_current().target_max_block_size = 8192 * 1024 * 1024`` prior to creating the Dataset.
Lower block sizes reduce the max amount of object store and Python heap memory required during execution.
However, having too many blocks may introduce task scheduling overheads.

We do not recommend adjusting this value for most workloads.
However, if shuffling a large amount of data, increasing the block size limit reduces the number of intermediate blocks (as a rule of thumb, shuffle creates ``O(num_blocks**2)`` intermediate blocks).
Alternatively, you can ``.repartition()`` the dataset to reduce the number of blocks prior to shuffle/groupby operations.
If you're seeing out of memory errors during map tasks, reducing the max block size may also be worth trying.

Note that the number of blocks a Dataset created from ``ray.data.read_*`` contains is not fully known until all read tasks are fully executed.
The number of blocks printed in the Dataset's string representation is initially set to the number of read tasks generated.
To view the actual number of blocks created after block splitting, use ``len(ds.get_internal_block_refs())``, which will block until all data has been read.
