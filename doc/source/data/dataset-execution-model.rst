Dataset Execution Model
=======================

This page overviews the execution model of Datasets, which may be useful for understanding and tuning performance.

Reading Data
------------

Datasets uses Ray tasks to read data from remote storage. When reading from a file-based datasource (e.g., S3, GCS), it creates a number of read tasks equal to the specified read parallelism (200 by default). One or more files will be assigned to each read task. Each read task reads its assigned files and produces one or more output blocks (Ray objects):

.. image:: dataset-read.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

In the common case, each read task produces a single output block. Read tasks may split the output into multiple blocks if the data exceeds the target max block size (2GiB by default). This automatic block splitting avoids out-of-memory errors when reading very large single files (e.g., a 100-gigabyte CSV file). All of the built-in datasources except for JSON currently support automatic block splitting.

.. note::

  Block splitting is currently off by default. See the block size tuning section below on how to enable block splitting (beta).

Deferred Read Task Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a Dataset is created using ``ray.data.read_*``, only the first read task will be executed initially. This avoids blocking Dataset creation on the reading of all data files, enabling inspection functions like ``ds.schema()`` and ``ds.show()`` to be used right away. Executing further transformations on the Dataset will trigger execution of all read tasks.


Dataset Transforms
------------------

Datasets use either Ray tasks or Ray actors to transform datasets (i.e., for ``.map``, ``.flat_map``, or ``.map_batches``). By default, tasks are used (``compute="tasks"``). Actors can be specified with ``compute="actors"``, in which case an autoscaling pool of Ray actors will be used to apply transformations. Using actors allows for expensive state initialization (e.g., for GPU-based tasks) to be re-used. Whichever compute strategy is used, each map task generally takes in one block and produces one or more output blocks. The output block splitting rule is the same as for file reads (blocks are split after hitting the target max block size of 2GiB):

.. image:: dataset-map.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1MGlGsPyTOgBXswJyLZemqJO1Mf7d-WiEFptIulvcfWE/edit

Shuffling Data
--------------

Certain operations like ``.sort`` and ``.groupby`` require data blocks to be partitioned by value. Datasets executes this in three phases. First, a wave of sampling tasks determines suitable partition boundaries based on a random sample of data. Second, map tasks divide each input block into a number of output blocks equal to the number of reduce tasks. Third, reduce tasks take assigned output blocks from each map task and combines them into one block. Overall, this strategy generates ``O(n^2)`` intermediate objects where ``n`` is the number of input blocks.

You can also change the partitioning of a Dataset using ``.random_shuffle`` or ``.repartition``. The former should be used if you want to randomize the order of elements in the dataset. The second should be used if you only want to equalize the size of the Dataset blocks (e.g., after a read or transformation that may skew the distribution of block sizes). Note that repartition has two modes, ``shuffle=False``, which performs the minimal data movement needed to equalize block sizes, and ``shuffle=True``, which performs a full (non-random) distributed shuffle:

.. image:: dataset-shuffle.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/132jhE3KXZsf29ho1yUdPrCHB9uheHBWHJhDQMXqIVPA/edit

Memory Management
-----------------

This section deals with how Datasets manages execution and object store memory.

Execution Memory
~~~~~~~~~~~~~~~~

During execution, certain types of intermediate data must fit in memory. This includes the input block of a task, as well as at least one of the output blocks of the task (when a task has multiple output blocks, only one needs to fit in memory at any given time). The input block consumes object stored shared memory (Python heap memory for non-Arrow data). The output blocks consume Python heap memory (prior to putting in the object store) as well as object store memory (after being put in the object store).

This means that large block sizes can lead to potential out-of-memory situations. To avoid OOM errors, Datasets tries to split blocks during map and read tasks into pieces smaller than the target max block size. In some cases, this splitting is not possible (e.g., if a single item in a block is extremely large, or the function given to ``.map_batches`` returns a very large batch). To avoid these issues, make sure no single item in your Datasets is too large, and always call ``.map_batches`` with batch size small enough such that the output batch can comfortably fit into memory.

Object Store Memory
~~~~~~~~~~~~~~~~~~~

Datasets uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

**Object Spilling**: Since Datasets uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:

.. image:: dataset-spill.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1H_vDiaXgyLU16rVHKqM3rEl0hYdttECXfxCj8YPrbks/edit

**Locality Scheduling**: Ray will preferentially schedule compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.

**Reference Counting**: Dataset blocks are kept alive by object store reference counting as long as there is any Dataset that references them. To free memory, delete any Python references to the Dataset object.

**Load Balancing**: Datasets uses Ray scheduling hints to spread read tasks out across the cluster to balance memory usage.


Performance Tips and Tuning
---------------------------

Debugging Statistics
~~~~~~~~~~~~~~~~~~~~

You can view debug stats for your Dataset and DatasetPipeline executions via ``ds.stats()``.

At a high level, execution stats for tasks (e.g., CPU time) are attached to block metadata objects. Datasets have stats objects that hold references to these stats and parent dataset stats (this avoids stats holding references to parent datasets, allowing them to be gc'ed). Similarly, DatasetPipelines hold stats from recently computed datasets.  In addition, we also collect statistics about iterator timings (time spent waiting / processing / in user code). Here's a sample output of getting stats in one of the most advanced use cases: iterating over a split of a dataset pipeline in a remote task:

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

    (consume pid=723600) == Pipeline Window 2 ==
    (consume pid=723600) Stage 0 read: 200/200 blocks executed in 0.13s
    (consume pid=723600) * Wall time: 117.63us min, 6.6ms max, 413.57us mean, 82.71ms total
    (consume pid=723600) * CPU time: 116.3us min, 6.48ms max, 380.74us mean, 76.15ms total
    (consume pid=723600) * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    (consume pid=723600) * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    (consume pid=723600) * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used
    (consume pid=723600) 
    (consume pid=723600) Stage 1 map: 200/200 blocks executed in 0.3s
    (consume pid=723600) * Wall time: 294.12us min, 2.55ms max, 918.35us mean, 183.67ms total
    (consume pid=723600) * CPU time: 292.68us min, 829.32us max, 554.42us mean, 110.88ms total
    (consume pid=723600) * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    (consume pid=723600) * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    (consume pid=723600) * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used
    (consume pid=723600) 
    (consume pid=723600) Stage 2 map: 200/200 blocks executed in 0.41s
    (consume pid=723600) * Wall time: 8.06ms min, 18.29ms max, 9.37ms mean, 1.87s total
    (consume pid=723600) * CPU time: 572.05us min, 3.43ms max, 992.16us mean, 198.43ms total
    (consume pid=723600) * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    (consume pid=723600) * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    (consume pid=723600) * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used
    (consume pid=723600) 
    (consume pid=723600) Stage 3 random_shuffle_TODO: 0/0 blocks executed in -1s
    (consume pid=723600) 
    (consume pid=723600) Dataset iterator time breakdown:
    (consume pid=723600) * In ray.wait(): 1.85ms
    (consume pid=723600) * In format_batch(): 15.28ms
    (consume pid=723600) * In user code: 429.99us
    (consume pid=723600) * Total time: 18.07ms
    (consume pid=723600) 
    (consume pid=723600) == Pipeline Window 3 ==
    (consume pid=723600) Stage 0 read: [execution cached]
    (consume pid=723600) Stage 1 map: [execution cached]
    (consume pid=723600) Stage 2 map: 200/200 blocks executed in 0.46s
    (consume pid=723600) * Wall time: 7.97ms min, 27.64ms max, 9.79ms mean, 1.96s total
    (consume pid=723600) * CPU time: 592.86us min, 1.75ms max, 1.01ms mean, 201.85ms total
    (consume pid=723600) * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    (consume pid=723600) * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    (consume pid=723600) * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used
    (consume pid=723600) 
    (consume pid=723600) Stage 3 random_shuffle_TODO: 0/0 blocks executed in -1s
    (consume pid=723600) 
    (consume pid=723600) Dataset iterator time breakdown:
    (consume pid=723600) * In ray.wait(): 1.2ms
    (consume pid=723600) * In format_batch(): 10.03ms
    (consume pid=723600) * In user code: 292.8us
    (consume pid=723600) * Total time: 11.86ms
    (consume pid=723600) 
    (consume pid=723600) == Pipeline Window 4 ==
    (consume pid=723600) Stage 0 read: [execution cached]
    (consume pid=723600) Stage 1 map: [execution cached]
    (consume pid=723600) Stage 2 map: 200/200 blocks executed in 0.42s
    (consume pid=723600) * Wall time: 8.04ms min, 16.93ms max, 9.48ms mean, 1.9s total
    (consume pid=723600) * CPU time: 662.75us min, 3.33ms max, 972.26us mean, 194.45ms total
    (consume pid=723600) * Output num rows: 50 min, 50 max, 50 mean, 10000 total
    (consume pid=723600) * Output size bytes: 456 min, 456 max, 456 mean, 91200 total
    (consume pid=723600) * Tasks per node: 200 min, 200 max, 200 mean; 1 nodes used
    (consume pid=723600) 
    (consume pid=723600) Stage 3 random_shuffle_TODO: 0/0 blocks executed in -1s
    (consume pid=723600) 
    (consume pid=723600) Dataset iterator time breakdown:
    (consume pid=723600) * In ray.wait(): 1.18ms
    (consume pid=723600) * In format_batch(): 9.98ms
    (consume pid=723600) * In user code: 284.75us
    (consume pid=723600) * Total time: 11.78ms
    (consume pid=723600) 
    (consume pid=723600) ##### Overall Pipeline Time Breakdown #####
    (consume pid=723600) * Time stalled waiting for next dataset: 2.74ms min, 701.09ms max, 491.05ms mean, 1.96s total
    (consume pid=723600) * Time in dataset iterator: 315.69ms
    (consume pid=723600) * Time in user code: 1.21ms
    (consume pid=723600) * Total time: 5.0s

Batching Transforms
~~~~~~~~~~~~~~~~~~~

Mapping individual records using ``.map(fn)`` can be quite slow. Instead, consider using ``.map_batches(batch_fn, batch_format="pandas")`` and writing your ``batch_fn`` to perform vectorized pandas operations.

Parquet Column Pruning
~~~~~~~~~~~~~~~~~~~~~~

Current Datasets will read all Parquet columns into memory. If you only need a subset of the columns, make sure to specify the list of columns explicitly when calling ``ray.data.read_parquet()`` to avoid loading unnecessary data.

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 0.5 CPUs per read task, which means two read tasks can concurrently execute per CPU. For datasources that can benefit from higher degress of I/O parallelism, you can specify a lower ``num_cpus`` value for the read function via the ``ray_remote_args`` parameter. For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

The number of read tasks can also be increased by increasing the ``parallelism`` parameter. For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to create up to 1000 read tasks. Typically, increasing the number of read tasks only helps if you have more cluster CPUs than the default parallelism.

Tuning Max Block Size
~~~~~~~~~~~~~~~~~~~~~

Block splitting is off by default. To enable block splitting (beta), run ``ray.data.context.DatasetContext.get_current().block_splitting_enabled = True``.

Once enabled, the max target block size can be adjusted via the Dataset context API. For example, to configure a max target block size of 8GiB, run ``ray.data.context.DatasetContext.get_current().target_max_block_size = 8192 * 1024 * 1024`` prior to creating the Dataset. Lower block sizes reduce the max amount of object store and Python heap memory required during execution. However, having too many blocks may introduce task scheduling overheads.

We do not recommend adjusting this value for most workloads. However, if shuffling a large amount of data, increasing the block size limit reduces the number of intermediate blocks (as a rule of thumb, shuffle creates ``O(num_blocks**2)`` intermediate blocks). Alternatively, you can ``.repartition()`` the dataset to reduce the number of blocks prior to shuffle/groupby operations. If you're seeing out of memory errors during map tasks, reducing the max block size may also be worth trying.

Note that the number of blocks a Dataset created from ``ray.data.read_*`` contains is not fully known until all read tasks are fully executed. The number of blocks printed in the Dataset's string representation is initially set to the number of read tasks generated. To view the actual number of blocks created after block splitting, use ``len(ds.get_internal_block_refs())``, which will block until all data has been read.
