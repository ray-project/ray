.. _shuffling_data:

==============
Shuffling Data
==============

When consuming or iterating over Ray :class:`Datasets <ray.data.dataset.Dataset>`, it can be useful to
shuffle or randomize the order of data (for example, randomizing data ingest order during ML training).
This guide shows several different methods of shuffling data with Ray Data and their respective trade-offs.

Types of shuffling
==================

Ray Data provides several different options for shuffling data, trading off the granularity of shuffle
control with memory consumption and runtime. The options below are listed in increasing order of
resource consumption and runtime; choose the most appropriate method for your use case.

.. _shuffling_file_order:

Shuffle the ordering of files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To randomly shuffle the ordering of input files before reading, call a :ref:`read function <input-output>` function that supports shuffling, such as
:func:`~ray.data.read_images`, and use the ``shuffle="files"`` parameter. This randomly assigns
input files to workers for reading.

This is the fastest option for shuffle, and is a purely metadata operation. This
option doesn't shuffle the actual rows inside files, so the randomness might be
poor if each file has many rows.

.. testcode::

    import ray

    ds = ray.data.read_images(
        "s3://anonymous@ray-example-data/image-datasets/simple",
        shuffle="files",
    )

.. _local_shuffle_buffer:

Local shuffle when iterating over batches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To locally shuffle a subset of rows using iteration methods, such as :meth:`~ray.data.Dataset.iter_batches`,
:meth:`~ray.data.Dataset.iter_torch_batches`, and :meth:`~ray.data.Dataset.iter_tf_batches`,
specify `local_shuffle_buffer_size`. This shuffles the rows up to a provided buffer
size during iteration. See more details in
:ref:`Iterating over batches with shuffling <iterating-over-batches-with-shuffling>`.

This is slower than shuffling ordering of files, and shuffles rows locally without
network transfer. This local shuffle buffer can be used together with shuffling
ordering of files; see :ref:`Shuffle the ordering of files <shuffling_file_order>`.

.. testcode::

    import ray

    ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

    for batch in ds.iter_batches(
        batch_size=2,
        batch_format="numpy",
        local_shuffle_buffer_size=250,
    ):
        print(batch)

.. tip::

    If you observe reduced throughput when using ``local_shuffle_buffer_size``,
    check the total time spent in batch creation by
    examining the ``ds.stats()`` output (``In batch formatting``, under
    ``Batch iteration time breakdown``). If this time is significantly larger than the
    time spent in other steps, decrease ``local_shuffle_buffer_size`` or turn off the local
    shuffle buffer altogether and only :ref:`shuffle the ordering of files <shuffling_file_order>`.

Shuffling block order
~~~~~~~~~~~~~~~~~~~~~

This option randomizes the order of blocks in a dataset. Blocks are the basic unit of data chunk that Ray Data stores in the object store. Applying this operation alone doesn't involve heavy computation and communication. However, it requires Ray Data to materialize all blocks in memory before applying the operation. Only use this option when your dataset is small enough to fit into the object store memory.

To perform block order shuffling, use :meth:`randomize_block_order <ray.data.Dataset.randomize_block_order>`.

.. testcode::
    import ray

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    # Randomize the block order of this dataset.
    ds = ds.randomize_block_order()

Shuffle all rows
~~~~~~~~~~~~~~~~

To randomly shuffle all rows globally, call :meth:`~ray.data.Dataset.random_shuffle`.
This is the slowest option for shuffle, and requires transferring data across
network between workers. This option achieves the best randomness among all options.

.. testcode::

    import ray

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
        .random_shuffle()
    )

.. _optimizing_shuffles:

Advanced: Optimizing shuffles
=============================
.. note:: This is an active area of development. If your Dataset uses a shuffle operation and you are having trouble configuring shuffle,
    `file a Ray Data issue on GitHub <https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdata&projects=&template=bug-report.yml&title=[data]+>`_.

When should you use global per-epoch shuffling?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use global per-epoch shuffling only if your model is sensitive to the
randomness of the training data. Based on a
`theoretical foundation <https://arxiv.org/abs/1709.10432>`__, all
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
For example, during a sort operation, data is reordered between blocks and therefore requires shuffling across partitions.
Shuffling can be challenging to scale to large data sizes and clusters, especially when the total dataset size can't fit into memory.

Ray Data provides an alternative shuffle implementation known as push-based shuffle for improving large-scale performance.
Try this out if your dataset has more than 1000 blocks or is larger than 1 TB in size.

To try this out locally or on a cluster, you can start with the `nightly release test <https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/sort_benchmark.py>`_ that Ray runs for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` and :meth:`Dataset.sort <ray.data.Dataset.sort>`.
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
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray

    ctx = ray.data.DataContext.get_current()
    ctx.use_push_based_shuffle = True

    ds = (
        ray.data.range(1000)
        .random_shuffle()
    )

Large-scale shuffles can take a while to finish.
For debugging purposes, shuffle operations support executing only part of the shuffle, so that you can collect an execution profile more quickly.
Here is an example that shows how to limit a random shuffle operation to two output blocks:

.. testcode::
    :hide:

    import ray
    ray.shutdown()

.. testcode::

    import ray

    ctx = ray.data.DataContext.get_current()
    ctx.set_config(
        "debug_limit_shuffle_execution_to_num_blocks", 2
    )

    ds = (
        ray.data.range(1000, override_num_blocks=10)
        .random_shuffle()
        .materialize()
    )
    print(ds.stats())

.. testoutput::
    :options: +MOCK

    Operator 1 ReadRange->RandomShuffle: executed in 0.08s

        Suboperator 0 ReadRange->RandomShuffleMap: 2/2 blocks executed
        ...
