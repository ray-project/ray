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
control with memory consumption and runtime. The list below presents options in increasing order of
resource consumption and runtime. Choose the most appropriate method for your use case.

.. _shuffling_file_order:

Shuffle the ordering of files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To randomly shuffle the ordering of input files before reading, call a :ref:`read function <input-output>` function that supports shuffling, such as
:func:`~ray.data.read_images`, and use the ``shuffle="files"`` parameter. This randomly assigns
input files to workers for reading.

This is the fastest "shuffle" option: it's purely a metadata operation---the system random-shuffles the list of files constituting the dataset before
fetching them with reading tasks. This option, however, doesn't shuffle the rows inside files, so the randomness might not be
sufficient for your needs in case of files with the large number of rows.

.. testcode::

    import ray

    ds = ray.data.read_images(
        "s3://anonymous@ray-example-data/image-datasets/simple",
        shuffle="files",
    )

.. _local_shuffle_buffer:

Local buffer shuffle
~~~~~~~~~~~~~~~~~~~~

To locally shuffle a subset of rows using iteration methods, such as :meth:`~ray.data.Dataset.iter_batches`,
:meth:`~ray.data.Dataset.iter_torch_batches`, and :meth:`~ray.data.Dataset.iter_tf_batches`,
specify `local_shuffle_buffer_size`.

This shuffles up to a `local_shuffle_buffer_size` number of rows buffered during iteration. See more details in
:ref:`Iterating over batches with shuffling <iterating-over-batches-with-shuffling>`.

This is slower than files shuffling, and shuffles rows locally without
network transfer. You can use this local shuffle buffer together with shuffling
ordering of files. See :ref:`Shuffle the ordering of files <shuffling_file_order>`.

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

Randomizing block order
~~~~~~~~~~~~~~~~~~~~~~~

This option randomizes the order of :ref:`blocks <data_key_concepts>` in a dataset. While applying this operation alone doesn't involve heavy computation
and communication, it requires Ray Data to materialize all blocks in memory before actually randomizing their ordering in the queue for subsequent operation.

.. note:: Ray Data doesn't guarantee any particular ordering of the blocks when reading blocks from different files in parallel by default, unless you set `DataContext.execution_options.preserve_order` to true. Henceforth, this particular option
    is primarily relevant in cases when the system yields blocks from relatively small set of very large files.

.. note:: Only use this option when your dataset is small enough to fit into the object store memory.

To perform block order shuffling, use :meth:`randomize_block_order <ray.data.Dataset.randomize_block_order>`.

.. testcode::
    import ray

    ds = ray.data.read_text(
        "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
    )

    # Randomize the block order of this dataset.
    ds = ds.randomize_block_order()

Global shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To shuffle all rows globally, across the whole dataset, multiple options are available

    1. *Random shuffling*: invoking :meth:`~ray.data.Dataset.random_shuffle` essentially permutes and shuffles individual rows
    from existing blocks into the new ones using an optionally provided seed.

    2. (**New in 2.46**) *Key-based repartitioning*: invoking :meth:`~ray.data.Dataset.repartition` with `keys` parameter triggers
    :ref:`hash-shuffle <hash-shuffle>` operation, shuffling the rows based on the hash of the values in the provided key columns, providing
    deterministic way of co-locating rows based on the hash of the column values.

Note that shuffle is an expensive operation requiring materializing of the whole dataset in memory as well as serving as a synchronization
barrier---subsequent operators won't be able to start executing until shuffle completion.

Example of random shuffling with seed:

.. testcode::

    import ray

    ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

    # Random shuffle with seed
    random_shuffled_ds = ds.random_shuffle(seed=123)


Example of hash shuffling based on column `id`:

.. testcode::

    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    # First enable hash-shuffle as shuffling strategy
    DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    # Hash-shuffle
    hash_shuffled_ds = ds.repartition(keys="id", num_blocks=200)

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
gradient-descent-based model trainers benefit from improved global shuffle quality.
In practice, the benefit's particularly pronounced for tabular data/models.
However, the more global the shuffle is, the more expensive the shuffling operation.
The increase compounds with distributed data-parallel training on a multi-node cluster due
to data transfer costs. This cost can be prohibitive when using very large datasets.

The best route for determining the best tradeoff between preprocessing time and cost and
per-epoch shuffle quality is to measure the precision gain per training step for your
particular model under different shuffling policies such as no shuffling, local shuffling, or global shuffling.

As long as your data loading and shuffling throughput is higher than your training throughput, your GPU should
saturate. If you have shuffle-sensitive models, push the
shuffle quality higher until you reach this threshold.

.. _shuffle_performance_tips:

Enabling push-based shuffle
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Dataset operations require a *shuffle* operation, meaning that the system shuffles data from all of the input partitions to all of the output partitions.
These operations include :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`,
:meth:`Dataset.sort <ray.data.Dataset.sort>` and :meth:`Dataset.groupby <ray.data.Dataset.groupby>`.
For example, during a sort operation, the system reorders data between blocks and therefore requires shuffling across partitions.
Shuffling can be challenging to scale to large data sizes and clusters, especially when the total dataset size can't fit into memory.

Ray Data provides an alternative shuffle implementation known as push-based shuffle for improving large-scale performance.
Try this out if your dataset has more than 1000 blocks or is larger than 1 TB in size.

To try this out locally or on a cluster, you can start with the `nightly release test <https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/sort_benchmark.py>`_ that Ray runs for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` and :meth:`Dataset.sort <ray.data.Dataset.sort>`.
To get an idea of the performance you can expect, here are some run time results for :meth:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` on 1-10 TB of data on 20 machines - m5.4xlarge instances on AWS EC2, each with 16 vCPUs, 64 GB RAM.

.. image:: https://docs.google.com/spreadsheets/d/e/2PACX-1vQvBWpdxHsW0-loasJsBpdarAixb7rjoo-lTgikghfCeKPQtjQDDo2fY51Yc1B6k_S4bnYEoChmFrH2/pubchart?oid=598567373&format=image
   :align: center

To try out push-based shuffle, set the environment variable ``RAY_DATA_PUSH_BASED_SHUFFLE=1`` when running your application:

.. code-block:: bash

    $ wget https://raw.githubusercontent.com/ray-project/ray/master/release/nightly_tests/dataset/sort_benchmark.py
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
