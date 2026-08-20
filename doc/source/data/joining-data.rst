.. meta::
   :description: Join Ray Data Datasets on key columns using the supported join types, and tune the partition and aggregator counts.

.. _joining-data:

============
Joining data
============

.. note:: Joins are experimental, and some behavior might not work as expected. Joins are available in Ray 2.46 and later.

Ray Data can join multiple :class:`~ray.data.dataset.Dataset` instances on the provided key columns, using any of the supported join types:

.. testcode::

    import ray

    doubles_ds = ray.data.range(4).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    squares_ds = ray.data.range(4).map(
        lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
    )

    doubles_and_squares_ds = doubles_ds.join(
        squares_ds,
        join_type="inner",
        num_partitions=2,
        on=("id",),
    )

Ray Data supports the following join types. See :meth:`Dataset.join <ray.data.Dataset.join>` for the current list.

**Inner and outer joins:**

- Inner, Left Outer, Right Outer, Full Outer

**Semi joins:**

- Left Semi, Right Semi return all rows that have at least one matching row in the other table,
  returning only columns from the requested side.

**Anti joins:**

- Left Anti, Right Anti return rows that have no matching rows in the other table, returning only
  columns from the requested side.

Internally, joins use the :ref:`hash-shuffle backend <hash-shuffle>`.
:ref:`Shuffle v2 <shuffle-v2>` (``ShuffleStrategy.SHUFFLE_V2``), which is in alpha, provides an
updated hash-shuffle implementation for joins. To use it, set the shuffle strategy before creating a
``Dataset``:
``ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.SHUFFLE_V2``. See
:ref:`Tuning shuffle v2 <tuning-shuffle-v2>` for the memory-related settings.

Configuring joins
-----------------

Joins are generally memory-intensive operations that require accurate memory accounting and projection, so they're sensitive to skews and imbalances in the dataset.

Ray Data provides the following levers to allow tuning the performance of joins for your workload:

-   ``num_partitions``: (required) specifies number of partitions both incoming datasets will be hash-partitioned into. Check out :ref:`configuring number of partitions <joins_configuring_num_partitions>` section for guidance on how to tune this up.
-   ``partition_size_hint``: (**deprecated**) Hint to joining operator about the estimated avg expected size of the individual partition (in bytes). Ray Data ignores this parameter and a future release removes it. Passing a value emits a ``DeprecationWarning``. The join path sizes reduce-task memory from observed partition sizes instead of from a hint.

.. _joins_configuring_num_partitions:

Configuring the number of partitions
------------------------------------

The number of partitions, also referred to as blocks, sets an important trade-off. It weighs the size of the batch of rows that each task handles against the memory the operation on those rows requires.

**Rule of thumb**: *keep partitions large, but not so large that they cause out-of-memory (OOM) errors.*

1.  Don't oversize partitions for joins, because joined partitions that are too large to fit in memory cause OOM errors.
2.  Don't create too many small partitions either, because passing a large number of smaller objects adds overhead.

Configuring the number of aggregators
-------------------------------------

*Aggregators* are worker actors that perform the joins, aggregations, and shuffling. They receive individual partition chunks from the incoming blocks and then aggregate them in the way the given operation requires.

Consider the following when you configure the number of aggregators in your pool:

- Defaults to the smallest of ``num_partitions``, the number of CPUs in the cluster, and ``DataContext.max_hash_shuffle_aggregators``, which is 128 by default.
- An individual aggregator might handle more than one partition. Ray Data splits partitions evenly among the aggregators, in round-robin fashion.
- Aggregators are stateful components that hold the partitions in memory during shuffling.

.. note:: As a rule of thumb, avoid setting ``num_partitions`` far higher than the number of aggregators, because doing so might create bottlenecks.

1.  Setting ``DataContext.max_hash_shuffle_aggregators`` caps the number of aggregators.
2.  Setting it to ``max_hash_shuffle_aggregators >= num_partitions`` allocates one partition per aggregator.
