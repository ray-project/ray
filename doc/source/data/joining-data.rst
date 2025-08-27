.. _joining-data:

================
Joining datasets
================

.. note:: This is a new feature released in Ray 2.46. Note, this is an experimental feature and some things might not work as expected.

Ray Data allows multiple :class:`~ray.data.dataset.Dataset` instances to be joined using different join types (inner, outer, semi, anti) based on the provided key columns like following:

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

Ray Data supports the following join types (check out `Dataset.join` docs for up-to-date list):

**Inner/Outer Joins:**
- Inner, Left Outer, Right Outer, Full Outer

**Semi Joins:**
- Left Semi, Right Semi (returns all rows that have at least one matching row in the other table,
only returning columns from the requested side)

**Anti Joins:**
- Left Anti, Right Anti (return rows that have no matching rows in the other table, only returning
columns from the requested side)

Internally joins are currently powered by the :ref:`hash-shuffle backend <hash-shuffle>`.

Configuring Joins
----------------------------------

Joins are generally memory-intensive operations that require accurate memory accounting and projection and hence are sensitive to skews and imbalances in the dataset.

Ray Data provides the following levers to allow tuning the performance of joins for your workload:

-   `num_partitions`: (required) specifies number of partitions both incoming datasets will be hash-partitioned into. Check out :ref:`configuring number of partitions <joins_configuring_num_partitions>` section for guidance on how to tune this up.
-   `partition_size_hint`: (optional) Hint to joining operator about the estimated avg expected size of the individual partition (in bytes). If not specified, defaults to DataContext.target_max_block_size (128Mb by default).
    -   Note that, `num_partitions * partition_size_hint` should ideally be approximating actual dataset size, ie `partition_size_hint` could be estimated as dataset size divided by `num_partitions` (assuming relatively evenly sized partitions)
    -   However, in cases when dataset partitioning is expected to be heavily skewed `partition_size_hint` should approximate largest partition to prevent Out-of-Memory (OOM) errors

.. note:: Be mindful that by default Ray reserves only 30% of the memory for its Object Store. This is recommended to be set at least to ***50%*** for all
    Ray Data workloads, but especially so for ones utilizing joins.

To configure Object Store to be 50% add to your image:

.. testcode::

    RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.5

.. _joins_configuring_num_partitions:

Configuring number of partitions
--------------------------------------------

Number of partitions (also referred to as blocks) provide an important trade-off between the size of individual batch of rows handled by individual tasks against memory requirements of the operation performed on them

**Rule of thumb**: *keep partitions large, but not too large to cause Out-of-Memory (OOM) errors*

1.  It’s important to not “oversize” partitions for joins as that could lead to OOM errors (if joined partitions might be too large to fit in memory)
2.  It’s also important to not create too many small partitions as this creates an overhead of passing large amount of smaller objects

Configuring number of Aggregators
----------------------------------------------

“Aggregators” are worker actors that perform actual joins/aggregations/shuffling, they receive individual partition chunks from the incoming blocks and subsequently "aggregate" them in the way that's required to perform given operation.

Following are important considerations for successfully configuring number of aggregators in your pool:

    - Defaults to 64 or `num_partitions` (in cases when there are less than 64 partitions)
    - Individual Aggregators might be assigned to handle more than one partition (partitions are evenly split in round-robin fashion among the aggregators)
    - Aggregators are stateful components that hold the state (partitions) during shuffling **in memory**

.. note:: *Rule of thumb* is to *avoid setting `num_partitions` >> number of aggregators as it might create bottlenecks*

1.  Setting `DataContext.max_hash_shuffle_aggregators` caps the number of aggregators
2.  Setting it to large enough value has an effect of allocating 1 partition to 1 aggregator (when `max_hash_shuffle_aggregators >= num_partitions`)
