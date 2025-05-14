.. _joining-data:

================
Joining datasets
================

.. note:: This is a new feature released in Ray 2.46. Note, this is an experimental feature and some things might not work as expected.

Hash-based shuffle is a way of distributing data in the cluster for subsequent processing, by hashing a tuple of *key columns* values for every row to determine to which *partition* every row should belong.

After rows of every block are hash-partititioned, they are distributed to nodes that are handling corresponding partitions.

Hash-shuffle is a new implementation of the distributed shuffling algorithm offering better performance while lowering memory requirements for operations like aggregations, allowing these to be efficiently performed on datasets exceeding available cluster memory.

Prerequisites
-------------

Enable hash-shuffling as default shuffling strategy used by aggregations, repartitioning and joins.

.. testcode::

    import ray
    from ray.data.context import ShuffleStrategy

    # Enable hash-shuffling by default for aggregations and repartitions
    ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

.. note:: Be mindful that by default Ray reserves only 30% of the memory for its Object Store. This is recommended to be set at least to ***50%*** for all
    Ray Data workloads, but especially so for ones utilizing joins.

To configure Object Store to be 50% add to your image:

.. testcode::

    RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.5

Joins
-----

Joins are generally memory-intensive operations that require accurate memory accounting and projection and hence are sensitive to skews and imbalances in the dataset.

Ray Data provides following levers to allow to tune up performance of joins for your workload:

-   num_partitions: (required) specifies number of partitions both incoming datasets will be hash-partitioned into. Check out `Effective Configuration`_ section for guidance on how to tune this up.
-   partition_size_hint: (optional) Hint to joining operator about the estimated avg expected size of the individual partition (in bytes). If not specified, defaults to DataContext.target_max_block_size (128Mb by default).

    -   Note that, num_partitions \* partition_size_hint should ideally be approximating actual dataset size, ie partition_size_hint could be estimated as dataset size divided by num_partitions (assuming relatively evenly sized partitions)
    -   However, in cases when dataset partitioning is expected to be heavily skewed partition_size_hint should approximate largest partition to prevent OOMs

Effective ConfigurationNumber of Partitions
--------------------------------------------

Number of partitions (also referred to as blocks) provide an important trade-off between the size of individual batch of rows handled by individual tasks against memory requirements of the operation performed on them

**Rule of thumb**: *keep partitions large, but not too large to OOM*

1.  It’s important to not “oversize” partitions for joins as that could lead to OOMs (if joined partitions might be be too large to fit in memory)
2.  It’s also important to not create too many small partitions as this creates an overhead of passing large amount of smaller objects

Number of Aggregators
----------------------

“Aggregators” are worker actors that perform actual joins/aggregations/shuffling, they receive individual partition chunks from the incoming blocks and subsequently ‘aggregate’ them in the way that is required to perform the required operation.

Following are important considerations for successfully configuring number of aggregators in your pool:

-   Defaults to 64 or num_partitions (if there are less than 64 partitions)
-   Individual aggregator might be handling more than one partition (partitions are evenly split in round-robin fashion among the aggregators)
-   Aggregators are stateful components that hold the state (partitions) during shuffling in memory

**Rule of thumb**: *avoid setting num_partitions >> number of aggregators as it might create bottlenecks*

1.  Setting DataContext.max_hash_shuffle_aggregators will cap the number of aggregators
2.  Setting it to large enough value will have an effect of allocating 1 partition to 1 aggregator (when max_hash_shuffle_aggregators >= num_partitions)

Appendix I: Joins API
---------------------

Below you can find API for Dataset.join operation along with its py-doc

::

    def join(

        self: DatasetProtocol,

        ds: "Dataset",

        join_type: str,

        num_partitions: int,

        on: Tuple[str] = ("id",),

        right_on: Optional[Tuple[str]] = None,

        left_suffix: Optional[str] = None,

        right_suffix: Optional[str] = None,

        *,

        partition_size_hint: Optional[int] = None,

        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,

        validate_schemas: bool = False,

    ) -> "Dataset":

        """Join :class:`Datasets <ray.data.Dataset>` on join keys.

        Args:

            ds: Other dataset to join against

            join_type: The kind of join that should be performed, one of ("inner",

                "left_outer", "right_outer", "full_outer")

            num_partitions: Total number of "partitions" input sequences will be split

                into with each partition being joined independently. Increasing number

                of partitions allows to reduce individual partition size, hence reducing

                memory requirements when individual partitions are being joined. Note

                that, consequently, this will also be a total number of blocks that will

                be produced as a result of executing join.

            on: The columns from the left operand that will be used as

                keys for the join operation.

            right_on: The columns from the right operand that will be

                used as keys for the join operation. When none, `on` will

                be assumed to be a list of columns to be used for the right dataset

                as well.

            left_suffix: (Optional) Suffix to be appended for columns of the left

                operand.

            right_suffix: (Optional) Suffix to be appended for columns of the right

                operand.

            partition_size_hint: (Optional) Hint to joining operator about the estimated

                avg expected size of the individual partition (in bytes).

                This is used in estimating the total dataset size and allow to tune

                memory requirement of the individual joining workers to prevent OOMs

                when joining very large datasets.

            aggregator_ray_remote_args: (Optional) Parameter overriding `ray.remote`

                args passed when constructing joining (aggregator) workers.

            validate_schemas:
```