---
myst:
  html_meta:
    description: "Join Ray Data Datasets on key columns using the supported join types, and tune the partition and aggregator counts."
---

(joining-data)=

# Joining data

:::{note}
Joins are experimental, and some behavior might not work as expected. Joins are available in Ray 2.46 and later.
:::

Ray Data joins multiple {class}`~ray.data.dataset.Dataset` instances on the key columns you provide, using any of the supported join types:

```{testcode}
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
```

Ray Data supports the following join types:

- **Inner and outer joins**: Inner, left outer, right outer, and full outer.
- **Semi joins**: Left semi and right semi joins return all rows that have at least one matching row in the other table. They return only the columns from the requested side.
- **Anti joins**: Left anti and right anti joins return rows that have no matching rows in the other table. They return only the columns from the requested side.

See {meth}`Dataset.join <ray.data.Dataset.join>` for the current list.

Internally, joins use a hash-shuffle backend that joins each hash partition with Polars. By default, joins use {ref}`shuffle v2 <shuffle-v2>`, which is the `ShuffleStrategy.SHUFFLE_V2` strategy. See {ref}`Tune shuffle v2 <tuning-shuffle-v2>` for the memory-related settings. To fall back to the previous {ref}`hash-shuffle implementation <hash-shuffle>`, set `ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE` before you create a `Dataset`.

(configuring-joins)=

## Configure joins

Joins are generally memory-intensive and require accurate memory accounting and projection, so they're sensitive to skew and imbalance in the dataset.

Tune join performance for your workload with the following two parameters:

- `num_partitions`: Required. The number of hash partitions to split both incoming datasets into. See {ref}`Configure the number of partitions <joins_configuring_num_partitions>` for tuning guidance.
- `partition_size_hint`: Deprecated. A hint to the join operator about the estimated average size of an individual partition, in bytes. Ray Data ignores this parameter, and a future release removes it. Passing a value emits a `DeprecationWarning`. Instead of a hint, the join path sizes reduce-task memory from observed partition sizes.

(joins_configuring_num_partitions)=
(configuring-the-number-of-partitions)=

## Configure the number of partitions

The number of partitions, also called blocks, sets a trade-off between the size of the batch of rows that each task handles and the memory that the operation on those rows requires.

As a rule of thumb, keep partitions large, but not so large that they cause out-of-memory (OOM) errors. Joined partitions that are too large to fit in memory cause OOM errors. Don't create too many small partitions either, because passing a large number of smaller objects adds overhead.

(configuring-the-number-of-aggregators)=

## Configure the number of aggregators

*Aggregators* are worker actors that perform the joins, aggregations, and shuffling. They receive individual partition chunks from the incoming blocks and aggregate them as the operation requires.

Consider the following when you configure the number of aggregators in your pool:

- The number of aggregators defaults to the smallest of `num_partitions`, the number of CPUs in the cluster, and `DataContext.max_hash_shuffle_aggregators`, which is 128 by default.
- An individual aggregator might handle more than one partition. Ray Data splits partitions evenly among the aggregators in round-robin fashion.
- Aggregators are stateful components that hold the partitions in memory during shuffling.

:::{note}
As a rule of thumb, avoid setting `num_partitions` far higher than the number of aggregators, because doing so might create bottlenecks.
:::

To cap the number of aggregators, set `DataContext.max_hash_shuffle_aggregators`. Setting `max_hash_shuffle_aggregators >= num_partitions` allocates one partition per aggregator.
