---
myst:
  html_meta:
    description: "Shuffle Ray Data at different granularities: file order, local buffer shuffle, parallel batch shuffle, block order, and global per-epoch shuffle."
---

(shuffling_data)=

# Shuffling data

When you consume or iterate over a Ray Data {class}`Dataset <ray.data.dataset.Dataset>`, shuffling the order of the data can be useful, for example to randomize the ingest order during ML training. This guide describes several methods for shuffling data with Ray Data and the trade-offs of each.

(types-of-shuffling)=

## Choose a shuffling method

Ray Data provides several options for shuffling data. Each option trades off the granularity of shuffle control against memory consumption and runtime. The following sections describe each option and its trade-offs. Choose the method that fits your use case.

(shuffling_file_order)=

### Shuffle the ordering of files

To randomly shuffle the ordering of input files before reading, call a {ref}`read function <loading-data-api>` that supports shuffling, such as {func}`~ray.data.read_images`, and pass the `shuffle="files"` parameter. This randomly assigns input files to workers for reading.

This is the fastest shuffle option because it's purely a metadata operation. Ray Data randomly shuffles the list of files that make up the dataset before read tasks fetch them. However, this option doesn't shuffle the rows inside each file, so the randomness might not be sufficient for your needs if your files have a large number of rows.

```{testcode}
import ray

ds = ray.data.read_images(
    "s3://anonymous@ray-example-data/image-datasets/simple",
    shuffle="files",
)
```

(local_shuffle_buffer)=
(local-buffer-shuffle)=

### Shuffle rows with a local buffer

To shuffle a subset of rows locally while you iterate with methods such as {meth}`~ray.data.Dataset.iter_batches`, {meth}`~ray.data.Dataset.iter_torch_batches`, and {meth}`~ray.data.Dataset.iter_tf_batches`, specify `local_shuffle_buffer_size`.

This option shuffles up to `local_shuffle_buffer_size` rows buffered during iteration. For more details, see {ref}`Iterate over batches with shuffling <iterating-over-batches-with-shuffling>`.

This option is slower than file order shuffling, and it shuffles rows locally without network transfer. You can combine the local shuffle buffer with file order shuffling. See {ref}`Shuffle the ordering of files <shuffling_file_order>`.

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

for batch in ds.iter_batches(
    batch_size=2,
    batch_format="numpy",
    local_shuffle_buffer_size=250,
):
    print(batch)
```

:::{tip}
If throughput drops when you use `local_shuffle_buffer_size`, check the total time spent in batch creation. In the `ds.stats()` output, find `In batch formatting` under `Batch iteration time breakdown`. If this time is much larger than the time spent in other steps, decrease `local_shuffle_buffer_size`, or turn off the local shuffle buffer and only {ref}`shuffle the ordering of files <shuffling_file_order>`. If local shuffling is a bottleneck, try {ref}`parallel batch shuffling <shuffle_batches_shuffle>` to move shuffle work out of the iterator.
:::

(map_batches_shuffle)=
(map_batches_vs_local_shuffle)=
(comparing-local-buffer-shuffle-and-map_batches-shuffle)=
(shuffle_batches_shuffle)=

### Shuffle rows in parallel batches

Use {meth}`~ray.data.Dataset.shuffle_batches` to randomly permute rows within batches using parallel Ray Data tasks. This moves shuffle work out of the consumer's iterator.

The `batch_size` parameter controls which rows can mix:

- With `batch_size=None`, the default, shuffle each input block independently. The input block sizes determine the shuffle scope.
- With `batch_size=W`, shuffle target-sized batches that can span input blocks. Each task's final batch can be smaller than `W`, including in the middle of the dataset.

For example, four consecutive input blocks of five rows each with `batch_size=8` can produce shuffle batches of `[8, 2, 8, 2]` rows. This isn't a guarantee of consecutive eight-row windows. The batch size you choose when iterating controls consumer batches separately.

```{testcode}
import ray

ds = ray.data.range(1000, override_num_blocks=10)
# Preserve upstream order for reproducible windows and output.
ds.context.execution_options.preserve_order = True

# Shuffle each input block independently.
block_shuffled = ds.shuffle_batches(seed=42)
block_shuffled.take(10)

# Shuffle target-sized batches, allowing smaller task-tail batches.
batch_shuffled = ds.shuffle_batches(batch_size=256, seed=42)
batch_shuffled.take(10)
```

To require exact consecutive windows, first use {meth}`~ray.data.Dataset.repartition` to construct blocks of the requested size, then shuffle each resulting block independently:

```{testcode}
# Construct 256-row windows, except for the final remainder.
exact_shuffled = ds.repartition(
    target_num_rows_per_block=256, strict=True
).shuffle_batches(batch_size=None, seed=42)
exact_shuffled.take(10)
```

Only the dataset's final window can be smaller in this composition. Window boundaries follow the upstream execution order. Both approaches stream results without materializing the whole dataset.

A local shuffle buffer can retain rows as later rows arrive. With `shuffle_batches`, rows stay within their shuffle batch, and the operation doesn't randomly reorder batches. Use {ref}`global shuffle <global-shuffle>` for a permutation across the whole dataset.

Each shuffle batch must fit in task memory, including input, output, indices, and temporary arrays. The `memory` parameter is a scheduling reservation, not a hard limit. `batch_size` doesn't cap input block sizes or peak memory.

:::{tip}
Combine `shuffle_batches` with {ref}`file order shuffling <shuffling_file_order>` for additional randomness. File order shuffling randomizes which files Ray Data reads first, while `shuffle_batches` randomizes rows within each batch.
:::

Eligible chunked tensor columns use Ray's optimized row selection for [fixed-shaped tensors](https://github.com/ray-project/ray/pull/64961) and [variable-shaped tensors](https://github.com/ray-project/ray/pull/66197), avoiding concatenation of the entire tensor column before gathering rows. Other inputs use existing fallback paths.

#### How does `shuffle_batches` compare to a PyArrow shuffle function?

The following CPU ingestion benchmark compares `shuffle_batches` with a `map_batches` function that calls `batch.take(indices)` on a PyArrow table. Both use the same input layout, batch sizes, task resources, and consumers. The exact-window cases also use the same strict repartition stage. The benchmark enables the tensor row-selection fast path for `shuffle_batches`.

| Usage | PyArrow baseline, rows per second | `shuffle_batches`, rows per second | Speedup |
| --- | ---: | ---: | ---: |
| Target-sized batches | 153,238 | 346,435 | 2.26× |
| Exact-window composition | 102,559 | 326,475 | 3.18× |

The benchmark uses one node with eight vCPUs, an Intel `Xeon Platinum 8362` processor, a 12 GiB object store, and four NumPy consumers. Rows contain 4 KiB tensors across 160 source blocks. The target shuffle batch size is 262,144 rows, and the consumer batch size is 4,096 rows. Each consumer skips 100 warmup batches and measures the next 200. Results are medians of three fresh executions, with no object spilling. The environment uses NumPy 1.26.4 and PyArrow 19.0.1. See the [benchmark script at the measured revision](https://github.com/ray-project/ray/blob/8232d6a11c16ae13c84974abd1d9add8a86c62f4/release/nightly_tests/dataset/benchmark_shuffle_batches.py).

These measurements cover CPU ingestion with model computation excluded. Source blocks are smaller than the target shuffle batch size. Splitting larger input blocks can coalesce tensor chunks during batching and reduce opportunities for the fast path.

(randomizing-block-order)=

### Randomize block order

This option randomizes the order of {ref}`blocks <data_key_concepts>` in a dataset. The operation alone doesn't involve heavy computation or communication, but Ray Data must materialize all blocks in memory before it randomizes their order in the queue for the subsequent operation.

:::{note}
By default, Ray Data doesn't guarantee any particular block order when it reads blocks from different files in parallel, unless you set `DataContext.execution_options.preserve_order` to true. As a result, this option is mainly relevant when Ray Data yields blocks from a relatively small set of large files.
:::

:::{note}
Use this option only when your dataset is small enough to fit in object store memory.
:::

To shuffle the block order, use {meth}`randomize_block_order <ray.data.Dataset.randomize_block_order>`.

```{testcode}
import ray

ds = ray.data.read_text(
    "s3://anonymous@ray-example-data/sms_spam_collection_subset.txt"
)

# Randomize the block order of this dataset.
ds = ds.randomize_block_order()
```

(global-shuffle)=

### Shuffle all rows globally

Ray Data provides the following options for shuffling all rows globally across the whole dataset:

- **Random shuffling**: Call {meth}`~ray.data.Dataset.random_shuffle` to shuffle individual rows from the existing blocks into new blocks. You can optionally provide a seed.
- **Key-based repartitioning**: Call {meth}`~ray.data.Dataset.repartition` with the `keys` parameter to shuffle the rows based on the hash of the values in the key columns you provide. This operation co-locates rows with the same key values deterministically. Ray 2.46 introduced this option.

A shuffle is an expensive operation. It requires materializing the whole dataset in memory, and it acts as a synchronization barrier, so subsequent operators can't start executing until the shuffle completes.

The following example shuffles rows randomly with a seed:

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

# Random shuffle with seed
random_shuffled_ds = ds.random_shuffle(seed=123)
```

The following example hash shuffles rows based on the `id` column:

```{testcode}
import ray

ds = ray.data.range(1000)

hash_shuffled_ds = ds.repartition(keys="id", num_blocks=200)
hash_shuffled_ds.materialize()
```

:::{tip}
By default, key-based repartitioning uses {ref}`shuffle v2 <shuffle-v2>`, which is `ShuffleStrategy.SHUFFLE_V2`. For the available settings, see {ref}`Tune shuffle v2 <tuning-shuffle-v2>`.

To fall back to the previous {ref}`hash-shuffle <hash-shuffle>` implementation, set `DataContext.shuffle_strategy` to `ShuffleStrategy.HASH_SHUFFLE`:

```python
from ray.data.context import DataContext, ShuffleStrategy

DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
```
:::

(optimizing_shuffles)=
(advanced-optimizing-shuffles)=

## Advanced: Optimize shuffles

:::{note}
Shuffle optimization is an active area of development. If your dataset uses a shuffle operation and you're having trouble configuring the shuffle, [file a Ray Data issue on GitHub](https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdata&projects=&template=bug-report.yml&title=[data]+).
:::

### When should you use global per-epoch shuffling?

Use global per-epoch shuffling only if your model is sensitive to the randomness of the training data. According to a [theoretical foundation](https://arxiv.org/abs/1709.10432), all gradient-descent-based model trainers benefit from improved global shuffle quality. In practice, the benefit is particularly pronounced for tabular data and models. However, the more global the shuffle, the more expensive the shuffling operation. Data transfer costs compound this increase in distributed data-parallel training on a multi-node cluster. This cost can be prohibitive for large datasets.

To find the best tradeoff between preprocessing time and cost and per-epoch shuffle quality, measure the precision gain per training step for your model under different shuffling policies, such as no shuffling, local shuffling, or global shuffling.

As long as your data loading and shuffling throughput is higher than your training throughput, your GPU should saturate. If your model is shuffle-sensitive, push the shuffle quality higher until you reach this threshold.

(shuffle_performance_tips)=
(enabling-push-based-shuffle)=

### Enable push-based shuffle

:::{note}
`DataContext.use_push_based_shuffle` and the `RAY_DATA_PUSH_BASED_SHUFFLE` environment variable are deprecated. Select push-based shuffle with `DataContext.shuffle_strategy` or the `RAY_DATA_DEFAULT_SHUFFLE_STRATEGY` environment variable instead, as this section shows.
:::

Some Dataset operations require a *shuffle* operation, which shuffles data from all of the input partitions to all of the output partitions. These operations include {meth}`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`, {meth}`Dataset.sort <ray.data.Dataset.sort>`, and {meth}`Dataset.groupby <ray.data.Dataset.groupby>`. For example, a sort operation reorders data between blocks, so it requires shuffling across partitions. Shuffling can be hard to scale to large data sizes and clusters, especially when the total dataset size doesn't fit in memory.

Ray Data provides an alternative shuffle implementation called push-based shuffle to improve large-scale performance. Try it if your dataset has more than 1,000 blocks or is larger than 1 TB.

To try it locally or on a cluster, start with the [nightly release test](https://github.com/ray-project/ray/blob/master/release/nightly_tests/dataset/random_shuffle_benchmark.py) that Ray runs for {meth}`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`. The following chart shows run time results for {meth}`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` on 1 to 10 TB of data, which gives an idea of the performance you can expect. The benchmark ran on 20 m5.4xlarge AWS EC2 instances, each with 16 vCPUs and 64 GB of RAM.

```{image} https://docs.google.com/spreadsheets/d/e/2PACX-1vQvBWpdxHsW0-loasJsBpdarAixb7rjoo-lTgikghfCeKPQtjQDDo2fY51Yc1B6k_S4bnYEoChmFrH2/pubchart?oid=598567373&format=image
:alt: Run time of Dataset.random_shuffle on 1 to 10 TB of data
:align: center
```

To try push-based shuffle, set the `RAY_DATA_DEFAULT_SHUFFLE_STRATEGY=sort_shuffle_push_based` environment variable when you run your application:

```bash
wget https://raw.githubusercontent.com/ray-project/ray/master/release/nightly_tests/dataset/random_shuffle_benchmark.py
RAY_DATA_DEFAULT_SHUFFLE_STRATEGY=sort_shuffle_push_based python random_shuffle_benchmark.py --num-partitions=10 --partition-size=1e7
```

You can also set the shuffle strategy while your program runs with `DataContext.shuffle_strategy`:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray

from ray.data.context import ShuffleStrategy

ctx = ray.data.DataContext.get_current()
ctx.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED

ds = (
    ray.data.range(1000)
    .random_shuffle()
)
```

Large-scale shuffles can take a while to finish. For debugging, you can execute only part of a shuffle, so that you can collect an execution profile more quickly. The following example limits a random shuffle operation to two output blocks:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
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
```

```{testoutput}
:options: +MOCK

Operator 1 ReadRange->RandomShuffle: executed in 0.08s

    Suboperator 0 ReadRange->RandomShuffleMap: 2/2 blocks executed
    ...
```
