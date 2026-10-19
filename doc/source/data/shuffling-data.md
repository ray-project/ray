---
myst:
  html_meta:
    description: "Shuffle Ray Data at different granularities: file order, local buffer shuffle, distributed window shuffle, block order, and global per-epoch shuffle."
---

(shuffling_data)=

# Shuffling data

When you consume or iterate over a Ray Data {class}`Dataset <ray.data.dataset.Dataset>`, shuffling the order of the data can be useful, for example to randomize the ingest order during ML training. This guide describes several methods for shuffling data with Ray Data and the trade-offs of each.

(types-of-shuffling)=

## Choose a shuffling method

Ray Data provides several options for shuffling data. Each option trades off the granularity of shuffle control against memory consumption and runtime. The following sections describe each option and its trade-offs. Choose the method that fits your use case.

For streaming row shuffling, use an iterator-local rolling buffer or distribute window shuffling across Ray Data tasks. The distributed option has two modes: target-sized batches and exact consecutive windows. See the {ref}`comparison below <row-shuffle-comparison>` to choose between them.

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

Each iterator maintains a rolling buffer controlled by `local_shuffle_buffer_size`. Rows can remain in the buffer while later rows arrive, so the shuffle can mix rows across successive output batches. The buffer operates on the rows available to that iterator. For more details, see {ref}`Iterate over batches with shuffling <iterating-over-batches-with-shuffling>`.

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
If throughput drops when you use `local_shuffle_buffer_size`, check the total time spent in batch creation. In the `ds.stats()` output, find `In batch formatting` under `Batch iteration time breakdown`. If this time is much larger than the time spent in other steps, decrease `local_shuffle_buffer_size` or move the shuffle work to {ref}`distributed windows <window_shuffle>`. You can combine either method with {ref}`file order shuffling <shuffling_file_order>`.
:::

(window_shuffle)=
(map_batches_shuffle)=

### Shuffle rows in distributed windows

Use {meth}`~ray.data.Dataset.window_shuffle` to shuffle rows in parallel Ray Data tasks across the cluster before consuming them. This moves shuffle work out of the consumer's iterator. The alpha API manages seeds and uses Ray's optimized row selection for eligible tensor columns.

The API provides two modes. With `strict=False`, the default, `window_size` is a target batch size. Ray groups input blocks into tasks and each task splits its input into shuffle batches. Its final batch can be smaller than `window_size`, including in the middle of the dataset. With `strict=True`, streaming repartition first constructs consecutive windows of exactly `window_size` rows. Only the final window in the dataset can be smaller. Strict mode adds a window-construction stage. Both modes stream results without materializing the whole dataset.

For example, suppose the input has four consecutive blocks of five rows each and `window_size=8`. If the default mode groups two input blocks per task, the shuffle batch sizes are `[8, 2, 8, 2]`. Strict mode produces shuffle windows of `[8, 8, 4]`. Each shuffle batch receives an independent random permutation, and rows stay within that batch. The `batch_size` you pass to `iter_batches` only controls the batches you consume, independently of the shuffle window sizes.

```{testcode}
import ray

ds = ray.data.range(1000, override_num_blocks=8)
shuffled = ds.window_shuffle(
    window_size=250,
    seed=42,
    compute=ray.data.TaskPoolStrategy(size=4),
)
for batch in shuffled.iter_batches(batch_size=32):
    pass

# Require exact consecutive windows, except for the final window.
strict_shuffled = ds.window_shuffle(window_size=250, strict=True, seed=42)
for batch in strict_shuffled.iter_batches(batch_size=32):
    pass
```

(row-shuffle-comparison)=
(map_batches_vs_local_shuffle)=
(comparing-local-buffer-shuffle-and-map_batches-shuffle)=

#### How do local and distributed row shuffling differ?

Choose a streaming row shuffle based on where you want the work to run and which mixing scope you need:

| Method | Where shuffle work runs | Mixing scope | When to use it |
| --- | --- | --- | --- |
| Local buffer | Inside each consumer's iterator | Rolling buffer that can mix rows across successive output batches | You want iterator-local mixing and have CPU and memory available in each consumer. |
| Distributed, `strict=False` | Parallel Ray Data tasks | Independent target-sized batches, with possible short batches at task boundaries | You want to offload shuffle work and can accept windows that follow task grouping. |
| Distributed, `strict=True` | Parallel Ray Data tasks after streaming repartition | Independent consecutive fixed-size windows, except for the dataset's final window | You need exact window membership and can accept the additional window-construction stage. |

Local and distributed window shuffling have different mixing scopes. For a dataset larger than the buffer or windows, these methods don't provide a uniform random permutation of all rows. Use {ref}`global shuffle <global-shuffle>` when you need that scope. You can combine either streaming method with {ref}`file order shuffling <shuffling_file_order>`.

Larger windows mix more rows together and require more memory. Each distributed window must fit in a shuffle task's memory. Budget for input, output, indices, and temporary arrays. Input blocks can exceed the shuffle window size, so `window_size` doesn't cap task input size or peak memory. The `memory` argument is a scheduling reservation rather than a hard memory cap. It doesn't include object-store usage or strict window-construction tasks. Strict mode also adds intermediate window blocks, so measure its cost on your workload.

For reproducible distributed output, enable `DataContext.execution_options.preserve_order` and keep the input layout, mode, and task grouping fixed. Window boundaries follow the upstream execution order. {class}`~ray.data.RandomSeedConfig` controls reseeding between executions. Calling `materialize()` caches a particular shuffled order.

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
