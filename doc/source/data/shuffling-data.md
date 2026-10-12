---
myst:
  html_meta:
    description: "Shuffle Ray Data at different granularities: file order, local buffer shuffle, parallel window shuffle, map_batches shuffle, block order, and global per-epoch shuffle."
---

(shuffling_data)=

# Shuffling data

When you consume or iterate over a Ray Data {class}`Dataset <ray.data.dataset.Dataset>`, shuffling the order of the data can be useful, for example to randomize the ingest order during ML training. This guide describes several methods for shuffling data with Ray Data and the trade-offs of each.

(types-of-shuffling)=

## Choose a shuffling method

Choose a method based on which rows need to mix, where the shuffle runs, and the
memory available to that stage. Increasing throughput doesn't imply equivalent
randomness across methods.

| Your requirement | Method | Mixing scope and execution cost |
| --- | --- | --- |
| Change the order in which files are read | {ref}`File order shuffle <shuffling_file_order>` | Shuffles file metadata; preserves row order within each file. |
| Mix rows across successive iterator batches | {ref}`Local buffer shuffle <local_shuffle_buffer>` | Uses a rolling buffer on the consuming worker. |
| Shuffle batches on Ray Data workers before consumption | {ref}`Window shuffle <window_shuffle>` | Independently permutes target-sized batches; task tails can be smaller. |
| Require consecutive windows of exactly a given row count | {ref}`Strict window shuffle <window_shuffle>` | Adds streaming repartition; only the final window can be smaller. |
| Apply a custom batch permutation | {ref}`Custom shuffle with map_batches <map_batches_shuffle>` | Runs your shuffle function on workers with target-sized batches. |
| Mix rows across the entire dataset | {ref}`Global shuffle <global-shuffle>` | Requires an expensive all-to-all shuffle and materialization. |

You can combine file order shuffling with either local buffer or window
shuffling. For training, compare model quality as well as throughput when
changing the mixing scope.

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

This option maintains a rolling shuffle buffer during iteration. The
`local_shuffle_buffer_size` parameter sets the minimum row threshold for the
buffer, rather than an exact window size or a memory cap. Rows retained in the
buffer can mix with newly arriving rows across successive output batches. The
buffer drains when the input ends. For more details, see
{ref}`Iterate over batches with shuffling <iterating-over-batches-with-shuffling>`.

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
If throughput drops when you use `local_shuffle_buffer_size`, check the total
time spent in batch creation. In the `ds.stats()` output, find `In batch
formatting` under `Batch iteration time breakdown`. If batch formatting is a
bottleneck and independent batch permutations provide sufficient mixing, try
{ref}`window shuffle <window_shuffle>` to move the work to Ray Data workers.
Keep local buffer shuffle when mixing across successive batches matters for
your workload. Decreasing the buffer size or using only
{ref}`file order shuffling <shuffling_file_order>` also changes the mixing scope;
evaluate model quality before making that change.
:::

(window_shuffle)=

### Shuffle rows within parallel windows

Use {meth}`~ray.data.Dataset.window_shuffle` to shuffle rows on Ray Data workers
before consuming them in a training loop when an independent permutation of each
shuffle batch meets your needs. This API is alpha. It divides the input execution
sequence into batches and applies a uniform random permutation within each batch.
By default, `window_size` is the target batch size, following `map_batches`
grouping. Batches can span input blocks. A task's final batch can be smaller,
including in the middle of the dataset. This mode adds no repartition stage.

Set `strict=True` to construct consecutive fixed-size windows with streaming
repartition. Each full window has `window_size` rows, and only the final window
can contain fewer rows. Each window is shuffled in a separate task. Streaming
repartition produces intermediate windows without materializing the whole dataset.
Strict mode fixes the membership and size of consecutive windows for a given
input execution sequence. It doesn't mix rows across windows or guarantee better
training results than the default mode.

```{testcode}
import ray

ds = ray.data.range(1000)
shuffled = ds.window_shuffle(
    window_size=250,
    seed=42,
    compute=ray.data.TaskPoolStrategy(size=4),
)

# Iterate directly. Materialization isn't required.
for batch in shuffled.iter_batches(batch_size=32):
    pass

# Require exact consecutive windows, except for the final window.
strict_shuffled = ds.window_shuffle(window_size=250, strict=True, seed=42)
for batch in strict_shuffled.iter_batches(batch_size=32):
    pass
```

Rows never move between shuffle batches. This differs from a rolling local shuffle
buffer, which can mix rows across successive output batches, and from a global
shuffle. If the input is grouped by label, time, or another correlated key,
window shuffle can leave correlations between rows when a shuffle batch covers
only one group. Choose a window size that provides enough mixing for your workload, or use
{ref}`global shuffle <global-shuffle>` when necessary.

The operation is lazy and doesn't materialize either its input or output.
Shuffle batches can be consumed while upstream data is still
arriving. When downstream consumption slows, the existing streaming executor
applies backpressure to task submission and output reading. Each window must
still fit in a shuffle task's memory. Blocking upstream operations can delay
the first window; the shuffle itself doesn't wait for the entire dataset.
Calling `shuffled.materialize()` caches a particular shuffled result, and
reading that cached result again reuses its order. Use `RandomSeedConfig` with
`reseed_after_execution=True` to change permutations between executions of an
unmaterialized pipeline. For reproducibility, enable
`DataContext.execution_options.preserve_order` and keep the input layout, mode,
and task grouping fixed. Without preserving order, upstream task completion order
can also change which rows form each window.
Completed windows can also be emitted out of order unless you preserve order.

Larger windows require more memory. Budget for the input, permuted output,
indices, and temporary arrays. The `memory` argument reserves scheduling
resources for shuffle tasks. It isn't a hard memory cap and doesn't cover
object-store usage or strict window-construction tasks. `compute` limits
shuffle-task parallelism and doesn't limit upstream stages. With `strict=True`,
intermediate window blocks can add data transfer or spilling. Use the default
mode when target-sized batch shuffling meets your needs. Use `strict=True` when
you need exact window membership across varying input block sizes, and measure
the execution cost on your input layout.

(map_batches_shuffle)=

### Customize row shuffling with `map_batches`

Use {meth}`~ray.data.Dataset.map_batches` when you need a custom shuffle function.
For an independent uniform permutation with Ray-managed seed and reseeding
behavior, use {ref}`window shuffle <window_shuffle>`.

Both approaches move shuffle work out of the iterator and onto Ray Data workers.
Tasks can run in parallel across machines, and streaming execution can overlap
shuffling with downstream processing. Shuffle tasks still consume CPU, memory,
and data transfer bandwidth, and can become a pipeline bottleneck.

The `batch_size` parameter sets a target batch size. A task can process multiple
batches, and its final batch can be smaller, including in the middle of the
dataset. Each actual batch is shuffled independently. Increasing `batch_size`
allows more rows to mix within a batch and requires more memory, but doesn't
mix rows across batches. Use `window_shuffle(..., strict=True)` if exact
consecutive window membership is required.

:::{important}
For large batches, set `memory` to reserve heap memory for each shuffle task
during scheduling. This reservation isn't a hard cap and doesn't include
object-store memory. `batch_size * row_bytes` estimates only the input payload;
budget for the permuted output, indices, and temporary allocations as well.
Measure peak memory for your data and shuffle function.
:::

```{testcode}
import numpy as np
import pyarrow as pa
import ray

def random_shuffle(batch: pa.Table) -> pa.Table:
    indices = np.random.permutation(len(batch))
    return batch.take(indices)

row_bytes = 512 * np.dtype(np.int64).itemsize  # 4 KiB tensor payload per row
target_shuffle_bytes = 4 * 2**20  # Target 4 MiB of input payload per batch
batch_size = target_shuffle_bytes // row_bytes

ds = ray.data.range_tensor(4096, shape=(512,))
ds = ds.map_batches(
    random_shuffle,
    batch_size=batch_size,
    batch_format="pyarrow",
    memory=256 * 2**20,  # Scheduling reservation; tune for measured peak memory
)
for batch in ds.iter_batches(batch_size=256):
    pass
```

:::{tip}
Combine `map_batches` shuffle with {ref}`file order shuffling <shuffling_file_order>`
for additional randomness. File order shuffling randomizes which files Ray Data
reads first, while `map_batches` shuffle randomizes rows within each actual
shuffle batch. A custom function using NumPy's process-global RNG, as in this
example, doesn't inherit `window_shuffle` seed or retry behavior.
:::

(map_batches_vs_local_shuffle)=
(comparing-local-buffer-shuffle-and-map_batches-shuffle)=

#### How does local buffer shuffle compare to `map_batches` shuffle?

The following benchmark compares steady-state training throughput for local buffer shuffle and `map_batches` shuffle on a synthetic workload. The workload uses `ray.data.range_tensor` with about 4 KB per row, four GPU workers, a batch size of 4096, and 200 steps with 100 warmup steps.

These results measure the `map_batches` implementation in that workload, not
`window_shuffle` or its strict mode. The two methods have different mixing
semantics; throughput alone doesn't establish equivalent training quality.

:::{list-table} Local buffer shuffle versus `map_batches` shuffle
:header-rows: 1
:widths: 30 20 15

* - Method
  - Throughput (rows/s)
  - \% of baseline
* - No shuffle (baseline)
  - 1,759,282
  - 100%
* - Local buffer shuffle 1 GB
  - 225,181
  - 13%
* - Local buffer shuffle 2 GB
  - 220,644
  - 13%
* - Local buffer shuffle 3 GB
  - 153,256
  - 9%
* - `map_batches` shuffle 1 GB
  - 1,400,734
  - 80%
* - `map_batches` shuffle 2 GB
  - 1,460,037
  - 83%
* - `map_batches` shuffle 3 GB
  - 1,588,428
  - 90%
:::

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
