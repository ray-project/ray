---
myst:
  html_meta:
    description: "Combine multiple Ray Data Datasets into one streaming dataset with weighted mixing strategies, block size control, and stopping conditions."
---

(mixing_data)=

# Weighted dataset mixing

This page describes how to combine multiple datasets into a single streaming dataset and control how often rows from each source appear. Weighted mixing helps with goals such as the following:

- **Class or scenario balancing**: upsample rare scenarios or harder tasks so training batches see them more often.
- **Multi-task pretraining**: combine code and web text datasets at fixed ratios.
- **Catastrophic forgetting prevention**: keep a small fraction of an older dataset in the mix while training on a newer one.

## Quickstart

```{testcode}
import ray.data
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

# Read and preprocess each source independently.
# NOTE: These are mocked datasets for demonstration purposes.
def preprocess(row):
    return row

ds1 = ray.data.from_items([{"x": 1} for _ in range(750)]).map(preprocess)
ds2 = ray.data.from_items([{"x": 2} for _ in range(250)]).map(preprocess)

# Output batches will contain 75% rows from ds1, 25% from ds2 (in expectation).
mixed = ds1.mix(ds2, weights=[0.75, 0.25])

def train_fn_per_worker(config):
    shard = ray.train.get_dataset_shard("train")
    for batch in shard.iter_torch_batches(batch_size=128):
        print(batch)

trainer = TorchTrainer(
    train_loop_per_worker=train_fn_per_worker,
    scaling_config=ScalingConfig(num_workers=4),
    datasets={"train": mixed},
)
```

(mixing-strategies)=
## Choose a mixing strategy

Compose {meth}`~ray.data.Dataset.mix` with other Ray Data operations to implement a mixing strategy that matches how granular you want the mixing ratio to be. The following sections cover two strategies. *Per-block mixing* uses {meth}`~ray.data.Dataset.mix` on its own. *Random mixing* follows {meth}`~ray.data.Dataset.mix` with a shuffle.

(per-block-mixing)=
### How does per-block mixing work?

By default, each output block comes from exactly one input dataset. {meth}`~ray.data.Dataset.mix` keeps a running row count for each source. At every step, it pulls the next block from whichever dataset is furthest behind its target ratio. Over time, the cumulative row counts converge to the requested weights.

Suppose you mix two datasets, `ds1` and `ds2`, with `weights=[0.75, 0.25]`, and both sources produce blocks of equal size. The pipeline then splits across four training workers, and data parallel training builds a global batch across all workers.

```{image} /data/images/dataset_mixing/per_block_mix.png
:alt: Per-block mixing, where blocks from ds1 and ds2 interleave in a 3:1 pattern and then split across four training workers to form a global batch.
```

With uniform block sizes, the ratio is exact within any window of `1 / min(weights)` blocks. With `weights=[0.9, 0.1]`, every 10-block window contains at least one block from the second dataset.

:::{note}
{ref}`Blocks <dataset_concept>` are the unit of data transfer in Ray Data, and they don't map one-to-one to training batches. Workers build each batch by pulling rows from one or more blocks. With per-block mixing, each local batch might contain data from one or more of the input datasets, depending on how block sizes compare to batch sizes. The next section shows how to align them with a streaming repartition.
:::

#### Advanced: Standardize input block sizes

If your input datasets produce blocks that differ widely in size, a single large block can temporarily push that source ahead of its target ratio. {meth}`~ray.data.Dataset.mix` self-corrects on later pulls, so the ratio is still correct in expectation. However, a global batch built from a small number of those blocks can look skewed.

To tighten the per-batch window, standardize input block sizes upstream with {meth}`ds.repartition(target_num_rows_per_block) <ray.data.Dataset.repartition>`:

```{testcode}
LOCAL_BATCH_SIZE = 128

ds1 = ray.data.from_items([{"x": 1} for _ in range(750)]).map(preprocess)
ds2 = ray.data.from_items([{"x": 2} for _ in range(250)]).map(preprocess)

# Standardize block sizes so the ratio holds within tighter windows.
ds1 = ds1.repartition(target_num_rows_per_block=LOCAL_BATCH_SIZE)
ds2 = ds2.repartition(target_num_rows_per_block=LOCAL_BATCH_SIZE)

mixed = ds1.mix(ds2, weights=[0.75, 0.25])
```

:::{tip}
If your rows are small in bytes, repartition to a multiple of the batch size, such as `N * LOCAL_BATCH_SIZE`. This prevents splitting blocks into tiny pieces that increase overhead.
:::

(random-mixing)=
### Add a shuffle for random mixing

Two factors determine how closely each batch matches the target ratio under per-block mixing. The first is the size of the input blocks, which the preceding section covers. The second is the number of training workers that contribute to each global batch. A global batch aggregates `num_workers * grad_accum_steps` local batches, each drawn from a single dataset. The more local batches each global batch contains, the closer its ratio stays to the target.

In the extreme case, you train on a single worker with no gradient accumulation. Every global batch is then a single local batch, so every batch comes from a single dataset.

To switch to random mixing, add a streaming shuffle after {meth}`~ray.data.Dataset.mix`. The shuffle redistributes rows across block boundaries, so each batch directly contains rows from multiple datasets in roughly the requested proportion, regardless of how many workers you train on. {meth}`~ray.data.Dataset.mix` still governs the ratio, and the shuffle spreads that ratio within each batch.

```{image} /data/images/dataset_mixing/random_mix.png
:alt: Random mixing, where a shuffle after mix() redistributes rows so that each worker batch contains rows from multiple datasets in the target proportion.
```

Use either of the following streaming-friendly shuffle options in Ray Data:

- {ref}`Local buffer shuffle <local_shuffle_buffer>`, which you enable by passing `local_shuffle_buffer_size` to {meth}`~ray.data.DataIterator.iter_batches`.
- {ref}`map_batches shuffle <map_batches_shuffle>`.

```{testcode}
import numpy as np
import pyarrow as pa

LOCAL_BATCH_SIZE = 128

ds1 = ray.data.from_items([{"x": 1} for _ in range(750)]).map(preprocess)
ds2 = ray.data.from_items([{"x": 2} for _ in range(250)]).map(preprocess)

ds1 = ds1.repartition(target_num_rows_per_block=LOCAL_BATCH_SIZE)
ds2 = ds2.repartition(target_num_rows_per_block=LOCAL_BATCH_SIZE)

mixed = ds1.mix(ds2, weights=[0.75, 0.25])

# Add a shuffle after mix() to get random mixing.
def random_shuffle(batch: pa.Table) -> pa.Table:
    indices = np.random.permutation(len(batch))
    return batch.take(indices)

# Set the shuffle buffer size to be large enough for good mixing quality across datasets.
SHUFFLE_BUFFER_SIZE = 64 * LOCAL_BATCH_SIZE
mixed = mixed.map_batches(random_shuffle, batch_size=SHUFFLE_BUFFER_SIZE, batch_format="pyarrow")
```

(stopping-conditions)=
## Choose a stopping condition

The stopping condition determines when the mixed pipeline ends. The following table describes each condition.

```{list-table}
:header-rows: 1

* - Condition
  - Behavior
* - `STOP_ON_LONGEST_DROP`
  - The default. The pipeline ends when the longest dataset is exhausted. Each shorter dataset drops out when it's exhausted, and the remaining batches come from the datasets that are still active.
* - `STOP_ON_SHORTEST`
  - The pipeline ends when the shortest dataset is exhausted. The other datasets are truncated.
```

For details, see {class}`~ray.data.MixStoppingCondition`.

## Limitations

Keep the following limitations in mind when you mix datasets:

- **Transform before mixing.** Avoid calling {meth}`~ray.data.Dataset.map` or {meth}`~ray.data.Dataset.filter` after {meth}`~ray.data.Dataset.mix`. Downstream transformations can combine or split blocks before they reach the trainer, which breaks the row-ratio guarantees that {meth}`~ray.data.Dataset.mix` provides. Apply per-dataset transforms upstream of {meth}`~ray.data.Dataset.mix`.
- **Match input schemas.** {meth}`~ray.data.Dataset.mix` doesn't unify schemas for you. Apply {meth}`~ray.data.Dataset.map` or {meth}`~ray.data.Dataset.select_columns` upstream to make all inputs structurally identical.
- **Avoid heavily skewed weights.** All input datasets execute concurrently, with a portion of cluster resources divided equally between them. With heavily skewed weights, such as `[0.95, 0.05]`, the high-weight dataset might bottleneck while the low-weight dataset idles. Keep weights within roughly 5x of each other, such as `[0.4, 0.3, 0.2, 0.1]`.

## See also

- {ref}`Using Ray Data with Ray Train for distributed training and data ingest <data-ingest-torch>`
- {ref}`Ray Data shuffling solutions <shuffling_data>`
- {meth}`ray.data.Dataset.repartition`
