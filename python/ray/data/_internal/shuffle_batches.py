"""Retry-stable row permutations for batch shuffle tasks."""

import numpy as np

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.random_config import RandomSeedConfig
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext


def _shuffle_batch(
    batch: Block, seed_config: RandomSeedConfig, *, batch_idx: int = 0
) -> Block:
    ctx = TaskContext.get_current()
    if ctx is None:
        raise RuntimeError("Batch shuffle must execute inside a Ray Data map task")

    # Use a fresh generator to reproduce each batch across task retries. A
    # task can process multiple batches, each with its own stream.
    seed_tuple = seed_config.get_seed_tuple(data_context=DataContext.get_current())
    assert seed_tuple is not None, "The shuffle base seed must be pinned on the driver"
    rng_args = seed_tuple.to_rng_args(ctx.task_idx)
    if batch_idx:
        rng_args += (batch_idx,)
    rng = np.random.default_rng(rng_args)
    accessor = BlockAccessor.for_block(batch)
    indices = rng.permutation(accessor.num_rows())
    return accessor.take(indices)
