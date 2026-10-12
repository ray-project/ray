"""Retry-stable row permutations for window shuffle tasks."""

import numpy as np
import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import take_table
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.random_config import RandomSeedConfig
from ray.data.context import DataContext


def _shuffle_window(
    batch: pa.Table, seed_config: RandomSeedConfig, *, batch_idx: int = 0
) -> pa.Table:
    ctx = TaskContext.get_current()
    if ctx is None:
        raise RuntimeError("Window shuffle must execute inside a Ray Data map task")

    # Use a fresh generator to reproduce each batch across task retries. A
    # non-strict task can process multiple batches, each with its own stream.
    seed_tuple = seed_config.get_seed_tuple(data_context=DataContext.get_current())
    assert seed_tuple is not None, "The shuffle base seed must be pinned on the driver"
    rng_args = seed_tuple.to_rng_args(ctx.task_idx)
    if batch_idx:
        rng_args += (batch_idx,)
    rng = np.random.default_rng(rng_args)
    indices = rng.permutation(batch.num_rows)
    return take_table(batch, indices)
