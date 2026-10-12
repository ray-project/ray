"""Window shuffle using batch maps and optional strict repartition."""

from typing import Callable, Iterable

import numpy as np
import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import take_table
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import WindowShuffle
from ray.data._internal.random_config import RandomSeedConfig, get_timestamp_seed
from ray.data.context import DataContext
from ray.data.dataset import Dataset


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


def _shuffle_windows(
    batches: Iterable[pa.Table],
    ctx: TaskContext,
    *,
    shuffle_fn: Callable,
    seed_config: RandomSeedConfig,
) -> Iterable[pa.Table]:
    # Enumeration restarts for each transform attempt, including in-process
    # retries. Keep the batch index out of mutable task or worker state.
    for batch_idx, batch in enumerate(batches):
        yield shuffle_fn(batch, seed_config, batch_idx=batch_idx)


def window_shuffle(
    self: Dataset,
    window_size: int,
    *,
    strict: bool = False,
    seed: int | RandomSeedConfig | None = None,
    compute: TaskPoolStrategy | None = None,
    num_cpus: float | None = None,
    memory: float | None = None,
) -> Dataset:
    if isinstance(window_size, bool) or not isinstance(window_size, int):
        raise TypeError("window_size must be an integer")
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    if not isinstance(strict, bool):
        raise TypeError("strict must be a boolean")
    if compute is not None and not isinstance(compute, TaskPoolStrategy):
        raise TypeError("window_shuffle currently supports TaskPoolStrategy only")

    seed_config = RandomSeedConfig.create_seed_config(seed)
    if seed_config.seed is not None:
        # Validate on the driver rather than failing a remote shuffle task.
        np.random.SeedSequence(seed_config.seed)
    if window_size == 1:
        return self
    if seed_config.seed is None:
        # Pin the base seed when constructing the plan, so task retries replay
        # the same permutation. Independent executions may reseed, like epochs.
        seed_config = RandomSeedConfig(
            seed=get_timestamp_seed(), reseed_after_execution=True
        )

    if strict:
        # Consume each prebuilt window whole, preserving its tensor chunks.
        source = self.repartition(target_num_rows_per_block=window_size, strict=True)
        batch_size = None
    else:
        source = self
        batch_size = window_size
    remote_args = {}
    if num_cpus is not None:
        remote_args["num_cpus"] = num_cpus
    if memory is not None:
        remote_args["memory"] = memory

    op = WindowShuffle(
        fn=_shuffle_window,
        input_dependencies=[source._logical_plan.dag],
        strict=strict,
        batch_size=batch_size,
        min_rows_per_bundled_input=batch_size,
        batch_format="pyarrow",
        zero_copy_batch=True,
        fn_args=(seed_config,),
        compute=compute,
        ray_remote_args=remote_args,
    )
    return Dataset._from_parent(source, LogicalPlan(op, self.context))
