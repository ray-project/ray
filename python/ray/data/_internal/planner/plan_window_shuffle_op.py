from typing import Iterable, List

import pyarrow as pa

from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import WindowShuffle
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.context import DataContext


def plan_window_shuffle_op(
    op: WindowShuffle,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan the built-in shuffle without the user-defined map machinery."""
    assert len(physical_children) == 1
    shuffle_fn = op.fn
    seed_config = op.seed_config

    def shuffle_windows(
        batches: Iterable[pa.Table], _: TaskContext
    ) -> Iterable[pa.Table]:
        # Restart enumeration for every attempt, including in-process retries.
        for batch_idx, batch in enumerate(batches):
            yield shuffle_fn(batch, seed_config, batch_idx=batch_idx)

    transform = BatchMapTransformFn(
        shuffle_windows,
        batch_size=None if op.strict else op.window_size,
        batch_format="pyarrow",
        zero_copy_batch=True,
        output_block_size_option=OutputBlockSizeOption.of(
            target_max_block_size=data_context.target_max_block_size,
        ),
    )
    return MapOperator.create(
        MapTransformer([transform]),
        physical_children[0],
        data_context,
        name=op.name,
        compute_strategy=get_compute(op.compute),
        min_rows_per_bundle=op.min_rows_per_bundled_input,
        ray_remote_args=op.ray_remote_args,
        ray_remote_args_fn=op.ray_remote_args_fn,
        # Preserve task grouping and the task/batch-indexed RNG stream.
        supports_fusion=False,
    )
