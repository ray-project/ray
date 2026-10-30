from itertools import chain
from typing import Iterable, List

from ray.data._internal import shuffle_batches as shuffle_batches_impl
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import ShuffleBatches
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext


def plan_shuffle_batches_op(
    op: ShuffleBatches,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan the built-in shuffle without the user-defined map machinery."""
    assert len(physical_children) == 1
    assert op.compute is not None
    shuffle_fn = shuffle_batches_impl._shuffle_batch
    seed_config = op.seed_config
    batch_size = op.batch_size

    def shuffle_batches(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        blocks_iter = iter(blocks)
        first_block = next(blocks_iter, None)
        if first_block is None:
            return
        empty_block = (
            first_block
            if BlockAccessor.for_block(first_block).num_rows() == 0
            else None
        )
        batches = batch_blocks(
            blocks=chain((first_block,), blocks_iter),
            stats=None,
            batch_size=batch_size,
            batch_format=None,
            ensure_copy=False,
        )
        del first_block
        has_batches = False
        # Restart enumeration for every attempt, including in-process retries.
        for batch_idx, batch in enumerate(batches):
            has_batches = True
            yield shuffle_fn(batch, seed_config, batch_idx=batch_idx)
        if not has_batches and empty_block is not None:
            # Batching skips empty inputs. Preserve their native schema instead
            # of letting output shaping synthesize an untyped empty block.
            yield empty_block

    # Preserve native blocks through output shaping. Batch-map output builders
    # can convert Pandas batches to Arrow, changing the schema of a permutation.
    transform = BlockMapTransformFn(
        shuffle_batches,
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
