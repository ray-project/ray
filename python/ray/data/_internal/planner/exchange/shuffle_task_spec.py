import logging
import math
from typing import List, Optional, Tuple, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR

logger = logging.getLogger(__name__)


def _partition_block(
    block: BlockAccessor,
    output_num_blocks: int,
    random_shuffle: bool,
    seed_i: Optional[int],
) -> List[Block]:
    """Partition a single block into ``output_num_blocks`` slices.

    Shared by both the standalone shuffle path (``ShuffleTaskSpec.map``) and the
    fused shuffle path (``ShuffleMapTransformFn``).

    Args:
        block: The block accessor wrapping the block to partition.
        output_num_blocks: Number of output partitions.
        random_shuffle: Whether to randomize the block before slicing and
            shuffle the resulting slice order.
        seed_i: Per-task random seed (``base_seed + task_idx``), or ``None``.

    Returns:
        A list of block slices.
    """
    if random_shuffle:
        block = block.random_shuffle(seed_i)
        block = BlockAccessor.for_block(block)

    slice_sz = max(1, math.ceil(block.num_rows() / output_num_blocks))
    slices = []
    for i in range(output_num_blocks):
        slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz))

    if random_shuffle:
        random = np.random.RandomState(seed_i)
        random.shuffle(slices)

    num_rows = sum(BlockAccessor.for_block(s).num_rows() for s in slices)
    assert num_rows == block.num_rows(), (num_rows, block.num_rows())

    return slices


class ShuffleTaskSpec(ExchangeTaskSpec):
    """
    The implementation for shuffle tasks.

    This is used by random_shuffle() and repartition().
    """

    SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME = "Split Repartition"

    def __init__(
        self,
        target_shuffle_max_block_size: int,
        random_shuffle: bool = False,
        random_seed: Optional[int] = None,
    ):
        super().__init__(
            map_args=[
                target_shuffle_max_block_size,
                random_shuffle,
                random_seed,
            ],
            reduce_args=[random_shuffle, random_seed],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        target_shuffle_max_block_size: int,
        random_shuffle: bool,
        random_seed: Optional[int],
    ) -> List[Union[Block, "BlockMetadataWithSchema"]]:
        stats = BlockExecStats.builder()
        block = BlockAccessor.for_block(block)
        if (
            block.size_bytes()
            > MAX_SAFE_BLOCK_SIZE_FACTOR * target_shuffle_max_block_size
        ):
            logger.warning(
                "Input block to map task has size "
                f"{block.size_bytes() // (1024 * 1024)}MiB, which exceeds "
                "DataContext.get_current().target_shuffle_max_block_size="
                f"{target_shuffle_max_block_size // (1024 * 1024)}MiB. "
                "This can lead to out-of-memory errors and can happen "
                "when map tasks are fused to the shuffle operation. "
                "To prevent fusion, call Dataset.materialize() on the "
                "dataset before shuffling."
            )

        seed_i = random_seed + idx if random_seed is not None else None
        slices = _partition_block(block, output_num_blocks, random_shuffle, seed_i)

        from ray.data.block import BlockMetadataWithSchema

        meta = block.get_metadata(exec_stats=stats.build())
        schema = block.schema()
        meta_with_schema = BlockMetadataWithSchema(metadata=meta, schema=schema)
        return slices + [meta_with_schema]

    @staticmethod
    def reduce(
        random_shuffle: bool,
        random_seed: Optional[int],
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, "BlockMetadataWithSchema"]:
        # TODO: Support fusion with other downstream operators.
        stats = BlockExecStats.builder()
        builder = DelegatingBlockBuilder()
        for block in mapper_outputs:
            builder.add_block(block)
        new_block = builder.build()
        accessor = BlockAccessor.for_block(new_block)
        if random_shuffle:
            new_block = accessor.random_shuffle(
                random_seed if random_seed is not None else None
            )
            accessor = BlockAccessor.for_block(new_block)
        new_metadata = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            input_files=None,
            exec_stats=stats.build(),
        )
        from ray.data.block import BlockMetadataWithSchema

        meta_with_schema = BlockMetadataWithSchema(
            metadata=new_metadata, schema=accessor.schema()
        )
        return new_block, meta_with_schema
