import math
from typing import Callable, Iterable, List, Optional, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.push_based_shuffle import PushBasedShufflePlan
from ray.data._internal.shuffle import ShuffleOp, SimpleShufflePlan
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata


class _ShufflePartitionOp(ShuffleOp):
    """
    Operator used for `random_shuffle` and `repartition` transforms.
    """

    def __init__(
        self,
        block_udf=None,
        random_shuffle: bool = False,
        random_seed: Optional[int] = None,
    ):
        super().__init__(
            map_args=[block_udf, random_shuffle, random_seed],
            reduce_args=[random_shuffle, random_seed],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        block_udf: Optional[Callable[[Block], Iterable[Block]]],
        random_shuffle: bool,
        random_seed: Optional[int],
    ) -> List[Union[BlockMetadata, Block]]:
        stats = BlockExecStats.builder()
        if block_udf:
            ctx = TaskContext(task_idx=idx)
            # TODO(ekl) note that this effectively disables block splitting.
            blocks = list(block_udf([block], ctx))
            if len(blocks) > 1:
                builder = BlockAccessor.for_block(blocks[0]).builder()
                for b in blocks:
                    builder.add_block(b)
                block = builder.build()
            else:
                block = blocks[0]
        block = BlockAccessor.for_block(block)

        # Randomize the distribution of records to blocks.
        if random_shuffle:
            seed_i = random_seed + idx if random_seed is not None else None
            block = block.random_shuffle(seed_i)
            block = BlockAccessor.for_block(block)

        slice_sz = max(1, math.ceil(block.num_rows() / output_num_blocks))
        slices = []
        for i in range(output_num_blocks):
            slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz))

        # Randomize the distribution order of the blocks (this prevents empty
        # outputs when input blocks are very small).
        if random_shuffle:
            random = np.random.RandomState(seed_i)
            random.shuffle(slices)

        num_rows = sum(BlockAccessor.for_block(s).num_rows() for s in slices)
        assert num_rows == block.num_rows(), (num_rows, block.num_rows())
        metadata = block.get_metadata(input_files=None, exec_stats=stats.build())
        return slices + [metadata]

    @staticmethod
    def reduce(
        random_shuffle: bool,
        random_seed: Optional[int],
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> (Block, BlockMetadata):
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
            schema=accessor.schema(),
            input_files=None,
            exec_stats=stats.build(),
        )
        return new_block, new_metadata


class SimpleShufflePartitionOp(_ShufflePartitionOp, SimpleShufflePlan):
    pass


class PushBasedShufflePartitionOp(_ShufflePartitionOp, PushBasedShufflePlan):
    pass
