import logging
import math
from typing import Callable, Iterable, List, Optional, Tuple, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR

logger = logging.getLogger(__name__)


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
        upstream_map_fn: Optional[Callable[[Iterable[Block]], Iterable[Block]]] = None,
    ):
        super().__init__(
            map_args=[
                target_shuffle_max_block_size,
                upstream_map_fn,
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
        upstream_map_fn: Optional[Callable[[Iterable[Block]], Iterable[Block]]],
        random_shuffle: bool,
        random_seed: Optional[int],
    ) -> List[Union[BlockMetadata, Block]]:
        stats = BlockExecStats.builder()
        if upstream_map_fn:
            # TODO: Support dynamic block splitting in
            # all-to-all ops, to avoid having to re-fuse
            # upstream blocks together.
            upstream_map_iter = upstream_map_fn([block])
            mapped_block = next(upstream_map_iter)
            builder = BlockAccessor.for_block(mapped_block).builder()
            builder.add_block(mapped_block)
            for mapped_block in upstream_map_iter:
                builder.add_block(mapped_block)
            # Drop the upstream inputs to reduce memory usage.
            del mapped_block
            block = builder.build()
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

        # Randomize the distribution of records to blocks.
        if random_shuffle:
            seed_i = random_seed + idx if random_seed is not None else None
            block = block.random_shuffle(seed_i)
            block = BlockAccessor.for_block(block)

        # Build a list of slices to return. It's okay to put the results in a
        # list instead of yielding them as a generator because slicing the
        # ArrowBlock is zero-copy.
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
    ) -> Tuple[Block, BlockMetadata]:
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
            schema=accessor.schema(),
            input_files=None,
            exec_stats=stats.build(),
        )
        return new_block, new_metadata
