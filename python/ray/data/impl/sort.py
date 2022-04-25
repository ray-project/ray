"""
We implement a distributed sorting algorithm similar to
[External Merge Sort](https://en.wikipedia.org/wiki/External_sorting).
Sorting is done in 3 stages: sampling, sorting individual blocks, and
merging sorted blocks.

Sampling: we get a number of sample items from each block, sort them, and
use them to compute boundaries that would partition all items into
approximately equal ranges.

Sorting: each block is sorted locally, then partitioned into smaller blocks
according to the boundaries. Each partitioned block is passed to a merge task.
This is an all-to-all shuffle.

Merging: a merge task would receive a block from every worker that consists
of items in a certain range. It then merges the sorted blocks into one sorted
block and becomes part of the new, sorted dataset.
"""
from typing import List, Any, Callable, TypeVar, Tuple, Union

import numpy as np
import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockAccessor, BlockExecStats
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.shuffle import ShuffleOp

T = TypeVar("T")

# Data can be sorted by value (None), a list of columns and
# ascending/descending orders (List), or a custom transform function
# (Callable).
SortKeyT = Union[None, List[Tuple[str, str]], Callable[[T], Any]]


class SortOp(ShuffleOp):
    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[T],
        key: SortKeyT,
        descending: bool,
    ) -> List[Union[BlockMetadata, Block]]:
        stats = BlockExecStats.builder()
        out = BlockAccessor.for_block(block).sort_and_partition(
            boundaries, key, descending
        )
        meta = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        )
        return [meta] + out

    @staticmethod
    def reduce(
        key: SortKeyT, descending: bool, *mapper_outputs: List[Block]
    ) -> (Block, BlockMetadata):
        return BlockAccessor.for_block(mapper_outputs[0]).merge_sorted_blocks(
            mapper_outputs, key, descending
        )


def sample_boundaries(
    blocks: List[ObjectRef[Block]], key: SortKeyT, num_reducers: int
) -> List[T]:
    """
    Return (num_reducers - 1) items in ascending order from the blocks that
    partition the domain into ranges with approximately equally many elements.
    """
    # TODO(Clark): Support multiple boundary sampling keys.
    if isinstance(key, list) and len(key) > 1:
        raise ValueError("Multiple boundary sampling keys not supported.")

    n_samples = int(num_reducers * 10 / len(blocks))

    sample_block = cached_remote_fn(_sample_block)

    sample_results = [sample_block.remote(block, n_samples, key) for block in blocks]
    sample_bar = ProgressBar("Sort Sample", len(sample_results))
    sample_bar.block_until_complete(sample_results)
    sample_bar.close()

    samples = ray.get(sample_results)
    samples = [s for s in samples if len(s) > 0]
    # The dataset is empty
    if len(samples) == 0:
        return [None] * (num_reducers - 1)
    builder = DelegatingBlockBuilder()
    for sample in samples:
        builder.add_block(sample)
    samples = builder.build()
    column = key[0][0] if isinstance(key, list) else None
    sample_items = BlockAccessor.for_block(samples).to_numpy(column)
    sample_items = np.sort(sample_items)
    ret = [
        np.quantile(sample_items, q, interpolation="nearest")
        for q in np.linspace(0, 1, num_reducers)
    ]
    return ret[1:]


# Note: currently the map_groups() API relies on this implementation
# to partition the same key into the same block.
def sort_impl(
    blocks: BlockList, clear_input_blocks: bool, key: SortKeyT, descending: bool = False
) -> Tuple[BlockList, dict]:
    stage_info = {}
    blocks_list = blocks.get_blocks()
    if len(blocks_list) == 0:
        return BlockList([], []), stage_info

    if isinstance(key, str):
        key = [(key, "descending" if descending else "ascending")]

    if isinstance(key, list):
        descending = key[0][1] == "descending"

    num_mappers = len(blocks_list)
    # Use same number of output partitions.
    num_reducers = num_mappers
    # TODO(swang): sample_boundaries could be fused with a previous stage.
    boundaries = sample_boundaries(blocks_list, key, num_reducers)
    if descending:
        boundaries.reverse()

    shuffle_op = SortOp(
        map_args=[boundaries, key, descending], reduce_args=[key, descending]
    )
    return shuffle_op.execute(
        blocks,
        num_reducers,
        clear_input_blocks,
    )


def _sample_block(block: Block[T], n_samples: int, key: SortKeyT) -> Block[T]:
    return BlockAccessor.for_block(block).sample(n_samples, key)
