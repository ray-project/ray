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
from ray.experimental.data.block import Block, BlockAccessor
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.progress_bar import ProgressBar

T = TypeVar("T")

# Data can be sorted by value (None), a list of columns and
# ascending/descending orders (List), or a custom transform function
# (Callable).
SortKeyT = Union[None, List[Tuple[str, str]], Callable[[T], Any]]


def sample_boundaries(blocks: BlockList[T], key: SortKeyT,
                      num_reducers: int) -> List[T]:
    """
    Return (num_reducers - 1) items in ascending order from the blocks that
    partition the domain into ranges with approximately equally many elements.
    """
    n_samples = int(num_reducers * 10 / len(blocks))

    @ray.remote
    def sample_block(block: Block[T]) -> np.ndarray:
        return BlockAccessor.for_block(block).sample(n_samples, key)

    sample_results = [sample_block.remote(block) for block in blocks]
    sample_bar = ProgressBar("Sort Sample", len(sample_results))
    sample_bar.block_until_complete(sample_results)
    sample_bar.close()

    samples = ray.get(sample_results)
    sample_items = np.concatenate(samples)
    sample_items.sort()
    ret = [
        np.quantile(sample_items, q, interpolation="nearest")
        for q in np.arange(0, 1, 1 / num_reducers)
    ]
    return ret[1:]


def sort_impl(blocks: BlockList[T], key: SortKeyT,
              descending: bool = False) -> BlockList[T]:
    if len(blocks) == 0:
        return BlockList([], [])

    if isinstance(key, str):
        key = [(key, "descending" if descending else "ascending")]

    if isinstance(key, list):
        descending = key[0][1] == "descending"

    num_mappers = len(blocks)
    num_reducers = num_mappers
    boundaries = sample_boundaries(blocks, key, num_reducers)
    if descending:
        boundaries.reverse()

    @ray.remote(num_returns=num_reducers)
    def sort_block(block, boundaries):
        return BlockAccessor.for_block(block).sort_and_partition(
            boundaries, key, descending)

    @ray.remote(num_returns=2)
    def merge_sorted_blocks(*blocks: List[Block[T]]) -> Block[T]:
        if len(blocks) == 1:
            blocks = blocks[0]  # Python weirdness
        return BlockAccessor.for_block(blocks[0]).merge_sorted_blocks(
            list(blocks), key, descending)

    map_results = np.empty((num_mappers, num_reducers), dtype=object)
    for i, block in enumerate(blocks):
        map_results[i, :] = sort_block.remote(block, boundaries)
    map_bar = ProgressBar("Sort Map", len(map_results))
    map_bar.block_until_complete([ret[0] for ret in map_results])
    map_bar.close()

    reduce_results = []
    for j in range(num_reducers):
        ret = merge_sorted_blocks.remote(*map_results[:, j].tolist())
        reduce_results.append(ret)
    merge_bar = ProgressBar("Sort Merge", len(reduce_results))
    merge_bar.block_until_complete([ret[0] for ret in reduce_results])
    merge_bar.close()

    blocks = [b for b, _ in reduce_results]
    metadata = ray.get([m for _, m in reduce_results])
    return BlockList(blocks, metadata)
