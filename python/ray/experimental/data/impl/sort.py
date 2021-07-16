from typing import List, Any, Callable, Iterator, Iterable, Generic, TypeVar, \
    Dict, Optional, Tuple, Union, TYPE_CHECKING

import numpy as np
import ray
from ray.experimental.data.block import Block, BlockMetadata
from ray.experimental.data.impl.arrow_block import ArrowBlock
from ray.experimental.data.impl.block_builder import SimpleBlock, SimpleBlockBuilder
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.compute import ComputeStrategy, get_compute

T = TypeVar("T")

SortKeyT = Union[None, str, List[str], Callable[[T], Any]]


def sample_boundaries(blocks: BlockList[T], num_reducers: int) -> List[T]:
    n_samples = int(num_reducers * 10 / len(blocks))

    @ray.remote
    def sample_simple_block(block: Block[T]) -> Tuple[T, T, List[T]]:
        items = block._items
        min_item = min(block._items)
        max_item = max(block._items)
        k = min(n_samples, len(items))
        samples = np.random.choice(items, k, replace=False)
        return (min_item, max_item, samples)

    samples = ray.get([sample_simple_block.remote(block) for block in blocks])
    min_item = min(m for m, _, _ in samples)
    max_item = max(m for _, m, _ in samples)
    sample_items = np.concatenate([s for _, _, s in samples] +
                                  [[min_item, max_item]])
    sample_items.sort()
    ret = [
        np.quantile(sample_items, q, interpolation="nearest")
        for q in np.arange(0, 1, 1 / num_reducers)
    ]
    return ret


def sort_simple_block(block: Block[T], boundaries: List[T],
                      key: SortKeyT) -> List[Block[T]]:
    items = block._items
    items = sorted(items, key=key)
    if len(boundaries) == 0:
        return SimpleBlock(items)
    parts = []
    bound_i = 0
    i = 0
    prev_i = 0
    part_offset = None
    N = len(items)
    while i < N and bound_i < len(boundaries):
        bound = boundaries[bound_i]
        while i < N and items[i] < bound:
            i += 1
        if part_offset is not None:
            parts.append((part_offset, i - prev_i))
        part_offset = i
        bound_i += 1
        prev_i = i
    if part_offset is not None:
        parts.append((part_offset, N - prev_i))
    ret = [
        SimpleBlock(items[offset:offset + count]) for offset, count in parts
    ]
    num_empty = len(boundaries) - len(ret)
    ret.extend([SimpleBlock([])] * num_empty)
    return ret


def merge_simple_blocks(blocks: List[Block[T]], key=SortKeyT) -> Block[T]:
    ret = [x for block in blocks for x in block._items]
    ret.sort(key=key)
    ret_block = SimpleBlock(ret)
    return ret_block, ret_block.get_metadata(None)


def sort_impl(blocks: BlockList[T],
              key: SortKeyT,
              descending: bool = False) -> BlockList[T]:
    num_mappers = len(blocks)
    num_reducers = num_mappers

    @ray.remote(num_returns=num_reducers)
    def sort_block(block, boundaries):
        return sort_simple_block(block, boundaries, key)

    @ray.remote(num_returns=2)
    def merge_sorted_blocks(*blocks: List[Block[T]]) -> Block[T]:
        return merge_simple_blocks(blocks, key)

    boundaries = sample_boundaries(blocks, len(blocks))
    map_results = np.empty((num_mappers, num_reducers), dtype=object)
    for i, block in enumerate(blocks):
        map_results[i, :] = sort_block.remote(block, boundaries)

    reduce_results = []
    for j in range(num_reducers):
        ret = merge_sorted_blocks.remote(*map_results[:, j].tolist())
        reduce_results.append(ret)

    blocks = [b for b, _ in reduce_results]
    metadata = ray.get([m for _, m in reduce_results])
    return BlockList(blocks, metadata)
