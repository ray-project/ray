"""
TODO: document the algorithm
"""
from typing import List, Any, Callable, TypeVar, Tuple, Union

import numpy as np
import pyarrow as pa
import ray
from ray.experimental.data.block import Block, BlockAccessor
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.progress_bar import ProgressBar

T = TypeVar("T")

# Data can be sorted by value (None), a column (str), multiple columns
# (List[str]) or a custom transform function (lambda).
SortKeyT = Union[None, str, List[str], Callable[[T], Any]]


def sample_boundaries(blocks: BlockList[T], key: SortKeyT,
                      num_reducers: int) -> List[T]:
    """return len(boundaries) == num_reducers - 1"""
    n_samples = int(num_reducers * 10 / len(blocks))

    def _sample_simple_block(items: List[T]) -> np.ndarray:
        k = min(n_samples, len(items))
        samples = np.random.choice(items, k, replace=False)
        vf = np.vectorize(lambda x: key(x) if key else x)
        return vf(samples)

    # TODO: type this
    def _sample_arrow_block(table: pa.Table):
        k = min(n_samples, table.num_rows)
        indices = np.random.choice(table.num_rows, k, replace=False)
        # TODO: handle multi column
        return table[key[0][0]].take(indices)

    @ray.remote
    def sample_block(block: Block[T]) -> np.ndarray:
        if isinstance(block, list):
            return _sample_simple_block(block)
        else:
            return _sample_arrow_block(block)

    sample_results = [sample_block.remote(block) for block in blocks]
    sample_bar = ProgressBar("Sort Sample", len(sample_results))
    sample_bar.block_until_complete(sample_results)
    sample_bar.close()

    samples = ray.get(sample_results)
    sample_items = np.concatenate(samples)
    sample_items.sort()
    print(sample_items)
    ret = [
        np.quantile(sample_items, q, interpolation="nearest")
        for q in np.arange(0, 1, 1 / num_reducers)
    ]
    return ret[1:]


def sort_impl(blocks: BlockList[T], key: SortKeyT,
              descending: bool = False) -> BlockList[T]:
    if len(blocks) == 0:
        return BlockList([], [])

    # first_block = next(iter(blocks))
    # if isinstance(first_block, list):
    #     if key is not None and not callable(key):
    #         raise TypeError(
    #             f"sort key must be callable for SimpleBlock, got: {key}")
    # elif isinstance(first_block, pa.Table):
    #     # TODO: no support for multi column sort yet
    #     if isinstance(key, str):
    #         key = [(key, "descending" if descending else "ascending")]
    #     if not isinstance(key, list):
    #         raise TypeError(
    #             f"sort key must be a list of (key, ascending) pairs for ArrowBlock, got: {key}"
    #         )
    if isinstance(key, str):
        key = [(key, "descending" if descending else "ascending")]

    num_mappers = len(blocks)
    num_reducers = num_mappers
    boundaries = sample_boundaries(blocks, key, num_reducers)
    print("boundaries", boundaries)

    @ray.remote(num_returns=num_reducers)
    def sort_block(block, boundaries):
        return BlockAccessor.for_block(block).sort_and_partition(
            boundaries, key)

    @ray.remote(num_returns=2)
    def merge_sorted_blocks(*blocks: List[Block[T]]) -> Block[T]:
        if len(blocks) == 1:
            blocks = blocks[0]  # Python weirdness
        return BlockAccessor.for_block(blocks[0]).merge_sorted_blocks(
            list(blocks), key)

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
