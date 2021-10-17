from typing import Any, Callable, Generic
import numpy as np
import ray
from ray.util.annotations import PublicAPI
from ray.data.dataset import Dataset, T
from ray.data.impl import sort
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.progress_bar import ProgressBar
from ray.data.block import BlockAccessor

@PublicAPI(stability="beta")
class GroupedDataset(Generic[T]):
    def __init__(self,
                 dataset : Dataset[T],
                 key: Callable[[T], Any]):
        self._dataset = dataset
        self._key = key

    def aggregate(self, agg_func):
        # Handle empty dataset.
        if self._dataset.num_blocks() == 0:
            return self._dataset

        num_mappers = len(self._dataset._blocks)
        num_reducers = num_mappers
        boundaries = sort.sample_boundaries(self._dataset._blocks, self._key, num_reducers)

        sort_and_partition_block = cached_remote_fn(
            _sort_and_partition_block, num_returns=num_reducers)
        group_and_aggregate_sorted_blocks = cached_remote_fn(
            _group_and_aggregate_sorted_blocks, num_returns=2)

        map_results = np.empty((num_mappers, num_reducers), dtype=object)
        for i, block in enumerate(self._dataset._blocks):
            map_results[i, :] = sort_and_partition_block.remote(
                block, boundaries, self._key)
        map_bar = ProgressBar("GroupBy Map", len(map_results))
        map_bar.block_until_complete([ret[0] for ret in map_results])
        map_bar.close()

        reduce_results = []
        for j in range(num_reducers):
            ret = group_and_aggregate_sorted_blocks.remote(self._key, agg_func,
                                            *map_results[:, j].tolist())
            reduce_results.append(ret)
        reduce_bar = ProgressBar("GroupBy Reduce", len(reduce_results))
        reduce_bar.block_until_complete([ret[0] for ret in reduce_results])
        reduce_bar.close()

        blocks = [b for b, _ in reduce_results]
        metadata = ray.get([m for _, m in reduce_results])
        return Dataset(BlockList(blocks, metadata), self._dataset._epoch)

def _sort_and_partition_block(block, boundaries, key):
    return BlockAccessor.for_block(block).sort_and_partition(
        boundaries, key, descending=False)

def _group_and_aggregate_sorted_blocks(key, agg_func, *blocks):
    if len(blocks) == 1:
        blocks = blocks[0]  # Python weirdness
    return BlockAccessor.for_block(blocks[0]).group_and_aggregate_sorted_blocks(
        list(blocks), key, agg_func)