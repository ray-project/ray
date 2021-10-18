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
    def __init__(self, dataset: Dataset[T], key: Callable[[T], Any]):
        self._dataset = dataset
        self._key = key

    def aggregate(self, init, accumulate, merge, finalize):
        """ Implement the accumulator-based aggregation interface
            (https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf)
            Current implementation is based on sort but can be changed to hash based
            if needed without modifying the API.

            init: This is called once for each key to return the initialized aggregation state.
            accumulate: This is called for each row of the same key.
                        This causes the row to be accumulated into the aggregation state.
            merge: This may be called multiple times, each time to combine two partial aggregation states.
            finalize: This is called once to compute the final aggregation result
                      from the fully merged aggregation state.
        """
        # Handle empty dataset.
        if self._dataset.num_blocks() == 0:
            return self._dataset

        num_mappers = len(self._dataset._blocks)
        num_reducers = num_mappers
        boundaries = sort.sample_boundaries(self._dataset._blocks, self._key,
                                            num_reducers)

        partition_and_combine_block = cached_remote_fn(
            _partition_and_combine_block, num_returns=num_reducers)
        aggregate_combined_blocks = cached_remote_fn(
            _aggregate_combined_blocks, num_returns=2)

        map_results = np.empty((num_mappers, num_reducers), dtype=object)
        for i, block in enumerate(self._dataset._blocks):
            map_results[i, :] = partition_and_combine_block.remote(
                block, boundaries, self._key, init, accumulate)
        map_bar = ProgressBar("GroupBy Map", len(map_results))
        map_bar.block_until_complete([ret[0] for ret in map_results])
        map_bar.close()

        reduce_results = []
        for j in range(num_reducers):
            ret = aggregate_combined_blocks.remote(merge, finalize,
                                                   *map_results[:, j].tolist())
            reduce_results.append(ret)
        reduce_bar = ProgressBar("GroupBy Reduce", len(reduce_results))
        reduce_bar.block_until_complete([ret[0] for ret in reduce_results])
        reduce_bar.close()

        blocks = [b for b, _ in reduce_results]
        metadata = ray.get([m for _, m in reduce_results])
        return Dataset(BlockList(blocks, metadata), self._dataset._epoch)


def _partition_and_combine_block(block, boundaries, key, init, accumulate):
    partitions = BlockAccessor.for_block(block).sort_and_partition(
        boundaries, key, descending=False)
    return [
        BlockAccessor.for_block(p).combine(key, init, accumulate)
        for p in partitions
    ]


def _aggregate_combined_blocks(merge, finalize, *blocks):
    if len(blocks) == 1:
        blocks = blocks[0]  # Python weirdness
    return BlockAccessor.for_block(blocks[0]).aggregate_combined_blocks(
        list(blocks), merge, finalize)
