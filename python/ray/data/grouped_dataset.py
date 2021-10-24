from typing import Any, Union, Callable, Generic, Tuple, List
import numpy as np
import ray
from ray.util.annotations import PublicAPI
from ray.data.dataset import Dataset
from ray.data.impl import sort
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.progress_bar import ProgressBar
from ray.data.block import Block, BlockAccessor, BlockMetadata, \
    T, U, KeyType, AggType

GroupKeyT = Union[Callable[[T], KeyType], str]


@PublicAPI(stability="beta")
class Aggregator(object):
    def __init__(self,
                 init: Callable[[KeyType], AggType],
                 accumulate: Callable[[AggType, T], AggType],
                 merge: Callable[[AggType, AggType], AggType],
                 finalize: Callable[[AggType], U] = lambda a: a,
                 name: Union[None, str] = None):
        """
        Args:
            init: This is called once for each key
                to return the empty accumulator.
                For example, an empty accumulator for a sum would be 0.
            accumulate: This is called once per row of the same key.
                This combines the accumulator and the row,
                returns the updated accumulator.
            merge: This may be called multiple times, each time to merge
                two accumulators into one.
            finalize: This is called once to compute the final aggregation
                result from the fully merged accumulator.
        """
        self.init = init
        self.accumulate = accumulate
        self.merge = merge
        self.finalize = finalize
        self.name = name


@PublicAPI(stability="beta")
class GroupedDataset(Generic[T]):
    """Implements a lazy dataset grouped by key (Experimental).

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(self, dataset: Dataset[T], key: GroupKeyT):
        """Construct a dataset grouped by key (internal API).

        The constructor is not part of the GroupedDataset API.
        Use the ``Dataset.groupby()`` method to construct one.
        """
        self._dataset = dataset
        self._key = key

    def aggregate(self, agg: Aggregator) -> Dataset[U]:
        """Implements the accumulator-based aggregation.

        This is a blocking operation.
        See https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
        for more details about accumulator-based aggregation.

        Examples:
            >>> grouped_ds.aggregate(Aggregator(
            ...     init=lambda k: [],
            ...     accumulate=lambda a, r: a.append(r),
            ...     merge=lambda a1, a2: a1 + a2,
            ...     finalize=lambda a: a
            ... ))

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the corresponding aggregation result.
        """
        # Handle empty dataset.
        if self._dataset.num_blocks() == 0:
            return self._dataset

        num_mappers = len(self._dataset._blocks)
        num_reducers = num_mappers
        boundaries = sort.sample_boundaries(
            self._dataset._blocks, [(self._key, "ascending")]
            if isinstance(self._key, str) else self._key, num_reducers)

        partition_and_combine_block = cached_remote_fn(
            _partition_and_combine_block).options(num_returns=num_reducers)
        aggregate_combined_blocks = cached_remote_fn(
            _aggregate_combined_blocks, num_returns=2)

        map_results = np.empty((num_mappers, num_reducers), dtype=object)
        for i, block in enumerate(self._dataset._blocks):
            map_results[i, :] = partition_and_combine_block.remote(
                block, boundaries, self._key, agg)
        map_bar = ProgressBar("GroupBy Map", len(map_results))
        map_bar.block_until_complete([ret[0] for ret in map_results])
        map_bar.close()

        reduce_results = []
        for j in range(num_reducers):
            ret = aggregate_combined_blocks.remote(agg,
                                                   *map_results[:, j].tolist())
            reduce_results.append(ret)
        reduce_bar = ProgressBar("GroupBy Reduce", len(reduce_results))
        reduce_bar.block_until_complete([ret[0] for ret in reduce_results])
        reduce_bar.close()

        blocks = [b for b, _ in reduce_results]
        metadata = ray.get([m for _, m in reduce_results])
        return Dataset(BlockList(blocks, metadata), self._dataset._epoch)

    def count(self) -> Dataset[U]:
        """Compute count of each group.

        This is a blocking operation.

        Example:
            >>> ray.data.range(100).groupby(lambda x: x % 3).count()

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the number of rows with that key.
        """
        return self.aggregate(
            Aggregator(
                init=lambda k: 0,
                accumulate=lambda a, r: a + 1,
                merge=lambda a1, a2: a1 + a2,
                name="count()"))

    def sum(self,
            on: Union[Callable[[T], Any], str] = lambda r: r) -> Dataset[U]:
        """Compute sum of each group.

        This is a blocking operation.

        Example:
            >>> ray.data.range(100).groupby(lambda x: x % 3).sum()

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the sum of the group.
        """
        on_fn = on
        if isinstance(on, str):
            # TODO(jjyao) Check to make sure it's arrow dataset
            on_fn = lambda r: r[on]
        return self.aggregate(
            Aggregator(
                init=lambda k: 0,
                accumulate=lambda a, r: a + on_fn(r),
                merge=lambda a1, a2: a1 + a2,
                name=(f"sum({on})" if isinstance(on, str) else None)))

    def min(self,
            on: Union[Callable[[T], Any], str] = lambda r: r) -> Dataset[U]:
        """Compute min of each group.

        This is a blocking operation.

        Example:
            >>> ray.data.range(100).groupby(lambda x: x % 3).min()

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the min of the group.
        """
        on_fn = on
        if isinstance(on, str):
            # TODO(jjyao) Check to make sure it's arrow dataset
            on_fn = lambda r: r[on]
        return self.aggregate(
            Aggregator(
                init=lambda k: None,
                accumulate=
                lambda a, r: on_fn(r) if a is None else min(a, on_fn(r)),
                merge=lambda a1, a2: min(a1, a2),
                name=(f"min({on})" if isinstance(on, str) else None)))

    def max(self,
            on: Union[Callable[[T], Any], str] = lambda r: r) -> Dataset[U]:
        """Compute max of each group.

        This is a blocking operation.

        Example:
            >>> ray.data.range(100).groupby(lambda x: x % 3).max()

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the max of the group.
        """
        on_fn = on
        if isinstance(on, str):
            # TODO(jjyao) Check to make sure it's arrow dataset
            on_fn = lambda r: r[on]
        return self.aggregate(
            Aggregator(
                init=lambda k: None,
                accumulate=
                lambda a, r: on_fn(r) if a is None else max(a, on_fn(r)),
                merge=lambda a1, a2: max(a1, a2),
                name=(f"max({on})" if isinstance(on, str) else None)))

    def mean(self,
             on: Union[Callable[[T], Any], str] = lambda r: r) -> Dataset[U]:
        """Compute mean of each group.

        This is a blocking operation.

        Example:
            >>> ray.data.range(100).groupby(lambda x: x % 3).mean()

        Returns:
            A new dataset of (k, v) pairs where k is the groupby key
            and v is the mean of the group.
        """
        on_fn = on
        if isinstance(on, str):
            # TODO(jjyao) Check to make sure it's arrow dataset
            on_fn = lambda r: r[on]
        return self.aggregate(
            Aggregator(
                init=lambda k: [0, 0],
                accumulate=lambda a, r: [a[0] + on_fn(r), a[1] + 1],
                merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
                finalize=lambda a: a[0] / a[1],
                name=(f"mean({on})" if isinstance(on, str) else None)))


def _partition_and_combine_block(
        block: Block[T], boundaries: List[KeyType], key: GroupKeyT,
        agg: Aggregator) -> List[Block[Tuple[KeyType, AggType]]]:
    """Partition the block and combine rows with the same key."""
    partitions = BlockAccessor.for_block(block).sort_and_partition(
        boundaries, [(key, "ascending")] if isinstance(key, str) else key,
        descending=False)
    return [BlockAccessor.for_block(p).combine(key, agg) for p in partitions]


def _aggregate_combined_blocks(agg: Aggregator, *blocks: Tuple[Block[U], ...]
                               ) -> Tuple[Block[U], BlockMetadata]:
    """Aggregate sorted and partially combined blocks."""
    if len(blocks) == 1:
        blocks = blocks[0]  # Ray weirdness
    return BlockAccessor.for_block(blocks[0]).aggregate_combined_blocks(
        list(blocks), agg)
