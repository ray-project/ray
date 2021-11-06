from typing import Union, Callable, Generic, Tuple, List
import numpy as np
import ray
from ray.util.annotations import PublicAPI
from ray.data.dataset import Dataset
from ray.data.impl import sort
from ray.data.aggregate import AggregateFn, Count, Sum, Max, Min, \
    Mean, Std, AggregateOnT
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.progress_bar import ProgressBar
from ray.data.block import Block, BlockAccessor, BlockMetadata, \
    T, U, KeyType

GroupKeyT = Union[None, Callable[[T], KeyType], str, List[str]]


@PublicAPI(stability="beta")
class GroupedDataset(Generic[T]):
    """Represents a grouped dataset created by calling ``Dataset.groupby()``.

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(self, dataset: Dataset[T], key: GroupKeyT):
        """Construct a dataset grouped by key (internal API).

        The constructor is not part of the GroupedDataset API.
        Use the ``Dataset.groupby()`` method to construct one.
        """
        self._dataset = dataset

        if isinstance(key, list):
            if len(key) > 1:
                # TODO(jjyao) Support multi-key groupby.
                raise NotImplementedError(
                    "Multi-key groupby is not supported yet")
            else:
                self._key = key[0]
        else:
            self._key = key

    def aggregate(self, *aggs: AggregateFn) -> Dataset[U]:
        """Implements the accumulator-based aggregation.

        This is a blocking operation.

        Examples:
            >>> grouped_ds.aggregate(AggregateFn(
            ...     init=lambda k: [],
            ...     accumulate=lambda a, r: a + [r],
            ...     merge=lambda a1, a2: a1 + a2,
            ...     finalize=lambda a: a
            ... ))

        Args:
            aggs: Aggregations to do.

        Returns:
            If the input dataset is simple dataset then the output is
            a simple dataset of (k, v_1, ..., v_n) tuples where k is the
            groupby key and v_i is the result of the ith given aggregation.
            If the input dataset is Arrow dataset then the output is
            an Arrow dataset of n + 1 columns where first column is
            the groupby key and the second through n + 1 columns are the
            results of the aggregations.
            If groupby key is None then the key part of return is omitted.
        """

        if len(aggs) == 0:
            raise ValueError("Aggregate requires at least one aggregation")
        # Handle empty dataset.
        if self._dataset.num_blocks() == 0:
            return self._dataset

        blocks = list(self._dataset._blocks.iter_blocks())
        num_mappers = len(blocks)
        num_reducers = num_mappers
        if self._key is None:
            num_reducers = 1
            boundaries = []
        else:
            boundaries = sort.sample_boundaries(
                blocks, [(self._key, "ascending")]
                if isinstance(self._key, str) else self._key, num_reducers)

        partition_and_combine_block = cached_remote_fn(
            _partition_and_combine_block).options(num_returns=num_reducers)
        aggregate_combined_blocks = cached_remote_fn(
            _aggregate_combined_blocks, num_returns=2)

        map_results = np.empty((num_mappers, num_reducers), dtype=object)
        for i, block in enumerate(blocks):
            map_results[i, :] = partition_and_combine_block.remote(
                block, boundaries, self._key, aggs)
        map_bar = ProgressBar("GroupBy Map", len(map_results))
        map_bar.block_until_complete([ret[0] for ret in map_results])
        map_bar.close()

        blocks = []
        metadata = []
        for j in range(num_reducers):
            block, meta = aggregate_combined_blocks.remote(
                num_reducers, self._key, aggs, *map_results[:, j].tolist())
            blocks.append(block)
            metadata.append(meta)
        reduce_bar = ProgressBar("GroupBy Reduce", len(blocks))
        reduce_bar.block_until_complete(blocks)
        reduce_bar.close()

        metadata = ray.get(metadata)
        return Dataset(BlockList(blocks, metadata), self._dataset._epoch)

    def count(self) -> Dataset[U]:
        """Compute count aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).count()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").count()

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the number of rows with that key.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Count())

    def sum(self, on: AggregateOnT = None) -> Dataset[U]:
        """Compute sum aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).sum()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").sum("B")

        Args:
            on: The data to sum on.
                It can be the column name for Arrow dataset.

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the sum result.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Sum(on))

    def min(self, on: AggregateOnT = None) -> Dataset[U]:
        """Compute min aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).min()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").min("B")

        Args:
            on: The data to min on.
                It can be the column name for Arrow dataset.

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the min result.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Min(on))

    def max(self, on: AggregateOnT = None) -> Dataset[U]:
        """Compute max aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).max()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").max("B")

        Args:
            on: The data to max on.
                It can be the column name for Arrow dataset.

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the max result.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Max(on))

    def mean(self, on: AggregateOnT = None) -> Dataset[U]:
        """Compute mean aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).mean()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").mean("B")

        Args:
            on: The data to mean on.
                It can be the column name for Arrow dataset.

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the mean result.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Mean(on))

    def std(self, on: AggregateOnT = None, ddof: int = 1) -> Dataset[U]:
        """Compute standard deviation aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).std()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").std("B")

        NOTE: This uses Welford's online method for an accumulator-style
        computation of the standard deviation. This method was chosen due to
        it's numerical stability, and it being computable in a single pass.
        This may give different (but more accurate) results than NumPy, Pandas,
        and sklearn, which use a less numerically stable two-pass algorithm.
        See
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        Args:
            on: The data on which to compute the standard deviation.
                It can be the column name for Arrow dataset.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is N - ddof, where N represents the number of elements.

        Returns:
            A simple dataset of (k, v) pairs or
            an Arrow dataset of [k, v] columns
            where k is the groupby key and
            v is the standard deviation result.
            If groupby key is None then the key part of return is omitted.
        """
        return self.aggregate(Std(on, ddof))


def _partition_and_combine_block(block: Block[T], boundaries: List[KeyType],
                                 key: GroupKeyT,
                                 aggs: Tuple[AggregateFn]) -> List[Block]:
    """Partition the block and combine rows with the same key."""
    if key is None:
        partitions = [block]
    else:
        partitions = BlockAccessor.for_block(block).sort_and_partition(
            boundaries, [(key, "ascending")] if isinstance(key, str) else key,
            descending=False)
    return [BlockAccessor.for_block(p).combine(key, aggs) for p in partitions]


def _aggregate_combined_blocks(
        num_reducers: int, key: GroupKeyT, aggs: Tuple[AggregateFn],
        *blocks: Tuple[Block, ...]) -> Tuple[Block[U], BlockMetadata]:
    """Aggregate sorted and partially combined blocks."""
    if num_reducers == 1:
        blocks = [b[0] for b in blocks]  # Ray weirdness
    return BlockAccessor.for_block(blocks[0]).aggregate_combined_blocks(
        list(blocks), key, aggs)
