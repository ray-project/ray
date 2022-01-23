from typing import Union, Generic, Tuple, List
import numpy as np
import ray
from ray.util.annotations import PublicAPI
from ray.data.dataset import Dataset
from ray.data.impl import sort
from ray.data.aggregate import AggregateFn, Count, Sum, Max, Min, \
    Mean, Std
from ray.data.block import BlockExecStats, KeyFn
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.progress_bar import ProgressBar
from ray.data.block import Block, BlockAccessor, BlockMetadata, \
    T, U, KeyType


@PublicAPI(stability="beta")
class GroupedDataset(Generic[T]):
    """Represents a grouped dataset created by calling ``Dataset.groupby()``.

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(self, dataset: Dataset[T], key: KeyFn):
        """Construct a dataset grouped by key (internal API).

        The constructor is not part of the GroupedDataset API.
        Use the ``Dataset.groupby()`` method to construct one.
        """
        self._dataset = dataset
        self._key = key

    def aggregate(self, *aggs: AggregateFn) -> Dataset[U]:
        """Implements an accumulator-based aggregation.

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
            If the input dataset is simple dataset then the output is a simple
            dataset of ``(k, v_1, ..., v_n)`` tuples where ``k`` is the groupby
            key and ``v_i`` is the result of the ith given aggregation.
            If the input dataset is an Arrow dataset then the output is an
            Arrow dataset of ``n + 1`` columns where the first column is the
            groupby key and the second through ``n + 1`` columns are the
            results of the aggregations.
            If groupby key is ``None`` then the key part of return is omitted.
        """

        stats = self._dataset._stats.child_builder("aggregate")
        stage_info = {}
        if len(aggs) == 0:
            raise ValueError("Aggregate requires at least one aggregation")
        for agg in aggs:
            agg._validate(self._dataset)
        # Handle empty dataset.
        if self._dataset.num_blocks() == 0:
            return self._dataset

        blocks = self._dataset._blocks.get_blocks()
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
            _partition_and_combine_block).options(num_returns=num_reducers + 1)
        aggregate_combined_blocks = cached_remote_fn(
            _aggregate_combined_blocks, num_returns=2)

        map_results = np.empty((num_mappers, num_reducers), dtype=object)
        map_meta = []
        for i, block in enumerate(blocks):
            results = partition_and_combine_block.remote(
                block, boundaries, self._key, aggs)
            map_results[i, :] = results[:-1]
            map_meta.append(results[-1])
        map_bar = ProgressBar("GroupBy Map", len(map_results))
        map_bar.block_until_complete(map_meta)
        stage_info["map"] = ray.get(map_meta)
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
        stage_info["reduce"] = metadata
        return Dataset(
            BlockList(blocks, metadata), self._dataset._epoch,
            stats.build_multistage(stage_info))

    def _aggregate_on(self, agg_cls: type, on: Union[KeyFn, List[KeyFn]],
                      *args, **kwargs):
        """Helper for aggregating on a particular subset of the dataset.

        This validates the `on` argument, and converts a list of column names
        or lambdas to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Dataset, and a single
        aggregation on the entire row for a simple Dataset.
        """
        aggs = self._dataset._build_multicolumn_aggs(
            agg_cls, on, *args, skip_cols=self._key, **kwargs)
        return self.aggregate(*aggs)

    def count(self) -> Dataset[U]:
        """Compute count aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).count()
            >>> ray.data.from_items([
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby(
            ...     "A").count()

        Returns:
            A simple dataset of ``(k, v)`` pairs or an Arrow dataset of
            ``[k, v]`` columns where ``k`` is the groupby key and ``v`` is the
            number of rows with that key.
            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self.aggregate(Count())

    def sum(self, on: Union[KeyFn, List[KeyFn]] = None) -> Dataset[U]:
        """Compute grouped sum aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).sum()
            >>> ray.data.from_items([
            ...     (i % 3, i, i**2)
            ...     for i in range(100)]) \
            ...     .groupby(lambda x: x[0] % 3) \
            ...     .sum(lambda x: x[2])
            >>> ray.data.range_arrow(100).groupby("value").sum()
            >>> ray.data.from_items([
            ...     {"A": i % 3, "B": i, "C": i**2}
            ...     for i in range(100)]) \
            ...     .groupby("A") \
            ...     .sum(["B", "C"])

        Args:
            on: The data subset on which to compute the sum.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a sum of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise sum of all
                  columns.

        Returns:
            The sum result.

            For a simple dataset, the output is:

            - ``on=None``: a simple dataset of ``(k, sum)`` tuples where ``k``
              is the groupby key and ``sum`` is sum of all rows in that group.
            - ``on=[callable_1, ..., callable_n]``: a simple dataset of
              ``(k, sum_1, ..., sum_n)`` tuples where ``k`` is the groupby key
              and ``sum_i`` is sum of the outputs of the ith callable called on
              each row in that group.

            For an Arrow dataset, the output is:

            - ``on=None``: an Arrow dataset containing a groupby key column,
              ``"k"``, and a column-wise sum column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: an Arrow dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Sum, on)

    def min(self, on: Union[KeyFn, List[KeyFn]] = None) -> Dataset[U]:
        """Compute grouped min aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).min()
            >>> ray.data.from_items([
            ...     (i % 3, i, i**2)
            ...     for i in range(100)]) \
            ...     .groupby(lambda x: x[0] % 3) \
            ...     .min(lambda x: x[2])
            >>> ray.data.range_arrow(100).groupby("value").min()
            >>> ray.data.from_items([
            ...     {"A": i % 3, "B": i, "C": i**2}
            ...     for i in range(100)]) \
            ...     .groupby("A") \
            ...     .min(["B", "C"])

        Args:
            on: The data subset on which to compute the min.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a min of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise min of all
                  columns.

        Returns:
            The min result.

            For a simple dataset, the output is:

            - ``on=None``: a simple dataset of ``(k, min)`` tuples where ``k``
              is the groupby key and min is min of all rows in that group.
            - ``on=[callable_1, ..., callable_n]``: a simple dataset of
              ``(k, min_1, ..., min_n)`` tuples where ``k`` is the groupby key
              and ``min_i`` is min of the outputs of the ith callable called on
              each row in that group.

            For an Arrow dataset, the output is:

            - ``on=None``: an Arrow dataset containing a groupby key column,
              ``"k"``, and a column-wise min column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: an Arrow dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Min, on)

    def max(self, on: Union[KeyFn, List[KeyFn]] = None) -> Dataset[U]:
        """Compute grouped max aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).max()
            >>> ray.data.from_items([
            ...     (i % 3, i, i**2)
            ...     for i in range(100)]) \
            ...     .groupby(lambda x: x[0] % 3) \
            ...     .max(lambda x: x[2])
            >>> ray.data.range_arrow(100).groupby("value").max()
            >>> ray.data.from_items([
            ...     {"A": i % 3, "B": i, "C": i**2}
            ...     for i in range(100)]) \
            ...     .groupby("A") \
            ...     .max(["B", "C"])

        Args:
            on: The data subset on which to compute the max.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a max of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise max of all
                  columns.

        Returns:
            The max result.

            For a simple dataset, the output is:

            - ``on=None``: a simple dataset of ``(k, max)`` tuples where ``k``
              is the groupby key and ``max`` is max of all rows in that group.
            - ``on=[callable_1, ..., callable_n]``: a simple dataset of
              ``(k, max_1, ..., max_n)`` tuples where ``k`` is the groupby key
              and ``max_i`` is max of the outputs of the ith callable called on
              each row in that group.

            For an Arrow dataset, the output is:

            - ``on=None``: an Arrow dataset containing a groupby key column,
              ``"k"``, and a column-wise max column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: an Arrow dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Max, on)

    def mean(self, on: Union[KeyFn, List[KeyFn]] = None) -> Dataset[U]:
        """Compute grouped mean aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).mean()
            >>> ray.data.from_items([
            ...     (i % 3, i, i**2)
            ...     for i in range(100)]) \
            ...     .groupby(lambda x: x[0] % 3) \
            ...     .mean(lambda x: x[2])
            >>> ray.data.range_arrow(100).groupby("value").mean()
            >>> ray.data.from_items([
            ...     {"A": i % 3, "B": i, "C": i**2}
            ...     for i in range(100)]) \
            ...     .groupby("A") \
            ...     .mean(["B", "C"])

        Args:
            on: The data subset on which to compute the mean.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a mean of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise mean of all
                  columns.

        Returns:
            The mean result.

            For a simple dataset, the output is:

            - ``on=None``: a simple dataset of ``(k, mean)`` tuples where ``k``
              is the groupby key and ``mean`` is mean of all rows in that
              group.
            - ``on=[callable_1, ..., callable_n]``: a simple dataset of
              ``(k, mean_1, ..., mean_n)`` tuples where ``k`` is the groupby
              key and ``mean_i`` is mean of the outputs of the ith callable
              called on each row in that group.

            For an Arrow dataset, the output is:

            - ``on=None``: an Arrow dataset containing a groupby key column,
              ``"k"``, and a column-wise mean column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: an Arrow dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Mean, on)

    def std(self, on: Union[KeyFn, List[KeyFn]] = None,
            ddof: int = 1) -> Dataset[U]:
        """Compute grouped standard deviation aggregation.

        This is a blocking operation.

        Examples:
            >>> ray.data.range(100).groupby(lambda x: x % 3).std()
            >>> ray.data.from_items([
            ...     (i % 3, i, i**2)
            ...     for i in range(100)]) \
            ...     .groupby(lambda x: x[0] % 3) \
            ...     .std(lambda x: x[2])
            >>> ray.data.range_arrow(100).groupby("value").std(ddof=0)
            >>> ray.data.from_items([
            ...     {"A": i % 3, "B": i, "C": i**2}
            ...     for i in range(100)]) \
            ...     .groupby("A") \
            ...     .std(["B", "C"])

        NOTE: This uses Welford's online method for an accumulator-style
        computation of the standard deviation. This method was chosen due to
        it's numerical stability, and it being computable in a single pass.
        This may give different (but more accurate) results than NumPy, Pandas,
        and sklearn, which use a less numerically stable two-pass algorithm.
        See
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        Args:
            on: The data subset on which to compute the std.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a std of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise std of all
                  columns.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is ``N - ddof``, where ``N`` represents the number of elements.

        Returns:
            The standard deviation result.

            For a simple dataset, the output is:

            - ``on=None``: a simple dataset of ``(k, std)`` tuples where ``k``
              is the groupby key and ``std`` is std of all rows in that group.
            - ``on=[callable_1, ..., callable_n]``: a simple dataset of
              ``(k, std_1, ..., std_n)`` tuples where ``k`` is the groupby key
              and ``std_i`` is std of the outputs of the ith callable called on
              each row in that group.

            For an Arrow dataset, the output is:

            - ``on=None``: an Arrow dataset containing a groupby key column,
              ``"k"``, and a column-wise std column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: an Arrow dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Std, on, ddof=ddof)


def _partition_and_combine_block(
        block: Block[T], boundaries: List[KeyType], key: KeyFn,
        aggs: Tuple[AggregateFn]) -> List[Union[Block, BlockMetadata]]:
    """Partition the block and combine rows with the same key."""
    stats = BlockExecStats.builder()
    if key is None:
        partitions = [block]
    else:
        partitions = BlockAccessor.for_block(block).sort_and_partition(
            boundaries, [(key, "ascending")] if isinstance(key, str) else key,
            descending=False)
    parts = [BlockAccessor.for_block(p).combine(key, aggs) for p in partitions]
    meta = BlockAccessor.for_block(block).get_metadata(
        input_files=None, exec_stats=stats.build())
    return parts + [meta]


def _aggregate_combined_blocks(
        num_reducers: int, key: KeyFn, aggs: Tuple[AggregateFn],
        *blocks: Tuple[Block, ...]) -> Tuple[Block[U], BlockMetadata]:
    """Aggregate sorted and partially combined blocks."""
    return BlockAccessor.for_block(blocks[0]).aggregate_combined_blocks(
        list(blocks), key, aggs)
