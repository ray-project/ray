from typing import Any, Union, Generic, Tuple, List, Callable
from ray.util.annotations import PublicAPI
from ray.data.dataset import Dataset
from ray.data.dataset import BatchType
from ray.data.impl import sort
from ray.data.aggregate import AggregateFn, Count, Sum, Max, Min, Mean, Std
from ray.data.block import BlockExecStats, KeyFn
from ray.data.impl.plan import AllToAllStage
from ray.data.impl.compute import CallableClass, ComputeStrategy
from ray.data.impl.shuffle import ShuffleOp, SimpleShufflePlan
from ray.data.block import Block, BlockAccessor, BlockMetadata, T, U, KeyType


class _GroupbyOp(ShuffleOp):
    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[KeyType],
        key: KeyFn,
        aggs: Tuple[AggregateFn],
    ) -> List[Union[BlockMetadata, Block]]:
        """Partition the block and combine rows with the same key."""
        stats = BlockExecStats.builder()
        if key is None:
            partitions = [block]
        else:
            partitions = BlockAccessor.for_block(block).sort_and_partition(
                boundaries,
                [(key, "ascending")] if isinstance(key, str) else key,
                descending=False,
            )
        parts = [BlockAccessor.for_block(p).combine(key, aggs) for p in partitions]
        meta = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        )
        return [meta] + parts

    @staticmethod
    def reduce(
        key: KeyFn, aggs: Tuple[AggregateFn], *mapper_outputs: List[Block]
    ) -> (Block, BlockMetadata):
        """Aggregate sorted and partially combined blocks."""
        return BlockAccessor.for_block(mapper_outputs[0]).aggregate_combined_blocks(
            list(mapper_outputs), key, aggs
        )


class SimpleShuffleGroupbyOp(_GroupbyOp, SimpleShufflePlan):
    pass


@PublicAPI
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
            >>> import ray
            >>> from ray.data.aggregate import AggregateFn
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> grouped_ds = ds.groupby(lambda x: x % 3) # doctest: +SKIP
            >>> grouped_ds.aggregate(AggregateFn( # doctest: +SKIP
            ...     init=lambda k: [], # doctest: +SKIP
            ...     accumulate=lambda a, r: a + [r], # doctest: +SKIP
            ...     merge=lambda a1, a2: a1 + a2, # doctest: +SKIP
            ...     finalize=lambda a: a # doctest: +SKIP
            ... )) # doctest: +SKIP

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

        def do_agg(blocks, clear_input_blocks: bool, *_):
            # TODO: implement clear_input_blocks
            stage_info = {}
            if len(aggs) == 0:
                raise ValueError("Aggregate requires at least one aggregation")
            for agg in aggs:
                agg._validate(self._dataset)
            # Handle empty dataset.
            if blocks.initial_num_blocks() == 0:
                return blocks, stage_info

            num_mappers = blocks.initial_num_blocks()
            num_reducers = num_mappers
            if self._key is None:
                num_reducers = 1
                boundaries = []
            else:
                boundaries = sort.sample_boundaries(
                    blocks.get_blocks(),
                    [(self._key, "ascending")]
                    if isinstance(self._key, str)
                    else self._key,
                    num_reducers,
                )
            shuffle_op = SimpleShuffleGroupbyOp(
                map_args=[boundaries, self._key, aggs], reduce_args=[self._key, aggs]
            )
            return shuffle_op.execute(
                blocks,
                num_reducers,
                clear_input_blocks,
            )

        plan = self._dataset._plan.with_stage(AllToAllStage("aggregate", None, do_agg))
        return Dataset(
            plan,
            self._dataset._epoch,
            self._dataset._lazy,
        )

    def _aggregate_on(
        self,
        agg_cls: type,
        on: Union[KeyFn, List[KeyFn]],
        ignore_nulls: bool,
        *args,
        **kwargs,
    ):
        """Helper for aggregating on a particular subset of the dataset.

        This validates the `on` argument, and converts a list of column names
        or lambdas to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Dataset, and a single
        aggregation on the entire row for a simple Dataset.
        """
        aggs = self._dataset._build_multicolumn_aggs(
            agg_cls, on, ignore_nulls, *args, skip_cols=self._key, **kwargs
        )
        return self.aggregate(*aggs)

    def map_groups(
        self,
        fn: Union[CallableClass, Callable[[BatchType], BatchType]],
        *,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: str = "native",
        **ray_remote_args,
    ) -> "Dataset[Any]":
        # TODO AttributeError: 'GroupedDataset' object has no attribute 'map_groups'
        #  in the example below.
        """Apply the given function to each group of records of this dataset.

        While map_groups() is very flexible, note that it comes with downsides:
            * It may be slower than using more specific methods such as min(), max().
            * It requires that each group fits in memory on a single node.

        In general, prefer to use aggregate() instead of map_groups().

        This is a blocking operation.

        Examples:
            >>> # Return a single record per group (list of multiple records in,
            >>> # list of a single record out). Note that median is not an
            >>> # associative function so cannot be computed with aggregate().
            >>> import ray
            >>> import pandas as pd
            >>> import numpy as np
            >>> ds = ray.data.range(100) # doctest: +SKIP
            >>> ds.groupby(lambda x: x % 3).map_groups( # doctest: +SKIP
            ...     lambda x: [np.median(x)])

            >>> # Return multiple records per group (dataframe in, dataframe out).
            >>> df = pd.DataFrame(
            ...     {"A": ["a", "a", "b"], "B": [1, 1, 3], "C": [4, 6, 5]}
            ... )
            >>> ds = ray.data.from_pandas(df) # doctest: +SKIP
            >>> grouped = ds.groupby("A") # doctest: +SKIP
            >>> grouped.map_groups( # doctest: +SKIP
            ...     lambda g: g.apply(
            ...         lambda c: c / g[c.name].sum() if c.name in ["B", "C"] else c
            ...     )
            ... ) # doctest: +SKIP

        Args:
            fn: The function to apply to each group of records, or a class type
                that can be instantiated to create such a callable. It takes as
                input a batch of all records from a single group, and returns a
                batch of zero or more records, similar to map_batches().
            compute: The compute strategy, either "tasks" (default) to use Ray
                tasks, or ActorPoolStrategy(min, max) to use an autoscaling actor pool.
            batch_format: Specify "native" to use the native block format
                (promotes Arrow to pandas), "pandas" to select
                ``pandas.DataFrame`` as the batch format,
                or "pyarrow" to select ``pyarrow.Table``.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).

        Returns:
            The return type is determined by the return type of ``fn``, and the return
            value is combined from results of all groups.
        """
        # Globally sort records by key.
        # Note that sort() will ensure that records of the same key partitioned
        # into the same block.
        if self._key is not None:
            sorted_ds = self._dataset.sort(self._key)
        else:
            sorted_ds = self._dataset.repartition(1)

        def get_key(row):
            if isinstance(self._key, Callable):
                return self._key(row)
            elif isinstance(self._key, str):
                return row[self._key]
            else:
                return None

        # Returns the group boundaries.
        def get_boundaries(block):
            boundaries = []
            pre = None
            for i, item in enumerate(block.iter_rows()):
                if pre is not None and get_key(pre) != get_key(item):
                    boundaries.append(i)
                pre = item
            if block.num_rows() > 0:
                boundaries.append(block.num_rows())
            return boundaries

        # The batch is the entire block, because we have batch_size=None for
        # map_batches() below.
        def group_fn(batch):
            block_accessor = BlockAccessor.for_block(batch)
            boundaries = get_boundaries(block_accessor)
            builder = block_accessor.builder()
            start = 0
            for end in boundaries:
                group = block_accessor.slice(start, end, False)
                applied = fn(group)
                builder.add_block(applied)
                start = end

            rs = builder.build()
            return rs

        # Note we set batch_size=None here, so it will use the entire block as a batch,
        # which ensures that each group will be contained within a batch in entirety.
        return sorted_ds.map_batches(
            group_fn,
            batch_size=None,
            compute=compute,
            batch_format=batch_format,
            **ray_remote_args,
        )

    def count(self) -> Dataset[U]:
        """Compute count aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).count() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby( # doctest: +SKIP
            ...     "A").count() # doctest: +SKIP

        Returns:
            A simple dataset of ``(k, v)`` pairs or an Arrow dataset of
            ``[k, v]`` columns where ``k`` is the groupby key and ``v`` is the
            number of rows with that key.
            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self.aggregate(Count())

    def sum(
        self, on: Union[KeyFn, List[KeyFn]] = None, ignore_nulls: bool = True
    ) -> Dataset[U]:
        """Compute grouped sum aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).sum() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .sum(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range_arrow(100).groupby("value").sum() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .sum(["B", "C"]) # doctest: +SKIP

        Args:
            on: The data subset on which to compute the sum.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a sum of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise sum of all
                  columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the sum; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

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
        return self._aggregate_on(Sum, on, ignore_nulls)

    def min(
        self, on: Union[KeyFn, List[KeyFn]] = None, ignore_nulls: bool = True
    ) -> Dataset[U]:
        """Compute grouped min aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).min() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .min(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range_arrow(100).groupby("value").min() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .min(["B", "C"]) # doctest: +SKIP

        Args:
            on: The data subset on which to compute the min.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a min of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise min of all
                  columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the min; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

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
        return self._aggregate_on(Min, on, ignore_nulls)

    def max(
        self, on: Union[KeyFn, List[KeyFn]] = None, ignore_nulls: bool = True
    ) -> Dataset[U]:
        """Compute grouped max aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).max() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .max(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range_arrow(100).groupby("value").max() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .max(["B", "C"]) # doctest: +SKIP

        Args:
            on: The data subset on which to compute the max.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a max of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise max of all
                  columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the max; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

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
        return self._aggregate_on(Max, on, ignore_nulls)

    def mean(
        self, on: Union[KeyFn, List[KeyFn]] = None, ignore_nulls: bool = True
    ) -> Dataset[U]:
        """Compute grouped mean aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).mean() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .mean(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range_arrow(100).groupby("value").mean() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .mean(["B", "C"]) # doctest: +SKIP

        Args:
            on: The data subset on which to compute the mean.

                - For a simple dataset: it can be a callable or a list thereof,
                  and the default is to take a mean of all rows.
                - For an Arrow dataset: it can be a column name or a list
                  thereof, and the default is to do a column-wise mean of all
                  columns.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the mean; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

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
        return self._aggregate_on(Mean, on, ignore_nulls)

    def std(
        self,
        on: Union[KeyFn, List[KeyFn]] = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> Dataset[U]:
        """Compute grouped standard deviation aggregation.

        This is a blocking operation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby(lambda x: x % 3).std() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .std(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range_arrow(100).groupby("value").std(ddof=0) # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .std(["B", "C"]) # doctest: +SKIP

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
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the std; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

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
        return self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)
