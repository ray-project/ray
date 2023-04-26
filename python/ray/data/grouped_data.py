from typing import List, Tuple, Union, Optional

from ray.data._internal import sort
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.shuffle import ShuffleOp, SimpleShufflePlan
from ray.data._internal.push_based_shuffle import PushBasedShufflePlan
from ._internal.table_block import TableBlockAccessor
from ray.data.aggregate import (
    _AggregateOnKeyBase,
    AggregateFn,
    Count,
    Max,
    Mean,
    Min,
    Std,
    Sum,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    KeyType,
    UserDefinedFunction,
)
from ray.data.context import DataContext
from ray.data.datastream import DataBatch, Datastream
from ray.util.annotations import PublicAPI


class _GroupbyOp(ShuffleOp):
    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[KeyType],
        key: str,
        aggs: Tuple[AggregateFn],
    ) -> List[Union[BlockMetadata, Block]]:
        """Partition the block and combine rows with the same key."""
        stats = BlockExecStats.builder()

        block = _GroupbyOp._prune_unused_columns(block, key, aggs)

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
        return parts + [meta]

    @staticmethod
    def reduce(
        key: str,
        aggs: Tuple[AggregateFn],
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> (Block, BlockMetadata):
        """Aggregate sorted and partially combined blocks."""
        return BlockAccessor.for_block(mapper_outputs[0]).aggregate_combined_blocks(
            list(mapper_outputs), key, aggs, finalize=not partial_reduce
        )

    @staticmethod
    def _prune_unused_columns(
        block: Block,
        key: str,
        aggs: Tuple[AggregateFn],
    ) -> Block:
        """Prune unused columns from block before aggregate."""
        prune_columns = True
        columns = set()

        if isinstance(key, str):
            columns.add(key)
        elif callable(key):
            prune_columns = False

        for agg in aggs:
            if isinstance(agg, _AggregateOnKeyBase) and isinstance(agg._key_fn, str):
                columns.add(agg._key_fn)
            elif not isinstance(agg, Count):
                # Don't prune columns if any aggregate key is not string.
                prune_columns = False

        block_accessor = BlockAccessor.for_block(block)
        if (
            prune_columns
            and isinstance(block_accessor, TableBlockAccessor)
            and block_accessor.num_rows() > 0
        ):
            return block_accessor.select(list(columns))
        else:
            return block


class SimpleShuffleGroupbyOp(_GroupbyOp, SimpleShufflePlan):
    pass


class PushBasedGroupbyOp(_GroupbyOp, PushBasedShufflePlan):
    pass


@PublicAPI
class GroupedData:
    """Represents a grouped datastream created by calling ``Datastream.groupby()``.

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(self, datastream: Datastream, key: str):
        """Construct a datastream grouped by key (internal API).

        The constructor is not part of the GroupedData API.
        Use the ``Datastream.groupby()`` method to construct one.
        """
        self._datastream = datastream
        self._key = key

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(datastream={self._datastream}, "
            f"key={self._key!r})"
        )

    def aggregate(self, *aggs: AggregateFn) -> Datastream:
        """Implements an accumulator-based aggregation.

        Args:
            aggs: Aggregations to do.

        Returns:
            The output is an datastream of ``n + 1`` columns where the first column
            is the groupby key and the second through ``n + 1`` columns are the
            results of the aggregations.
            If groupby key is ``None`` then the key part of return is omitted.
        """

        def do_agg(blocks, task_ctx: TaskContext, clear_input_blocks: bool, *_):
            # TODO: implement clear_input_blocks
            stage_info = {}
            if len(aggs) == 0:
                raise ValueError("Aggregate requires at least one aggregation")
            for agg in aggs:
                agg._validate(self._datastream.schema(fetch_if_missing=True))
            # Handle empty datastream.
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
                    task_ctx,
                )
            ctx = DataContext.get_current()
            if ctx.use_push_based_shuffle:
                shuffle_op_cls = PushBasedGroupbyOp
            else:
                shuffle_op_cls = SimpleShuffleGroupbyOp
            shuffle_op = shuffle_op_cls(
                map_args=[boundaries, self._key, aggs], reduce_args=[self._key, aggs]
            )
            return shuffle_op.execute(
                blocks,
                num_reducers,
                clear_input_blocks,
                ctx=task_ctx,
            )

        plan = self._datastream._plan.with_stage(
            AllToAllStage(
                "Aggregate",
                None,
                do_agg,
                sub_stage_names=["SortSample", "ShuffleMap", "ShuffleReduce"],
            )
        )

        logical_plan = self._datastream._logical_plan
        if logical_plan is not None:
            op = Aggregate(
                logical_plan.dag,
                key=self._key,
                aggs=aggs,
            )
            logical_plan = LogicalPlan(op)
        return Datastream(
            plan,
            self._datastream._epoch,
            self._datastream._lazy,
            logical_plan,
        )

    def _aggregate_on(
        self,
        agg_cls: type,
        on: Union[str, List[str]],
        ignore_nulls: bool,
        *args,
        **kwargs,
    ):
        """Helper for aggregating on a particular subset of the datastream.

        This validates the `on` argument, and converts a list of column names
        to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Datastream, and a single
        aggregation on the entire row for a simple Datastream.
        """
        aggs = self._datastream._build_multicolumn_aggs(
            agg_cls, on, ignore_nulls, *args, skip_cols=self._key, **kwargs
        )
        return self.aggregate(*aggs)

    def map_groups(
        self,
        fn: UserDefinedFunction[DataBatch, DataBatch],
        *,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: Optional[str] = "default",
        **ray_remote_args,
    ) -> "Datastream":
        """Apply the given function to each group of records of this datastream.

        While map_groups() is very flexible, note that it comes with downsides:
            * It may be slower than using more specific methods such as min(), max().
            * It requires that each group fits in memory on a single node.

        In general, prefer to use aggregate() instead of map_groups().

        Examples:
            >>> # Return a single record per group (list of multiple records in,
            >>> # list of a single record out).
            >>> import ray
            >>> import pandas as pd
            >>> import numpy as np
            >>> # Get first value per group.
            >>> ds = ray.data.from_items([ # doctest: +SKIP
            ...     {"group": 1, "value": 1},
            ...     {"group": 1, "value": 2},
            ...     {"group": 2, "value": 3},
            ...     {"group": 2, "value": 4}])
            >>> ds.groupby("group").map_groups( # doctest: +SKIP
            ...     lambda g: {"result": np.array([g["value"][0]])})

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
                tasks, ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed-size actor
                pool, or ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` for an
                autoscaling actor pool.
            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.
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
            sorted_ds = self._datastream.sort(self._key)
        else:
            sorted_ds = self._datastream.repartition(1)

        # Returns the group boundaries.
        def get_key_boundaries(block_accessor: BlockAccessor):
            import numpy as np

            boundaries = []
            # Get the keys of the batch in numpy array format
            keys = block_accessor.to_numpy(self._key)
            start = 0
            while start < keys.size:
                end = start + np.searchsorted(keys[start:], keys[start], side="right")
                boundaries.append(end)
                start = end
            return boundaries

        # The batch is the entire block, because we have batch_size=None for
        # map_batches() below.
        def group_fn(batch):
            block = BlockAccessor.batch_to_block(batch)
            block_accessor = BlockAccessor.for_block(block)
            if self._key:
                boundaries = get_key_boundaries(block_accessor)
            else:
                boundaries = [block_accessor.num_rows()]
            builder = DelegatingBlockBuilder()
            start = 0
            for end in boundaries:
                group_block = block_accessor.slice(start, end)
                group_block_accessor = BlockAccessor.for_block(group_block)
                # Convert block of each group to batch format here, because the
                # block format here can be different from batch format
                # (e.g. block is Arrow format, and batch is NumPy format).
                group_batch = group_block_accessor.to_batch_format(batch_format)
                applied = fn(group_batch)
                builder.add_batch(applied)
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

    def count(self) -> Datastream:
        """Compute count aggregation.

        Examples:
            >>> import ray
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby( # doctest: +SKIP
            ...     "A").count() # doctest: +SKIP

        Returns:
            A datastream of ``[k, v]`` columns where ``k`` is the groupby key and
            ``v`` is the number of rows with that key.
            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self.aggregate(Count())

    def sum(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Datastream:
        r"""Compute grouped sum aggregation.

        Examples:
            >>> import ray
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3) \ # doctest: +SKIP
            ...     .sum(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range(100).groupby("id").sum() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .sum(["B", "C"]) # doctest: +SKIP

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the sum; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The sum result.

            For different values of ``on``, the return varies:

            - ``on=None``: a datastream containing a groupby key column,
              ``"k"``, and a column-wise sum column for each original column
              in the datastream.
            - ``on=["col_1", ..., "col_n"]``: a datastream of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Sum, on, ignore_nulls)

    def min(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Datastream:
        """Compute grouped min aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").min() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .min(["B", "C"]) # doctest: +SKIP

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the min; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The min result.

            For different values of ``on``, the return varies:

            - ``on=None``: a datastream containing a groupby key column,
              ``"k"``, and a column-wise min column for each original column in
              the datastream.
            - ``on=["col_1", ..., "col_n"]``: a datastream of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Min, on, ignore_nulls)

    def max(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Datastream:
        """Compute grouped max aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").max() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .max(["B", "C"]) # doctest: +SKIP

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the max; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The max result.

            For different values of ``on``, the return varies:

            - ``on=None``: a datastream containing a groupby key column,
              ``"k"``, and a column-wise max column for each original column in
              the datastream.
            - ``on=["col_1", ..., "col_n"]``: a datastream of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Max, on, ignore_nulls)

    def mean(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Datastream:
        """Compute grouped mean aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").mean() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)]) \ # doctest: +SKIP
            ...     .groupby("A") \ # doctest: +SKIP
            ...     .mean(["B", "C"]) # doctest: +SKIP

        Args:
            on: a column name or a list of column names to aggregate.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the mean; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The mean result.

            For different values of ``on``, the return varies:

            - ``on=None``: a datastream containing a groupby key column,
              ``"k"``, and a column-wise mean column for each original column
              in the datastream.
            - ``on=["col_1", ..., "col_n"]``: a datastream of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Mean, on, ignore_nulls)

    def std(
        self,
        on: Union[str, List[str]] = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> Datastream:
        """Compute grouped standard deviation aggregation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby("id").std(ddof=0) # doctest: +SKIP
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
            on: a column name or a list of column names to aggregate.
            ddof: Delta Degrees of Freedom. The divisor used in calculations
                is ``N - ddof``, where ``N`` represents the number of elements.
            ignore_nulls: Whether to ignore null values. If ``True``, null
                values will be ignored when computing the std; if ``False``,
                if a null value is encountered, the output will be null.
                We consider np.nan, None, and pd.NaT to be null values.
                Default is ``True``.

        Returns:
            The standard deviation result.

            For different values of ``on``, the return varies:

            - ``on=None``: a datastream containing a groupby key column,
              ``"k"``, and a column-wise std column for each original column in
              the datastream.
            - ``on=["col_1", ..., "col_n"]``: a datastream of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)


# Backwards compatibility alias.
GroupedDataset = GroupedData
