from collections.abc import Iterator as IteratorABC
from functools import partial
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Std, Sum
from ray.data.block import (
    Block,
    BlockAccessor,
    CallableClass,
    DataBatch,
    UserDefinedFunction,
)
from ray.data.context import ShuffleStrategy
from ray.data.dataset import EXPRESSION_API_GROUP, Dataset
from ray.data.expressions import DownloadExpr, Expr, StarExpr
from ray.util.annotations import PublicAPI

CDS_API_GROUP = "Computations or Descriptive Stats"
FA_API_GROUP = "Function Application"


class GroupedData:
    """Represents a grouped dataset created by calling ``Dataset.groupby()``.

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(
        self,
        dataset: Dataset,
        key: Optional[Union[str, List[str]]],
        *,
        num_partitions: Optional[int],
    ):
        """Construct a dataset grouped by key (internal API).

        The constructor is not part of the GroupedData API.
        Use the ``Dataset.groupby()`` method to construct one.
        """
        self._dataset: Dataset = dataset
        self._key: Optional[Union[str, List[str]]] = key
        self._num_partitions: Optional[int] = num_partitions

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(dataset={self._dataset}, " f"key={self._key!r})"
        )

    @PublicAPI(api_group=FA_API_GROUP)
    def aggregate(self, *aggs: AggregateFn) -> Dataset:
        """Implements an accumulator-based aggregation.

        Args:
            aggs: Aggregations to do.

        Returns:
            The output is an dataset of ``n + 1`` columns where the first column
            is the groupby key and the second through ``n + 1`` columns are the
            results of the aggregations.
            If groupby key is ``None`` then the key part of return is omitted.
        """

        plan = self._dataset._plan.copy()
        op = Aggregate(
            self._dataset._logical_plan.dag,
            key=self._key,
            aggs=aggs,
            num_partitions=self._num_partitions,
        )
        logical_plan = LogicalPlan(op, self._dataset.context)
        return Dataset(
            plan,
            logical_plan,
        )

    def _aggregate_on(
        self,
        agg_cls: type,
        on: Union[str, List[str]],
        *args,
        **kwargs,
    ):
        """Helper for aggregating on a particular subset of the dataset.

        This validates the `on` argument, and converts a list of column names
        to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Dataset, and a single
        aggregation on the entire row for a simple Dataset.
        """
        aggs = self._dataset._build_multicolumn_aggs(
            agg_cls, on, *args, skip_cols=self._key, **kwargs
        )
        return self.aggregate(*aggs)

    @PublicAPI(api_group=FA_API_GROUP)
    def map_groups(
        self,
        fn: UserDefinedFunction[DataBatch, DataBatch],
        *,
        zero_copy_batch: bool = True,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: Optional[str] = "default",
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[Union[int, Tuple[int, int], Tuple[int, int, int]]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Apply the given function to each group of records of this dataset.

        While map_groups() is very flexible, note that it comes with downsides:

        * It may be slower than using more specific methods such as min(), max().
        * It requires that each group fits in memory on a single node.

        In general, prefer to use `aggregate()` instead of `map_groups()`.

        .. warning::
            Specifying both ``num_cpus`` and ``num_gpus`` for map tasks is experimental,
            and may result in scheduling or stability issues. Please
            `report any issues <https://github.com/ray-project/ray/issues/new/choose>`_
            to the Ray team.

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
            zero_copy_batch: If True, each group of rows (batch) will be provided w/o
                making an additional copy.
            compute: The compute strategy to use for the map operation.

                * If ``compute`` is not specified for a function, will use ``ray.data.TaskPoolStrategy()`` to launch concurrent tasks based on the available resources and number of input blocks.

                * Use ``ray.data.TaskPoolStrategy(size=n)`` to launch at most ``n`` concurrent Ray tasks.

                * If ``compute`` is not specified for a callable class, will use ``ray.data.ActorPoolStrategy(min_size=1, max_size=None)`` to launch an autoscaling actor pool from 1 to unlimited workers.

                * Use ``ray.data.ActorPoolStrategy(size=n)`` to use a fixed size actor pool of ``n`` workers.

                * Use ``ray.data.ActorPoolStrategy(min_size=m, max_size=n)`` to use an autoscaling actor pool from ``m`` to ``n`` workers.

                * Use ``ray.data.ActorPoolStrategy(min_size=m, max_size=n, initial_size=initial)`` to use an autoscaling actor pool from ``m`` to ``n`` workers, with an initial size of ``initial``.

            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.
            fn_args: Arguments to `fn`.
            fn_kwargs: Keyword arguments to `fn`.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPUs to reserve for each parallel map worker.
            num_gpus: The number of GPUs to reserve for each parallel map worker. For
                example, specify `num_gpus=1` to request 1 GPU for each parallel map
                worker.
            memory: The heap memory in bytes to reserve for each parallel map worker.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor or task, and will be called each time prior
                to initializing the worker. Args returned from this dict will always
                override the args in ``ray_remote_args``. Note: this is an advanced,
                experimental feature.
            concurrency: This argument is deprecated. Use ``compute`` argument.
            ray_remote_args: Additional resource requirements to request from
                Ray (e.g., num_gpus=1 to request GPUs for the map tasks). See
                :func:`ray.remote` for details.

        Returns:
            The return type is determined by the return type of ``fn``, and the return
            value is combined from results of all groups.

        .. seealso::

            :meth:`GroupedData.aggregate`
                Use this method for common aggregation use cases.
        """

        # Prior to applying map operation we have to shuffle the data based on provided
        # key and (optionally) number of partitions
        #
        #   - In case key is none, we repartition into a single block
        #   - In case when hash-shuffle strategy is employed -- perform `repartition_and_sort`
        #   - Otherwise we perform "global" sort of the dataset (to co-locate rows with the
        #     same key values)
        if self._key is None:
            shuffled_ds = self._dataset.repartition(1)
        elif self._dataset.context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
            num_partitions = (
                self._num_partitions
                or self._dataset.context.default_hash_shuffle_parallelism
            )
            shuffled_ds = self._dataset.repartition(
                num_partitions,
                keys=self._key,
                # Blocks must be sorted after repartitioning, such that group
                # of rows sharing the same key values are co-located
                sort=True,
            )
        else:
            shuffled_ds = self._dataset.sort(self._key)

        # The batch is the entire block, because we have batch_size=None for
        # map_batches() below.

        if self._key is None:
            keys = []
        elif isinstance(self._key, str):
            keys = [self._key]
        elif isinstance(self._key, List):
            keys = self._key
        else:
            raise ValueError(
                f"Group-by keys are expected to either be a single column (str) "
                f"or a list of columns (got '{self._key}')"
            )

        # NOTE: It's crucial to make sure that UDF isn't capturing `GroupedData`
        #       object in its closure to ensure its serializability
        #
        # See https://github.com/ray-project/ray/issues/54280 for more details
        if isinstance(fn, CallableClass):

            class wrapped_fn:
                def __init__(self, *args, **kwargs):
                    self.fn = fn(*args, **kwargs)

                def __call__(self, batch, *args, **kwargs):
                    yield from _apply_udf_to_groups(
                        self.fn, batch, keys, batch_format, *args, **kwargs
                    )

        else:

            def wrapped_fn(batch, *args, **kwargs):
                yield from _apply_udf_to_groups(
                    fn, batch, keys, batch_format, *args, **kwargs
                )

        # Change the name of the wrapped function so that users see the name of their
        # function rather than `wrapped_fn` in the progress bar.
        if isinstance(fn, partial):
            wrapped_fn.__name__ = fn.func.__name__
        else:
            wrapped_fn.__name__ = fn.__name__

        # NOTE: We set batch_size=None here, so that every batch contains the entire block,
        #       guaranteeing that groups are contained in full (ie not being split)
        return shuffled_ds._map_batches_without_batch_size_validation(
            wrapped_fn,
            batch_size=None,
            compute=compute,
            # NOTE: We specify `batch_format` as none to avoid converting
            #       back-n-forth between batch and block formats (instead we convert
            #       once per group inside the method applying the UDF itself)
            batch_format=None,
            zero_copy_batch=zero_copy_batch,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            concurrency=concurrency,
            udf_modifying_row_count=False,
            ray_remote_args_fn=ray_remote_args_fn,
            **ray_remote_args,
        )

    @PublicAPI(api_group=EXPRESSION_API_GROUP, stability="alpha")
    def with_column(
        self,
        column_name: str,
        expr: Expr,
        **ray_remote_args,
    ) -> Dataset:
        """Add a new column to each group using an expression.

        The supplied expression is evaluated against every row in each group, and
        the resulting column is appended to the group's records. The output dataset
        preserves the original rows and columns.

        Examples:
            >>> import ray
            >>> from ray.data.expressions import col
            >>> ds = (
            ...     ray.data.from_items([{"group": 1, "value": 1}, {"group": 1, "value": 2}])
            ...     .groupby("group")
            ...     .with_column("value_twice", col("value") * 2)
            ...     .sort(["group", "value"])
            ... )
            >>> ds.take_all()
            [{'group': 1, 'value': 1, 'value_twice': 2}, {'group': 1, 'value': 2, 'value_twice': 4}]

        Args:
            column_name: Name of the column to add.
            expr: Expression that yields the values for the new column.
            **ray_remote_args: Additional resource requirements to request from Ray
                for the underlying map tasks (for example, ``num_gpus=1``).

        Returns:
            A new :class:`~ray.data.Dataset` containing all existing columns plus
            the newly computed column.
        """
        if not isinstance(column_name, str) or not column_name:
            raise ValueError(
                f"column_name must be a non-empty string, got: {column_name!r}"
            )
        if not isinstance(expr, Expr):
            raise TypeError(
                "expr must be a Ray Data expression created via the expression API."
            )
        if isinstance(expr, DownloadExpr):
            raise TypeError(
                "GroupedData.with_column does not yet support download expressions."
            )

        aliased_expr = expr.alias(column_name)
        projection_exprs = [StarExpr(), aliased_expr]

        def _project_group(block: Block) -> Block:
            from ray.data._internal.planner.plan_expression.expression_evaluator import (
                eval_projection,
            )

            return eval_projection(projection_exprs, block)

        return self.map_groups(
            _project_group,
            batch_format=None,
            zero_copy_batch=True,
            **ray_remote_args,
        )

    @PublicAPI(api_group=CDS_API_GROUP)
    def count(self) -> Dataset:
        """Compute count aggregation.

        Examples:
            >>> import ray
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": x % 3, "B": x} for x in range(100)]).groupby( # doctest: +SKIP
            ...     "A").count() # doctest: +SKIP

        Returns:
            A dataset of ``[k, v]`` columns where ``k`` is the groupby key and
            ``v`` is the number of rows with that key.
            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self.aggregate(Count())

    @PublicAPI(api_group=CDS_API_GROUP)
    def sum(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
        r"""Compute grouped sum aggregation.

        Examples:
            >>> import ray
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     (i % 3, i, i**2) # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby(lambda x: x[0] % 3)  # doctest: +SKIP
            ...     .sum(lambda x: x[2]) # doctest: +SKIP
            >>> ray.data.range(100).groupby("id").sum() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby("A")  # doctest: +SKIP
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise sum column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Sum, on, ignore_nulls=ignore_nulls)

    @PublicAPI(api_group=CDS_API_GROUP)
    def min(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
        r"""Compute grouped min aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").min() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby("A")  # doctest: +SKIP
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise min column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Min, on, ignore_nulls=ignore_nulls)

    @PublicAPI(api_group=CDS_API_GROUP)
    def max(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
        r"""Compute grouped max aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").max() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby("A")  # doctest: +SKIP
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise max column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Max, on, ignore_nulls=ignore_nulls)

    @PublicAPI(api_group=CDS_API_GROUP)
    def mean(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
        r"""Compute grouped mean aggregation.

        Examples:
            >>> import ray
            >>> ray.data.le(100).groupby("value").mean() # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby("A")  # doctest: +SKIP
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise mean column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Mean, on, ignore_nulls=ignore_nulls)

    @PublicAPI(api_group=CDS_API_GROUP)
    def std(
        self,
        on: Union[str, List[str]] = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ) -> Dataset:
        r"""Compute grouped standard deviation aggregation.

        Examples:
            >>> import ray
            >>> ray.data.range(100).groupby("id").std(ddof=0) # doctest: +SKIP
            >>> ray.data.from_items([ # doctest: +SKIP
            ...     {"A": i % 3, "B": i, "C": i**2} # doctest: +SKIP
            ...     for i in range(100)])  # doctest: +SKIP
            ...     .groupby("A")  # doctest: +SKIP
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise std column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Std, on, ignore_nulls=ignore_nulls, ddof=ddof)


def _apply_udf_to_groups(
    udf: Union[
        Callable[[DataBatch, ...], DataBatch],
        Callable[[DataBatch, ...], Iterator[DataBatch]],
    ],
    block: Block,
    keys: List[str],
    batch_format: Optional[str],
    *args: Any,
    **kwargs: Any,
) -> Iterator[DataBatch]:
    """Apply UDF to groups of rows having the same set of values of the specified
    columns (keys).

    NOTE: This function is defined at module level to avoid capturing closures and make it serializable.
    """
    block_accessor = BlockAccessor.for_block(block)

    boundaries = block_accessor._get_group_boundaries_sorted(keys)

    for start, end in zip(boundaries[:-1], boundaries[1:]):
        group_block = block_accessor.slice(start, end, copy=False)
        group_block_accessor = BlockAccessor.for_block(group_block)

        # Convert corresponding block of each group to batch format here,
        # because the block format here can be different from batch format
        # (e.g. block is Arrow format, and batch is NumPy format).
        result = udf(
            group_block_accessor.to_batch_format(batch_format), *args, **kwargs
        )

        # Check if the UDF returned an iterator/generator.
        if isinstance(result, IteratorABC):
            # If so, yield each item from the iterator.
            yield from result
        else:
            # Otherwise, yield the single result.
            yield result


# Backwards compatibility alias.
GroupedDataset = GroupedData
