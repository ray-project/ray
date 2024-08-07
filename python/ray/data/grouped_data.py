from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ray.data._internal.aggregate import Count, Max, Mean, Min, Std, Sum
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate
from ray.data.aggregate import AggregateFn
from ray.data.block import BlockAccessor, CallableClass, UserDefinedFunction
from ray.data.dataset import DataBatch, Dataset
from ray.util.annotations import PublicAPI


class _MultiColumnSortedKey:
    """Represents a tuple of group keys with a ``__lt__`` method

    This is a simple implementation to support multi-column groupby.
    While a 1D array of tuples suffices to maintain the lexicographical
    sorted order, a comparison method is also needed in ``np.searchsorted``
    (for computing the group key boundaries).
    """

    __slots__ = ("data",)

    def __init__(self, *args):
        self.data = tuple(args)

    def __lt__(self, obj: "_MultiColumnSortedKey") -> bool:
        return self.data < obj.data

    def __repr__(self) -> str:
        """Print as T(1, 2)"""
        return "T" + self.data.__repr__()


@PublicAPI
class GroupedData:
    """Represents a grouped dataset created by calling ``Dataset.groupby()``.

    The actual groupby is deferred until an aggregation is applied.
    """

    def __init__(
        self,
        dataset: Dataset,
        key: Union[str, List[str]],
    ):
        """Construct a dataset grouped by key (internal API).

        The constructor is not part of the GroupedData API.
        Use the ``Dataset.groupby()`` method to construct one.
        """
        self._dataset = dataset
        self._key = key

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(dataset={self._dataset}, " f"key={self._key!r})"
        )

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
        )
        logical_plan = LogicalPlan(op)
        return Dataset(
            plan,
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
        """Helper for aggregating on a particular subset of the dataset.

        This validates the `on` argument, and converts a list of column names
        to a multi-aggregation. A null `on` results in a
        multi-aggregation on all columns for an Arrow Dataset, and a single
        aggregation on the entire row for a simple Dataset.
        """
        aggs = self._dataset._build_multicolumn_aggs(
            agg_cls, on, ignore_nulls, *args, skip_cols=self._key, **kwargs
        )
        return self.aggregate(*aggs)

    def map_groups(
        self,
        fn: UserDefinedFunction[DataBatch, DataBatch],
        *,
        compute: Union[str, ComputeStrategy] = None,
        batch_format: Optional[str] = "default",
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None,
        concurrency: Optional[Union[int, Tuple[int, int]]] = None,
        **ray_remote_args,
    ) -> "Dataset":
        """Apply the given function to each group of records of this dataset.

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

        def get_key_boundaries(block_accessor: BlockAccessor) -> List[int]:
            """Compute block boundaries based on the key(s)"""

            import numpy as np

            # Get the keys of the batch in numpy array format
            keys = block_accessor.to_numpy(self._key)

            if isinstance(keys, dict):
                # For multiple keys, we generate a separate tuple column
                convert_to_multi_column_sorted_key = np.vectorize(_MultiColumnSortedKey)
                keys: np.ndarray = convert_to_multi_column_sorted_key(*keys.values())

            boundaries = []
            start = 0
            while start < keys.size:
                end = start + np.searchsorted(keys[start:], keys[start], side="right")
                boundaries.append(end)
                start = end
            return boundaries

        # The batch is the entire block, because we have batch_size=None for
        # map_batches() below.
        def apply_udf_to_groups(udf, batch, *args, **kwargs):
            block = BlockAccessor.batch_to_block(batch)
            block_accessor = BlockAccessor.for_block(block)
            if self._key:
                boundaries = get_key_boundaries(block_accessor)
            else:
                boundaries = [block_accessor.num_rows()]
            start = 0
            for end in boundaries:
                group_block = block_accessor.slice(start, end)
                group_block_accessor = BlockAccessor.for_block(group_block)
                # Convert block of each group to batch format here, because the
                # block format here can be different from batch format
                # (e.g. block is Arrow format, and batch is NumPy format).
                group_batch = group_block_accessor.to_batch_format(batch_format)
                applied = udf(group_batch, *args, **kwargs)
                yield applied
                start = end

        if isinstance(fn, CallableClass):

            class wrapped_fn:
                def __init__(self, *args, **kwargs):
                    self.fn = fn(*args, **kwargs)

                def __call__(self, batch, *args, **kwargs):
                    yield from apply_udf_to_groups(self.fn, batch, *args, **kwargs)

        else:

            def wrapped_fn(batch, *args, **kwargs):
                yield from apply_udf_to_groups(fn, batch, *args, **kwargs)

        # Change the name of the wrapped function so that users see the name of their
        # function rather than `wrapped_fn` in the progress bar.
        wrapped_fn.__name__ = fn.__name__

        # Note we set batch_size=None here, so it will use the entire block as a batch,
        # which ensures that each group will be contained within a batch in entirety.
        return sorted_ds._map_batches_without_batch_size_validation(
            wrapped_fn,
            batch_size=None,
            compute=compute,
            batch_format=batch_format,
            zero_copy_batch=False,
            fn_args=fn_args,
            fn_kwargs=fn_kwargs,
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            concurrency=concurrency,
            ray_remote_args_fn=None,
            **ray_remote_args,
        )

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

    def sum(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise sum column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Sum, on, ignore_nulls)

    def min(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise min column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Min, on, ignore_nulls)

    def max(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise max column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Max, on, ignore_nulls)

    def mean(
        self, on: Union[str, List[str]] = None, ignore_nulls: bool = True
    ) -> Dataset:
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise mean column for each original column
              in the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
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
    ) -> Dataset:
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

            - ``on=None``: a dataset containing a groupby key column,
              ``"k"``, and a column-wise std column for each original column in
              the dataset.
            - ``on=["col_1", ..., "col_n"]``: a dataset of ``n + 1``
              columns where the first column is the groupby key and the second
              through ``n + 1`` columns are the results of the aggregations.

            If groupby key is ``None`` then the key part of return is omitted.
        """
        return self._aggregate_on(Std, on, ignore_nulls, ddof=ddof)


# Backwards compatibility alias.
GroupedDataset = GroupedData
