from __future__ import annotations

import abc
import logging
import pickle
import time
import types
import typing
from collections import defaultdict
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

import pyarrow as pa

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.gpu_shuffle.hash_shuffle import (
    _GPU_PARTITION_ID_KEY,
    GPURankPool,
    GPUShuffleOperator,
    _derive_num_gpu_ranks,
)
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.aggregate import AggregateFn, AggregateFnV2, Count, Max, Mean, Min, Sum
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockStats,
    Schema,
)
from ray.data.context import DataContext
from ray.data.datatype import DataType

if typing.TYPE_CHECKING:
    import cudf

    from ray.data._internal.progress.base_progress import BaseProgressBar


logger = logging.getLogger(__name__)


_GLOBAL_AGGREGATE_KEY = "__hash_aggregate_global_key"


def _resolve_aggregation_names(aggregation_fns: Sequence[AggregateFn]) -> List[str]:
    """Resolve duplicate aggregate names the same way TableBlockAccessor does."""
    counts: Dict[str, int] = defaultdict(int)
    resolved_names: List[str] = []

    for agg in aggregation_fns:
        name = agg.name
        if counts[name] > 0:
            name = TableBlockAccessor._munge_conflict(name, counts[name])
        counts[agg.name] += 1
        resolved_names.append(name)

    return resolved_names


def to_datatypes(
    dtypes: Dict[str, Any],
) -> Dict[str, Optional[DataType]]:
    """Convert a dict of column names and dtypes to ``DataType`` values."""
    return {column: DataType.from_dtype(dtype) for column, dtype in dtypes.items()}


def _cudf_column_dtype(df: cudf.DataFrame, column: str) -> Optional[DataType]:
    """Get the DataType of a column from a ``cudf.DataFrame``.

    Returns None if the column is not found.
    """
    if column not in df.columns:
        return None

    raw_dtype = df[column].dtype
    try:
        import cudf

        if isinstance(df, cudf.DataFrame):
            return DataType.from_cudf(raw_dtype)
    except (AttributeError, ImportError, TypeError):
        pass
    return DataType.from_dtype(raw_dtype)


def _schema_column_dtype(
    schema: Optional[Schema], column: Optional[str]
) -> Optional[DataType]:
    """Get the DataType of a column from a ``Schema`` (Arrow or Pandas).

    Returns None if the column is not found (or None).
    """
    if schema is None or column is None:
        return None

    try:
        names = schema.names
    except AttributeError:
        return None

    if not isinstance(names, (list, tuple)) or column not in names:
        return None

    if isinstance(schema, pa.Schema):
        return DataType.from_dtype(schema.field(column).type)

    if isinstance(schema, PandasBlockSchema):
        return DataType.from_dtype(schema.types[names.index(column)])

    return None


def _reduction_dtype(dtype: Optional[DataType], aggregation: str) -> Optional[DataType]:
    """Return the CPU aggregate path's scalar reduction dtype for common dtypes."""
    if dtype is None:
        return None

    if dtype.is_null_type():
        return DataType.from_numpy("float64" if aggregation == "mean" else "int64")

    if aggregation in ("sum", "mean"):
        if dtype.is_boolean_type():
            return DataType.from_numpy("int64")
        if dtype.is_integer_type():
            return DataType.from_numpy("uint64" if dtype.is_uint64_type() else "int64")
        if dtype.is_floating_type():
            return DataType.from_numpy("float64")

    if aggregation in ("min", "max"):
        if dtype.is_boolean_type():
            return DataType.from_numpy("bool")
        if dtype.is_integer_type():
            return DataType.from_numpy("uint64" if dtype.is_uint64_type() else "int64")
        if dtype.is_floating_type():
            return DataType.from_numpy("float64")

    return None


def _resolve_null_dtype(
    dtype: Optional[DataType], aggregation: str
) -> Optional[DataType]:
    """Choose a concrete data type for null schema types.

    Used for GPU shuffle key and accumulator columns whose input schema is
    ``pa.null()`` (e.g. all-null columns with unknown type or empty columns).

    Non-null dtypes are returned unchanged.

    ``float64`` is used for shuffle keys and ``mean`` accumulators so those
    columns can hold NaN/null values; ``int64`` is used for other accumulators.
    """
    if dtype is None:
        return None
    if dtype.is_null_type():
        return DataType.from_numpy(
            "float64" if aggregation in ("key", "mean") else "int64"
        )
    return dtype


def _cast_column_to_dtype(
    df: cudf.DataFrame, column: str, dtype: Optional[DataType]
) -> None:
    """Cast a ``cudf.DataFrame`` column to specified dtype in place."""
    if dtype is None or column not in df.columns:
        return
    dtype = DataType.from_dtype(dtype)
    if dtype is None:
        return

    try:
        cast_dtype = dtype.to_cudf_type()
    except (TypeError, ValueError, NotImplementedError):
        return

    try:
        df[column] = df[column].astype(cast_dtype)
    except (TypeError, ValueError, NotImplementedError):
        # fallback for handling all-null columns
        is_all_null = len(df) == 0 or bool(df[column].isnull().all())
        if not is_all_null:
            return

        import cudf

        if isinstance(df, cudf.DataFrame) and hasattr(cudf, "Series"):
            df[column] = cudf.Series([None] * len(df), dtype=cast_dtype)


def _fill_missing_count(
    result: cudf.DataFrame, count_column: str, dtype: Optional[DataType] = None
) -> None:
    """Fill missing counts with 0 and cast to specified dtype."""
    if count_column not in result.columns:
        result[count_column] = 0
    else:
        result[count_column] = result[count_column].fillna(0)
    _cast_column_to_dtype(result, count_column, dtype)


def _fill_missing_reduction(
    result: cudf.DataFrame, reduction_column: str, dtype: Optional[DataType] = None
) -> None:
    """Fill any missing reduction values with None and cast to specified dtype."""
    if reduction_column not in result.columns:
        result[reduction_column] = None
    _cast_column_to_dtype(result, reduction_column, dtype)


def _all_counts_zero(df: cudf.DataFrame, count_column: str) -> bool:
    """Check if all counts are zero."""
    if len(df) == 0 or count_column not in df.columns:
        # do not evaluate empty dataframes or missing columns
        return False
    try:
        return bool((df[count_column] == 0).all())
    except TypeError:
        # fallback for non-numeric columns
        return False


def _empty_dataframe(
    cudf_module: types.ModuleType,
    columns: Sequence[str],
    dtypes: Optional[Dict[str, Optional[DataType]]] = None,
) -> cudf.DataFrame:
    """Create an empty ``cudf.DataFrame`` with specified columns and dtypes."""
    dtypes = dtypes or {}
    df = cudf_module.DataFrame()
    for column in columns:
        df[column] = []
        _cast_column_to_dtype(df, column, dtypes.get(column))
    return df


class GPUAggregateFn(AggregateFnV2):
    """Extension point for GPU-enabled aggregations.

    This class adds methods to the standard ``AggregateFnV2`` contract for GPU-enabled
    aggregations.
    """

    @abc.abstractmethod
    def gpu_accumulator_columns(self, accumulator_prefix: str) -> Tuple[str, ...]:
        """Return intermediate GPU accumulator column names."""
        ...

    @abc.abstractmethod
    def gpu_partial_aggregate(
        self,
        df: Any,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
        input_schema: Optional[Schema] = None,
    ) -> Any:
        """Aggregate one input block into GPU accumulator columns."""
        ...

    @abc.abstractmethod
    def gpu_final_aggregate(
        self,
        df: Any,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
    ) -> Any:
        """Aggregate shuffled GPU accumulator columns into final output."""
        ...

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        """Return accumulator values for an empty global-aggregation input block."""
        return {
            column: None for column in self.gpu_accumulator_columns(accumulator_prefix)
        }

    def gpu_partial_accumulator_dtypes(
        self,
        df: Any,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return preferred dtypes for partial GPU accumulator columns."""
        return {
            column: None for column in self.gpu_accumulator_columns(accumulator_prefix)
        }

    def gpu_final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        """Return Arrow output type overrides for GPU final outputs."""
        return {}

    def gpu_final_output_dtypes(
        self,
        df: Any,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return preferred GPU dtypes for final output columns."""
        return {}


class _BuiltinGPUAggregateFn(GPUAggregateFn):
    """GPU aggregation implementation for builtin aggregations.

    Supported builtin aggregations: count, sum, min, max, mean.

    Args:
        cpu_agg: Original built-in CPU aggregate instance from plan.
        source_dtype: Source dtype of the target column. If not provided, it will
            be inferred from the input schema.
    """

    _cudf_aggregate_name: ClassVar[str]

    def __init__(
        self,
        cpu_agg: AggregateFnV2,
        *,
        source_dtype: Optional[DataType] = None,
    ) -> None:
        self._wrapped = cpu_agg
        self.source_dtype = source_dtype
        super().__init__(
            cpu_agg.name,
            on=cpu_agg.get_target_column(),
            ignore_nulls=cpu_agg._ignore_nulls,
            zero_factory=self._zero_factory,
        )

    def _zero_factory(self) -> Any:
        return 0

    def aggregate_block(self, block: Block) -> Any:
        return self._wrapped.aggregate_block(block)

    def combine(self, current_accumulator: Any, new: Any) -> Any:
        return self._wrapped.combine(current_accumulator, new)

    def finalize(self, accumulator: Any) -> Any:
        return self._wrapped.finalize(accumulator)

    @property
    def target_column(self) -> Optional[str]:
        return self.get_target_column()

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    def gpu_accumulator_columns(self, accumulator_prefix: str) -> Tuple[str, ...]:
        """Return the intermediate accumulator columns from partial aggregations.

        The accumulator column is ``{accumulator_prefix}_value``.
        """
        return (f"{accumulator_prefix}_value",)

    def gpu_partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and accumulator columns.

        The partial aggregate is called for each block of the dataset before the
        shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.
            output_name: The name of the output column.
            accumulator_prefix: The prefix for intermediate accumulator columns.
            input_schema: The input schema.

        Returns:
            A cuDF DataFrame with key columns and accumulator columns.
        """
        assert self.target_column is not None

        acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
        size_col = f"{accumulator_prefix}_size"
        count_col = f"{accumulator_prefix}_count"
        target_dtype = self._target_accumulator_dtype(df, input_schema=input_schema)

        result, _ = self._group_with_optional_reduction(
            df,
            key_columns,
            value_column=self.target_column,
            size_col=size_col,
            count_col=count_col,
            aggregate_name=self._cudf_aggregate_name,
            output_column=acc_col,
            output_dtype=target_dtype,
        )
        self._apply_null_reduction_semantics(
            result, size_col=size_col, count_col=count_col, output_col=acc_col
        )

        return result[list(key_columns) + [acc_col]]

    def gpu_partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return preferred dtypes for partial accumulator columns."""
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {
            accumulator_columns[0]: self._target_accumulator_dtype(
                df, input_schema=input_schema
            )
        }

    def gpu_final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and the final output column.

        The final aggregate is called on each block after the shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.
            output_name: The name of the output column.
            accumulator_prefix: The prefix for intermediate accumulator columns.
        Returns:
            A cuDF DataFrame with key columns and the final output column.
        """
        acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
        size_col = f"{accumulator_prefix}_partial_size"
        count_col = f"{accumulator_prefix}_partial_count"
        acc_dtype = _cudf_column_dtype(df, acc_col)

        result, _ = self._group_with_optional_reduction(
            df,
            key_columns,
            value_column=acc_col,
            size_col=size_col,
            count_col=count_col,
            aggregate_name=self._cudf_aggregate_name,
            output_column=output_name,
            output_dtype=acc_dtype,
        )
        self._apply_null_reduction_semantics(
            result, size_col=size_col, count_col=count_col, output_col=output_name
        )

        return result[list(key_columns) + [output_name]]

    def gpu_final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        """Return the CPU Arrow types for the final output column."""
        plan_dtype = self.source_dtype
        runtime_dtype = _schema_column_dtype(input_schema, self.target_column)

        # Use the plan-time dtype when it is concrete; otherwise fall back to
        # runtime schema so merge_input_schema can upgrade plan-time null types.
        if plan_dtype is not None and not plan_dtype.is_null_type():
            effective_dtype = plan_dtype
        else:
            effective_dtype = runtime_dtype

        if effective_dtype is None or not effective_dtype.is_null_type():
            return {}

        return {output_name: pa.null()}

    def _target_accumulator_dtype(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Optional[DataType]:
        """Return the preferred GPU dtype for the target accumulator column.

        This specifies widened reduction dtypes for common dtypes to avoid overflows."""
        dtype = self.source_dtype
        if dtype is not None:
            reduction_dtype = _reduction_dtype(dtype, self._cudf_aggregate_name)
            if reduction_dtype is not None:
                return reduction_dtype
            return _resolve_null_dtype(dtype, self._cudf_aggregate_name)

        dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is not None:
            reduction_dtype = _reduction_dtype(dtype, self._cudf_aggregate_name)
            if reduction_dtype is not None:
                return reduction_dtype
            return _resolve_null_dtype(dtype, self._cudf_aggregate_name)

        dtype = _cudf_column_dtype(df, self.target_column)
        return _resolve_null_dtype(dtype, self._cudf_aggregate_name)

    def gpu_final_output_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return preferred GPU dtypes for final output columns on empty partitions."""
        acc_dtype = _cudf_column_dtype(
            df, self.gpu_accumulator_columns(accumulator_prefix)[0]
        )
        if acc_dtype is None:
            acc_dtype = self._target_accumulator_dtype(
                df,
                input_schema=input_schema,
            )
        return {output_name: acc_dtype}

    def _group_size_and_count(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        target_column: str,
        *,
        size_col: str,
        count_col: str,
    ) -> Tuple[cudf.DataFrame, Optional[DataType]]:
        """Return a cuDF DataFrame with group size (including nulls) and count
        (excluding nulls) columns."""
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)
        return result, count_dtype

    def _apply_null_reduction_semantics(
        self,
        result: cudf.DataFrame,
        *,
        size_col: str,
        count_col: str,
        output_col: str,
    ) -> None:
        """Handle null reduction semantics for the output column."""
        if self.ignore_nulls:
            null_mask = result[count_col] == 0
        else:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, output_col] = None

    def _group_with_optional_reduction(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        value_column: str,
        size_col: str,
        count_col: str,
        aggregate_name: str,
        output_column: str,
        output_dtype: Optional[DataType],
    ) -> Tuple[cudf.DataFrame, Optional[DataType]]:
        """Group by key columns and aggregate the value column if the group counts are
        not all zero."""
        result, count_dtype = self._group_size_and_count(
            df,
            key_columns,
            value_column,
            size_col=size_col,
            count_col=count_col,
        )
        if _all_counts_zero(result, count_col):
            result[output_column] = None
            _cast_column_to_dtype(result, output_column, output_dtype)
        else:
            column_group = df.groupby(list(key_columns), dropna=False)[value_column]
            # Note: cuDF groupby/sum does not support min_count (rapidsai/cudf#9009)
            # Null-group semantics are handled via size/count columns instead.
            aggregated = getattr(column_group, aggregate_name)()
            aggregated = aggregated.reset_index()
            aggregated = aggregated.rename(
                columns={aggregated.columns[-1]: output_column}
            )
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, output_column, output_dtype)
        return result, count_dtype


class GPUCount(_BuiltinGPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Count`."""

    _cudf_aggregate_name = "count"

    def __init__(self, agg: Count, *, source_dtype: Optional[DataType] = None) -> None:
        super().__init__(agg, source_dtype=source_dtype)

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {accumulator_columns[0]: 0}

    def gpu_partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {accumulator_columns[0]: DataType.from_numpy("int64")}

    def gpu_final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        return {}

    def gpu_partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
        grouped = df.groupby(list(key_columns), dropna=False)
        if self.target_column is None or not self.ignore_nulls:
            result = grouped.size().reset_index()
            return result.rename(columns={result.columns[-1]: acc_col})

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: acc_col})
        count_dtype = _cudf_column_dtype(sizes, acc_col)

        counts = grouped[self.target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: acc_col})

        result = sizes[list(key_columns)].merge(
            counts, on=list(key_columns), how="left"
        )
        _fill_missing_count(result, acc_col, count_dtype)
        return result[list(key_columns) + [acc_col]]

    def gpu_final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
    ) -> cudf.DataFrame:
        acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        result = result.rename(columns={result.columns[-1]: output_name})
        return result[list(key_columns) + [output_name]]

    def gpu_final_output_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("int64")}


class GPUSum(_BuiltinGPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Sum`."""

    _cudf_aggregate_name = "sum"

    def __init__(self, agg: Sum, *, source_dtype: Optional[DataType] = None) -> None:
        super().__init__(agg, source_dtype=source_dtype)

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {accumulator_columns[0]: None if self.ignore_nulls else 0}


class GPUMin(_BuiltinGPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Min`."""

    _cudf_aggregate_name = "min"

    def __init__(self, agg: Min, *, source_dtype: Optional[DataType] = None) -> None:
        super().__init__(agg, source_dtype=source_dtype)

    def _zero_factory(self) -> Any:
        return float("+inf")

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {accumulator_columns[0]: None if self.ignore_nulls else float("+inf")}


class GPUMax(_BuiltinGPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Max`."""

    _cudf_aggregate_name = "max"

    def __init__(self, agg: Max, *, source_dtype: Optional[DataType] = None) -> None:
        super().__init__(agg, source_dtype=source_dtype)

    def _zero_factory(self) -> Any:
        return float("-inf")

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        return {accumulator_columns[0]: None if self.ignore_nulls else float("-inf")}


class GPUMean(_BuiltinGPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Mean`."""

    _cudf_aggregate_name = "mean"

    def __init__(self, agg: Mean, *, source_dtype: Optional[DataType] = None) -> None:
        super().__init__(agg, source_dtype=source_dtype)

    def _zero_factory(self) -> Any:
        return [0, 0]

    def gpu_accumulator_columns(self, accumulator_prefix: str) -> Tuple[str, ...]:
        return (
            f"{accumulator_prefix}_sum",
            f"{accumulator_prefix}_count",
            f"{accumulator_prefix}_null_count",
        )

    def gpu_empty_global_partial_values(
        self, accumulator_prefix: str
    ) -> Dict[str, Any]:
        sum_col, count_col, null_count_col = self.gpu_accumulator_columns(
            accumulator_prefix
        )
        return {sum_col: None, count_col: 0, null_count_col: 0}

    def gpu_partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        sum_col, count_col, null_count_col = self.gpu_accumulator_columns(
            accumulator_prefix
        )
        return {
            sum_col: self._target_accumulator_dtype(df, input_schema=input_schema),
            count_col: DataType.from_numpy("int64"),
            null_count_col: DataType.from_numpy("int64"),
        }

    def gpu_partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        assert self.target_column is not None

        accumulator_columns = self.gpu_accumulator_columns(accumulator_prefix)
        sum_col, count_col, null_count_col = accumulator_columns
        size_col = f"{accumulator_prefix}_size"
        target_dtype = self._target_accumulator_dtype(df, input_schema=input_schema)

        result, count_dtype = self._group_with_optional_reduction(
            df,
            key_columns,
            value_column=self.target_column,
            size_col=size_col,
            count_col=count_col,
            aggregate_name="sum",
            output_column=sum_col,
            output_dtype=target_dtype,
        )

        result[null_count_col] = result[size_col] - result[count_col]
        _cast_column_to_dtype(result, null_count_col, count_dtype)

        if not self.ignore_nulls:
            result.loc[result[null_count_col] > 0, sum_col] = None

        return result[list(key_columns) + list(accumulator_columns)]

    def gpu_final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        *,
        output_name: str,
        accumulator_prefix: str,
    ) -> cudf.DataFrame:
        sum_col, count_col, null_count_col = self.gpu_accumulator_columns(
            accumulator_prefix
        )
        final_sum_col = f"{accumulator_prefix}_final_sum"
        final_count_col = f"{accumulator_prefix}_final_count"
        final_null_count_col = f"{accumulator_prefix}_final_null_count"
        sum_dtype = _cudf_column_dtype(df, sum_col)

        accumulator_columns = [count_col, null_count_col, sum_col]
        aggregated = (
            df.groupby(list(key_columns), dropna=False)[accumulator_columns]
            .sum()
            .reset_index()
        )
        result = aggregated.rename(
            columns={
                count_col: final_count_col,
                null_count_col: final_null_count_col,
                sum_col: final_sum_col,
            }
        )
        _fill_missing_reduction(result, final_sum_col, sum_dtype)

        result[output_name] = result[final_sum_col] / result[final_count_col]

        null_mask = result[final_count_col] == 0
        if not self.ignore_nulls:
            null_mask = null_mask | (result[final_null_count_col] > 0)
        result.loc[null_mask, output_name] = None

        return result[list(key_columns) + [output_name]]

    def gpu_final_output_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("float64")}


class GPUAggregationPlan:
    """Executable GPU aggregation plan shared by the driver and GPU actors.

    Args:
        key_columns: The key columns to group by.
        gpu_aggregates: The GPU aggregate functions to apply.
        output_names: The names of the output columns.
        accumulator_prefixes: The prefixes for intermediate accumulator columns.
        input_schema: The schema of the input data.
    """

    def __init__(
        self,
        key_columns: Tuple[str, ...],
        gpu_aggregates: Tuple[GPUAggregateFn, ...],
        output_names: Tuple[str, ...],
        accumulator_prefixes: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> None:
        if not (len(gpu_aggregates) == len(output_names) == len(accumulator_prefixes)):
            raise ValueError("GPU aggregation plan entries must have matching lengths.")

        self._key_columns = key_columns
        self._gpu_aggregates = gpu_aggregates
        self._output_names = output_names
        self._accumulator_prefixes = accumulator_prefixes
        self._input_schema = input_schema
        self._is_global = len(key_columns) == 0
        global_key = _GLOBAL_AGGREGATE_KEY
        required_columns = {
            column
            for agg in gpu_aggregates
            if (column := agg.get_target_column()) is not None
        }
        while global_key in required_columns:
            global_key = f"_{global_key}"
        self._shuffle_key_columns = (global_key,) if self._is_global else key_columns

    def _iter_aggregations(
        self,
    ) -> Iterator[Tuple[GPUAggregateFn, str, str]]:
        return zip(self._gpu_aggregates, self._output_names, self._accumulator_prefixes)

    @property
    def shuffle_key_columns(self) -> Tuple[str, ...]:
        return self._shuffle_key_columns

    @property
    def output_names(self) -> Tuple[str, ...]:
        return self._output_names

    @property
    def required_columns(self) -> Tuple[str, ...]:
        columns = list(self._key_columns)
        for agg in self._gpu_aggregates:
            target_column = agg.get_target_column()
            if target_column is not None and target_column not in columns:
                columns.append(target_column)
        return tuple(columns)

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        columns: List[str] = []
        for agg, _, accumulator_prefix in self._iter_aggregations():
            columns.extend(agg.gpu_accumulator_columns(accumulator_prefix))
        return tuple(columns)

    def _partial_output_dtypes(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        dtypes: Dict[str, Optional[DataType]] = {}
        for column in key_columns:
            if column not in df.columns:
                continue
            dtype = self._effective_column_dtype(column, input_schema)
            if dtype is not None:
                dtype = _resolve_null_dtype(dtype, "key")
            else:
                dtype = _cudf_column_dtype(df, column)
            dtypes[column] = dtype
        for agg, _, accumulator_prefix in self._iter_aggregations():
            dtypes.update(
                to_datatypes(
                    agg.gpu_partial_accumulator_dtypes(
                        df,
                        accumulator_prefix,
                        input_schema=input_schema,
                    )
                )
            )
        return dtypes

    def _normalize_partial_output(
        self,
        result: cudf.DataFrame,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        for column, dtype in self._partial_output_dtypes(
            df, key_columns, input_schema=input_schema
        ).items():
            _cast_column_to_dtype(result, column, dtype)
        return result

    def merge_input_schema(
        self, current: Optional[pa.Schema], observed: Optional[Schema]
    ) -> Optional[pa.Schema]:
        """Merge an observed block schema into the runtime input schema."""
        if observed is None:
            return current

        fields: Dict[str, pa.DataType] = {}
        if current is not None:
            fields.update({field.name: field.type for field in current})

        for column in self.required_columns:
            observed_dtype = _schema_column_dtype(observed, column)
            if observed_dtype is None:
                continue

            try:
                observed_arrow_dtype = observed_dtype.to_arrow_dtype()
            except (AssertionError, TypeError, pa.ArrowNotImplementedError):
                continue

            current_dtype = fields.get(column)
            if current_dtype is None or pa.types.is_null(current_dtype):
                fields[column] = observed_arrow_dtype
            elif not current_dtype.equals(observed_arrow_dtype):
                # Unify schemas using arrow_ops
                try:
                    from ray.data._internal.arrow_ops.transform_pyarrow import (
                        unify_schemas,
                    )

                    fields[column] = (
                        unify_schemas(
                            [
                                pa.schema([(column, current_dtype)]),
                                pa.schema([(column, observed_arrow_dtype)]),
                            ],
                            promote_types=True,
                        )
                        .field(column)
                        .type
                    )
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    pass

        if not fields:
            return current

        ordered_names = [column for column in self.required_columns if column in fields]
        ordered_names.extend(name for name in fields if name not in ordered_names)
        return pa.schema([(name, fields[name]) for name in ordered_names])

    def _effective_input_schema(
        self, runtime_input_schema: Optional[Schema] = None
    ) -> Optional[Schema]:
        return (
            runtime_input_schema
            if runtime_input_schema is not None
            else self._input_schema
        )

    def _effective_column_dtype(
        self, column: str, runtime_input_schema: Optional[Schema] = None
    ) -> Optional[DataType]:
        dtype = _schema_column_dtype(self._input_schema, column)
        if dtype is not None:
            return dtype
        return _schema_column_dtype(runtime_input_schema, column)

    def _final_arrow_types(
        self, input_schema: Optional[Schema] = None
    ) -> Dict[str, pa.DataType]:
        input_schema = self._effective_input_schema(input_schema)
        types: Dict[str, pa.DataType] = {}
        for agg, output_name, _ in self._iter_aggregations():
            types.update(
                agg.gpu_final_arrow_types(
                    output_name,
                    input_schema=input_schema,
                )
            )
        return types

    def _final_output_dtypes(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        input_schema = self._effective_input_schema(input_schema)
        dtypes: Dict[str, Optional[DataType]] = {}

        if not self._is_global:
            for column in self._shuffle_key_columns:
                dtype = self._effective_column_dtype(column, input_schema)
                if dtype is not None:
                    dtype = _resolve_null_dtype(dtype, "key")
                else:
                    dtype = _cudf_column_dtype(df, column)
                dtypes[column] = dtype

        for agg, output_name, accumulator_prefix in self._iter_aggregations():
            dtypes.update(
                to_datatypes(
                    agg.gpu_final_output_dtypes(
                        df,
                        output_name,
                        accumulator_prefix,
                        input_schema=input_schema,
                    )
                )
            )
        return dtypes

    def normalize_output_arrow(
        self,
        table: pa.Table,
        input_schema: Optional[Schema] = None,
    ) -> pa.Table:
        arrow_types = self._final_arrow_types(input_schema)
        if not arrow_types:
            return table

        columns = []
        for column_name in table.column_names:
            if column_name in arrow_types:
                columns.append(pa.nulls(table.num_rows, type=arrow_types[column_name]))
            else:
                columns.append(table[column_name])
        return pa.table(columns, names=table.column_names)

    def partial_aggregate(
        self, df: cudf.DataFrame, input_schema: Optional[Schema] = None
    ) -> cudf.DataFrame:
        import cudf as cudf_module

        if self._is_global:
            df = df.copy(deep=False)
            df[self._shuffle_key_columns[0]] = 0

        key_columns = self._shuffle_key_columns
        if len(df) == 0:
            if self._is_global:
                values: Dict[str, List[Any]] = {key_columns[0]: [0]}
                for agg, _, accumulator_prefix in self._iter_aggregations():
                    for column, value in agg.gpu_empty_global_partial_values(
                        accumulator_prefix
                    ).items():
                        values[column] = [value]
                result = cudf_module.DataFrame(values)[
                    list(key_columns) + list(self.accumulator_columns)
                ]
                for column, dtype in self._partial_output_dtypes(
                    df, key_columns, input_schema=input_schema
                ).items():
                    _cast_column_to_dtype(result, column, dtype)
                return result
            return _empty_dataframe(
                cudf_module,
                list(key_columns) + list(self.accumulator_columns),
                dtypes=self._partial_output_dtypes(
                    df, key_columns, input_schema=input_schema
                ),
            )

        result = None
        for agg, output_name, accumulator_prefix in self._iter_aggregations():
            partial = agg.gpu_partial_aggregate(
                df,
                key_columns,
                output_name=output_name,
                accumulator_prefix=accumulator_prefix,
                input_schema=input_schema,
            )
            result = (
                partial
                if result is None
                else result.merge(partial, on=list(key_columns), how="outer")
            )

        assert result is not None
        result = self._normalize_partial_output(
            result, df, key_columns, input_schema=input_schema
        )
        return result[list(key_columns) + list(self.accumulator_columns)]

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        import cudf as cudf_module

        key_columns = self._shuffle_key_columns
        output_columns = ([] if self._is_global else list(key_columns)) + list(
            self.output_names
        )

        if len(df) == 0:
            return _empty_dataframe(
                cudf_module,
                output_columns,
                dtypes=self._final_output_dtypes(
                    df,
                    input_schema=input_schema,
                ),
            )

        result = None
        for agg, output_name, accumulator_prefix in self._iter_aggregations():
            finalized = agg.gpu_final_aggregate(
                df,
                key_columns,
                output_name=output_name,
                accumulator_prefix=accumulator_prefix,
            )
            result = (
                finalized
                if result is None
                else result.merge(finalized, on=list(key_columns), how="outer")
            )

        assert result is not None
        if self._is_global:
            result = result.drop(columns=[self._shuffle_key_columns[0]])

        return result[output_columns]


def _get_builtin_gpu_aggregate_fn(
    agg: AggregateFn,
    *,
    input_schema: Optional[Schema] = None,
) -> Optional[GPUAggregateFn]:
    """Get a GPU equivalent function for a built-in aggregation function.

    Args:
        agg: The built-in aggregation function to convert.
        input_schema: The schema of the input data.

    Returns:
        A GPU aggregate function, or None if the aggregation function is not supported.
    """
    if not isinstance(agg, AggregateFnV2):
        return None

    target_column = agg.get_target_column()
    source_dtype = _schema_column_dtype(input_schema, target_column)

    if isinstance(agg, Count):
        return GPUCount(agg, source_dtype=source_dtype)

    if target_column is None:
        return None

    if isinstance(agg, Sum):
        return GPUSum(agg, source_dtype=source_dtype)
    if isinstance(agg, Min):
        return GPUMin(agg, source_dtype=source_dtype)
    if isinstance(agg, Max):
        return GPUMax(agg, source_dtype=source_dtype)
    if isinstance(agg, Mean):
        return GPUMean(agg, source_dtype=source_dtype)

    return None


def build_gpu_aggregation_plan(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[AggregateFn, ...],
    input_schema: Optional[Schema] = None,
) -> Optional[GPUAggregationPlan]:
    """Build a GPU aggregation plan.

    Args:
        key_columns: The key columns to group by.
        aggregation_fns: The aggregation functions to apply.
        input_schema: The schema of the input data.

    Returns:
        A GPU aggregation plan, or None if the aggregation plan is not supported.
    """
    if not aggregation_fns:
        # No aggregation functions, no plan needed.
        return None
    if key_columns and not all(
        _schema_column_dtype(input_schema, column) is not None for column in key_columns
    ):
        # Missing key columns in the input schema, fallback to CPU.
        return None

    resolved_names = _resolve_aggregation_names(aggregation_fns)
    gpu_aggregates: List[GPUAggregateFn] = []
    accumulator_prefixes: List[str] = []

    for index, agg in enumerate(aggregation_fns):
        accumulator_prefix = f"__ray_gpu_agg_{index}"
        gpu_aggregate = (
            agg
            if isinstance(agg, GPUAggregateFn)
            else _get_builtin_gpu_aggregate_fn(agg, input_schema=input_schema)
        )
        if gpu_aggregate is None:
            return None
        gpu_aggregates.append(gpu_aggregate)
        accumulator_prefixes.append(accumulator_prefix)

    return GPUAggregationPlan(
        key_columns,
        tuple(gpu_aggregates),
        tuple(resolved_names),
        tuple(accumulator_prefixes),
        input_schema=input_schema,
    )


@ray.remote(num_gpus=1)
class GPUHashAggregateActor:
    """One GPU rank for hash shuffle plus aggregate."""

    def __init__(
        self,
        nranks: int,
        total_nparts: int,
        aggregation_plan: GPUAggregationPlan,
        rmm_pool_size: Optional[int | str] = None,
        spill_memory_limit: Optional[int | str] = "auto",
    ) -> None:
        from ray.data._internal.gpu_shuffle.rapidsmpf_backend import (
            BulkRapidsMPFShuffler,
        )

        self._aggregation_plan = aggregation_plan
        self._shuffler = BulkRapidsMPFShuffler(
            nranks=nranks,
            total_nparts=total_nparts,
            shuffle_on=list(aggregation_plan.shuffle_key_columns),
            rmm_pool_size=rmm_pool_size,
            spill_memory_limit=spill_memory_limit,
        )
        self._shuffle_columns: Optional[List[str]] = None
        self._runtime_input_schema: Optional[pa.Schema] = (
            aggregation_plan._input_schema
            if isinstance(aggregation_plan._input_schema, pa.Schema)
            else None
        )

    def setup_root(self) -> Tuple[int, bytes]:
        logger.info("UCXX setup_root starting on GPU hash aggregate rank 0.")
        t0 = time.perf_counter()
        result = self._shuffler.setup_root()
        elapsed = time.perf_counter() - t0
        logger.info(
            "UCXX setup_root completed in %.2fs for GPU hash aggregate rank %d.",
            elapsed,
            result[0],
        )
        return result

    def setup_worker(self, root_address: bytes) -> None:
        logger.info(
            "UCXX setup_worker starting for GPU hash aggregate "
            "(root_address=%d bytes).",
            len(root_address),
        )
        t0 = time.perf_counter()
        self._shuffler.setup_worker(root_address)
        elapsed = time.perf_counter() - t0
        logger.info("UCXX setup_worker completed in %.2fs.", elapsed)

    def insert_batch(self, block: Block) -> int:
        import cudf

        table = BlockAccessor.for_block(block).to_arrow()
        required_columns = self._aggregation_plan.required_columns
        if required_columns:
            projected_table = table.select(list(required_columns))
            df = cudf.DataFrame.from_arrow(projected_table)
        else:
            df = cudf.DataFrame(index=range(table.num_rows))

        self._runtime_input_schema = self._aggregation_plan.merge_input_schema(
            self._runtime_input_schema,
            table.schema,
        )
        partial = self._aggregation_plan.partial_aggregate(
            df,
            input_schema=self._runtime_input_schema,
        )
        if self._shuffle_columns is None:
            self._shuffle_columns = list(partial.columns)

        self._shuffler.insert_chunk(table=partial, column_names=self._shuffle_columns)
        return table.num_rows

    def finish_and_extract(self) -> Iterator[pa.Table | bytes]:
        self._shuffler.insert_finished()

        import cudf
        from rapidsmpf.utils.cudf import pylibcudf_to_cudf_dataframe

        self._shuffle_columns = self._shuffle_columns or list(
            self._aggregation_plan.shuffle_key_columns
            + self._aggregation_plan.accumulator_columns
        )

        for partition_id, partition in self._shuffler.extract():
            exec_stats_builder = BlockExecStats.builder()
            if partition.num_columns() == 0:
                cdf = cudf.DataFrame()
            else:
                cdf = pylibcudf_to_cudf_dataframe(
                    partition, column_names=self._shuffle_columns
                ).copy(deep=True)

            output_df = self._aggregation_plan.final_aggregate(
                cdf,
                input_schema=self._runtime_input_schema,
            )
            block = output_df.to_arrow(preserve_index=False)
            block = self._aggregation_plan.normalize_output_arrow(
                block, input_schema=self._runtime_input_schema
            )

            existing_metadata = block.schema.metadata or {}
            tagged_schema = block.schema.with_metadata(
                {**existing_metadata, _GPU_PARTITION_ID_KEY: str(partition_id).encode()}
            )
            exec_stats = exec_stats_builder.build()
            stats = yield block
            if stats:
                object.__setattr__(
                    exec_stats, "block_ser_time_s", stats.object_creation_dur_s
                )
            block_meta = BlockMetadataWithSchema.from_block(
                block, block_exec_stats=exec_stats
            )
            bm = BlockMetadataWithSchema.from_metadata(
                block_meta.metadata, schema=tagged_schema
            )
            yield pickle.dumps(bm)


class GPUHashAggregateOperator(GPUShuffleOperator):
    """GPU-native hash aggregate using RAPIDS MPF for the shuffle stage."""

    def __init__(
        self,
        data_context: DataContext,
        input_op: PhysicalOperator,
        key_columns: Tuple[str, ...],
        aggregation_plan: GPUAggregationPlan,
        *,
        num_partitions: Optional[int] = None,
    ) -> None:
        if aggregation_plan is None:
            raise ValueError(
                "GPUHashAggregateOperator received unsupported aggregations."
            )

        nranks = _derive_num_gpu_ranks(data_context)
        target_num_partitions = (
            num_partitions
            if len(key_columns) > 0 and num_partitions is not None
            else nranks
            if len(key_columns) == 0
            else data_context.default_hash_shuffle_parallelism
        )
        # rapidsmpf requires total_nparts >= nranks
        target_num_partitions = max(target_num_partitions, nranks)

        rank_pool = GPURankPool(
            nranks=nranks,
            total_nparts=target_num_partitions,
            setup_timeout_s=data_context.gpu_shuffle_setup_timeout_s,
            actor_cls_factory=lambda: GPUHashAggregateActor,
            actor_kwargs={
                "aggregation_plan": aggregation_plan,
                "rmm_pool_size": data_context.gpu_shuffle_rmm_pool_size,
                "spill_memory_limit": data_context.gpu_shuffle_spill_memory_limit,
            },
            log_label="GPUHashAggregatePool",
            label_selector=data_context.execution_options.label_selector,
        )

        super().__init__(
            input_op,
            data_context,
            key_columns=aggregation_plan.shuffle_key_columns,
            columns=None,
            num_partitions=target_num_partitions,
            should_sort=False,
            name=(
                f"GPUHashAggregate(key_columns={key_columns}, "
                f"num_partitions={target_num_partitions})"
            ),
            nranks=nranks,
            rank_pool=rank_pool,
        )

        self._aggregation_plan = aggregation_plan

    def get_sub_progress_bar_names(self) -> List[str]:
        return ["GPU Shuffle", "GPU Aggregation"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "GPU Shuffle":
            self._shuffle_bar = pg
        elif name == "GPU Aggregation":
            self._reduce_bar = pg

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        shuffle_name = f"{self._name}_shuffle"
        aggregate_name = f"{self._name}_aggregate"
        return {
            shuffle_name: self._shuffled_blocks_stats,
            aggregate_name: self._output_blocks_stats,
        }
