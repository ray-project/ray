from __future__ import annotations

import logging
import pickle
import time
import types
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
)

import numpy as np
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

if typing.TYPE_CHECKING:
    import cudf
    from pandas.api.extensions import ExtensionDtype

    from ray.data._internal.progress.base_progress import BaseProgressBar

    # Column / cast dtypes: Arrow schemas, Pandas block schemas, and cuDF columns.
    DTypeLike: TypeAlias = (
        str | pa.DataType | np.dtype[np.generic] | ExtensionDtype | cudf.dtype
    )
else:
    DTypeLike: TypeAlias = str | pa.DataType | np.dtype[np.generic]


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


def _groupby(
    df: cudf.DataFrame, key_columns: Tuple[str, ...]
) -> "cudf.core.groupby.groupby.DataFrameGroupBy":
    """Group by key columns, keeping nulls to match Ray's behavior.

    Note that cuDF defaults to dropna=True."""
    return df.groupby(list(key_columns), dropna=False)


def _group_size(
    df: cudf.DataFrame, key_columns: Tuple[str, ...], output_column: str
) -> cudf.DataFrame:
    """Group by key columns and return the size of each group."""
    grouped = _groupby(df, key_columns)
    out = grouped.size().reset_index()
    return out.rename(columns={out.columns[-1]: output_column})


def _group_count(
    df: cudf.DataFrame,
    key_columns: Tuple[str, ...],
    target_column: str,
    output_column: str,
) -> cudf.DataFrame:
    """Group by key columns and return the count of each group."""
    grouped = _groupby(df, key_columns)
    out = grouped[target_column].count().reset_index()
    return out.rename(columns={out.columns[-1]: output_column})


def _group_aggregate(
    df: cudf.DataFrame,
    key_columns: Tuple[str, ...],
    target_column: str,
    aggregate_name: str,
    output_column: str,
) -> cudf.DataFrame:
    """Group by key columns and aggregate the target column for each group using the
    specified aggregation.

    Args:
        df: The input cuDF DataFrame.
        key_columns: The key columns to group by.
        target_column: The column to aggregate.
        aggregate_name: The name of the aggregation to use.
        output_column: The name of the final output column.

    Returns:
        A cuDF DataFrame with the key columns and the final output column.
    """
    grouped = _groupby(df, key_columns)
    column_group = grouped[target_column]

    if aggregate_name == "sum":
        try:
            # try to match Ray/Pandas behavior
            aggregated = column_group.sum(min_count=1)
        except (TypeError, NotImplementedError):
            # fallback if min_count is not supported
            aggregated = column_group.sum()
    else:
        # all other aggregations
        aggregated = getattr(column_group, aggregate_name)()

    out = aggregated.reset_index()
    return out.rename(columns={out.columns[-1]: output_column})


def _get_column_dtype(df: cudf.DataFrame, column: str) -> Optional[DTypeLike]:
    """Get the dtype of a column from a ``cudf.DataFrame``.

    Returns None if the column is not found.
    """
    if column not in df.columns:
        return None
    return df[column].dtype


def _schema_column_dtype(
    schema: Optional[Schema], column: Optional[str]
) -> Optional[DTypeLike]:
    """Get the logical dtype of a column from a ``Schema`` (Arrow or Pandas).

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
        return schema.field(column).type

    if isinstance(schema, PandasBlockSchema):
        return schema.types[names.index(column)]

    return None


def _is_null_dtype(dtype: Optional[DTypeLike]) -> bool:
    """Check if a dtype is null-like (including PyArrow null type)."""
    if dtype is None:
        return False

    if isinstance(dtype, pa.DataType):
        return pa.types.is_null(dtype)

    return str(dtype) == "null"


def _is_boolean_dtype(dtype: Optional[DTypeLike]) -> bool:
    """True if ``dtype`` is boolean-like."""
    if dtype is None:
        return False

    if isinstance(dtype, pa.DataType):
        return pa.types.is_boolean(dtype)

    try:
        from pandas.api.types import is_bool_dtype

        return is_bool_dtype(dtype)
    except (ImportError, TypeError):
        return False


def _is_floating_dtype(dtype: Optional[DTypeLike]) -> bool:
    """True if ``dtype`` is floating-point."""
    if dtype is None:
        return False

    if isinstance(dtype, pa.DataType):
        return pa.types.is_floating(dtype)

    try:
        from pandas.api.types import is_float_dtype

        return is_float_dtype(dtype)
    except (ImportError, TypeError):
        return False


def _is_integer_dtype(dtype: Optional[DTypeLike]) -> bool:
    """True if ``dtype`` is integer-like, excluding booleans."""
    if dtype is None:
        return False

    if isinstance(dtype, pa.DataType):
        return pa.types.is_integer(dtype)

    try:
        from pandas.api.types import is_bool_dtype, is_integer_dtype

        return is_integer_dtype(dtype) and not is_bool_dtype(dtype)
    except (ImportError, TypeError):
        return False


def _is_uint64_dtype(dtype: Optional[DTypeLike]) -> bool:
    """True if ``dtype`` is uint64-like."""
    if dtype is None:
        return False

    if isinstance(dtype, pa.DataType):
        return pa.types.is_uint64(dtype)

    try:
        from pandas.api.types import is_unsigned_integer_dtype

        itemsize = getattr(dtype, "itemsize", None)
        if itemsize is None:
            itemsize = np.dtype(dtype).itemsize
        return is_unsigned_integer_dtype(dtype) and itemsize == 8
    except (ImportError, TypeError, ValueError):
        return False


def _reduction_dtype(
    dtype: Optional[DTypeLike], aggregation: str
) -> Optional[DTypeLike]:
    """Return the CPU aggregate path's scalar reduction dtype for common dtypes."""
    if dtype is None:
        return None

    if _is_null_dtype(dtype):
        return "float64" if aggregation == "mean" else "int64"

    if aggregation in ("sum", "mean"):
        if _is_boolean_dtype(dtype):
            return "int64"
        if _is_integer_dtype(dtype):
            return "uint64" if _is_uint64_dtype(dtype) else "int64"
        if _is_floating_dtype(dtype):
            return "float64"

    if aggregation in ("min", "max"):
        if _is_boolean_dtype(dtype):
            return "bool"
        if _is_integer_dtype(dtype):
            return "uint64" if _is_uint64_dtype(dtype) else "int64"
        if _is_floating_dtype(dtype):
            return "float64"

    return None


def _normalize_intermediate_dtype(
    dtype: Optional[DTypeLike], aggregation: str
) -> Optional[DTypeLike]:
    """Normalize an intermediate dtype to a canonical ``cudf.dtype`` string.

    Null dtypes are converted to ``float64`` for ``key`` and ``mean`` aggregations,
    and ``int64`` otherwise.
    """
    if isinstance(dtype, pa.DataType):
        if pa.types.is_null(dtype):
            return "float64" if aggregation in ("key", "mean") else "int64"
        if pa.types.is_int8(dtype):
            return "int8"
        if pa.types.is_int16(dtype):
            return "int16"
        if pa.types.is_int32(dtype):
            return "int32"
        if pa.types.is_int64(dtype):
            return "int64"
        if pa.types.is_uint8(dtype):
            return "uint8"
        if pa.types.is_uint16(dtype):
            return "uint16"
        if pa.types.is_uint32(dtype):
            return "uint32"
        if pa.types.is_uint64(dtype):
            return "uint64"
        if pa.types.is_float32(dtype):
            return "float32"
        if pa.types.is_float64(dtype):
            return "float64"
        if pa.types.is_boolean(dtype):
            return "bool"
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            return "str"

    if _is_null_dtype(dtype):
        return "float64" if aggregation in ("key", "mean") else "int64"
    return dtype


def _cast_column_to_dtype(
    df: cudf.DataFrame, column: str, dtype: Optional[DTypeLike]
) -> None:
    """Cast a ``cudf.DataFrame`` column to specified dtype in place."""
    if dtype is None or column not in df.columns:
        return

    try:
        df[column] = df[column].astype(dtype)
    except (TypeError, ValueError, NotImplementedError):
        # fallback for handling all-null columns
        try:
            import cudf

            is_all_null = len(df) == 0 or bool(df[column].isnull().all())
            if isinstance(df, cudf.DataFrame) and is_all_null:
                df[column] = cudf.Series([None] * len(df), dtype=dtype)
        except (ImportError, TypeError, ValueError, NotImplementedError):
            pass


def _fill_missing_count(
    result: cudf.DataFrame, count_column: str, dtype: Optional[DTypeLike] = None
) -> None:
    """Fill missing counts with 0 and cast to specified dtype."""
    if count_column not in result.columns:
        result[count_column] = 0
    else:
        result[count_column] = result[count_column].fillna(0)
    _cast_column_to_dtype(result, count_column, dtype)


def _fill_missing_reduction(
    result: cudf.DataFrame, reduction_column: str, dtype: Optional[DTypeLike] = None
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
    dtypes: Optional[Dict[str, Optional[DTypeLike]]] = None,
) -> cudf.DataFrame:
    """Create an empty ``cudf.DataFrame`` with specified columns and dtypes."""
    dtypes = dtypes or {}
    df = cudf_module.DataFrame()
    for column in columns:
        df[column] = []
        _cast_column_to_dtype(df, column, dtypes.get(column))
    return df


class GPUAggregationSpec(ABC):
    """GPU aggregation contract for custom ``AggregateFnV2`` implementations.

    A custom ``AggregateFnV2`` can opt in to GPU hash aggregation by implementing
    ``gpu_aggregate_spec(output_name, accumulator_prefix)`` and returning an
    instance of this class.

    Attributes:
        output_name: Name of the column created by this aggregation.
        required_columns: Input columns that are required for the aggregation.
        accumulator_columns: Intermediate column names produced by
            :meth:`partial_aggregate` and consumed by :meth:`final_aggregate`.
    """

    output_name: str
    required_columns: Tuple[str, ...]
    accumulator_columns: Tuple[str, ...]

    @abstractmethod
    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and accumulator columns.

        The partial aggregate is called for each block of the dataset before the
        shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.
            input_schema: The input schema.

        Returns:
            A cuDF DataFrame with key columns and accumulator columns.
        """
        ...

    @abstractmethod
    def final_aggregate(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and the final output column.

        The final aggregate is called for each block of the dataset after the
        shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.

        Returns:
            A cuDF DataFrame with key columns and the final output column.
        """

        ...

    def empty_global_partial_values(self) -> Dict[str, Any]:
        """Return accumulator values for an empty global-aggregation input block."""
        return {column: None for column in self.accumulator_columns}

    def partial_accumulator_dtypes(
        self, df: cudf.DataFrame, input_schema: Optional[Schema] = None
    ) -> Dict[str, Any]:
        """Return preferred dtypes for partial accumulator columns."""
        return {column: None for column in self.accumulator_columns}

    def final_arrow_types(self, input_schema: Optional[Schema] = None) -> Dict[str, Any]:
        """Return the CPU Arrow types for the final output columns.

        This is usually one entry per aggregation/spec.
        The plan merges multiple aggregations/specs into one dict.
        """
        return {}

    def final_output_dtypes(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Any]:
        """Return preferred GPU dtypes for final output columns on empty partitions.

        This is usually one entry per aggregation/spec.
        The plan merges multiple aggregations/specs into one dict.
        """
        return {}


class BuiltinGPUAggregationSpec(GPUAggregationSpec):
    """GPU aggregation specification for builtin aggregations.

    Supported builtin aggregations: count, sum, min, max, mean.

    Args:
        aggregation: The kind of builtin aggregation (count, sum, min, max, mean).
        output_name: Name of the column created by this aggregation.
        target_column: Name of the column to aggregate.
        ignore_nulls: Whether to ignore null values during aggregation.
        accumulator_prefix: Prefix for the intermediate accumulator columns.
        source_dtype: Source dtype of the target column. If not provided, it will
            be inferred from the input schema.
    """

    def __init__(
        self,
        aggregation: Literal["count", "sum", "min", "max", "mean"],
        *,
        output_name: str,
        target_column: Optional[str],
        ignore_nulls: bool,
        accumulator_prefix: str,
        source_dtype: Optional[DTypeLike] = None,
    ) -> None:
        self.aggregation = aggregation
        self.output_name = output_name
        self.target_column = target_column
        self.ignore_nulls = ignore_nulls
        self.accumulator_prefix = accumulator_prefix
        self.source_dtype = source_dtype

    @property
    def required_columns(self) -> Tuple[str, ...]:
        """Return the required columns (target column) for the builtin aggregation."""
        return (self.target_column,) if self.target_column is not None else tuple()

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        """Return the intermediate accumulator columns from partial aggregations.

        For ``mean`` aggregation, the accumulator columns are:
        - {accumulator_prefix}_sum
        - {accumulator_prefix}_count
        - {accumulator_prefix}_null_count

        For all other aggregations, the accumulator columns are:
        - {accumulator_prefix}_value
        """
        if self.aggregation == "mean":
            return (
                f"{self.accumulator_prefix}_sum",
                f"{self.accumulator_prefix}_count",
                f"{self.accumulator_prefix}_null_count",
            )
        return (f"{self.accumulator_prefix}_value",)

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and accumulator columns.

        The partial aggregate is called for each block of the dataset before the
        shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.
            input_schema: The input schema.

        Returns:
            A cuDF DataFrame with key columns and accumulator columns.
        """
        if self.aggregation == "count":
            return self._partial_count(df, key_columns)
        if self.aggregation == "mean":
            return self._partial_mean(df, key_columns, input_schema=input_schema)
        return self._partial_simple_reduction(
            df, key_columns, input_schema=input_schema
        )

    def empty_global_partial_values(self) -> Dict[str, Any]:
        """Return accumulator values for an empty global-aggregation input block."""
        if self.aggregation == "count":
            return {self.accumulator_columns[0]: 0}
        if self.aggregation == "sum":
            return {self.accumulator_columns[0]: None if self.ignore_nulls else 0}
        if self.aggregation == "min":
            return {
                self.accumulator_columns[0]: (
                    None if self.ignore_nulls else float("+inf")
                )
            }
        if self.aggregation == "max":
            return {
                self.accumulator_columns[0]: (
                    None if self.ignore_nulls else float("-inf")
                )
            }
        if self.aggregation == "mean":
            sum_col, count_col, null_count_col = self.accumulator_columns
            return {sum_col: None, count_col: 0, null_count_col: 0}
        return super().empty_global_partial_values()

    def partial_accumulator_dtypes(
        self, df: cudf.DataFrame, input_schema: Optional[Schema] = None
    ) -> Dict[str, Any]:
        """Return preferred dtypes for partial accumulator columns."""
        if self.aggregation == "count":
            return {self.accumulator_columns[0]: "int64"}
        if self.aggregation == "mean":
            sum_col, count_col, null_count_col = self.accumulator_columns
            return {
                sum_col: self._target_accumulator_dtype(df, input_schema=input_schema),
                count_col: "int64",
                null_count_col: "int64",
            }
        return {
            self.accumulator_columns[0]: self._target_accumulator_dtype(
                df, input_schema=input_schema
            )
        }

    def final_aggregate(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with key columns and the final output column.

        The final aggregate is called on each block after the shuffle.

        Args:
            df: The input cuDF DataFrame.
            key_columns: The key columns to group by.

        Returns:
            A cuDF DataFrame with key columns and the final output column.
        """
        if self.aggregation == "count":
            return self._final_count(df, key_columns)
        if self.aggregation == "mean":
            return self._final_mean(df, key_columns)
        return self._final_simple_reduction(df, key_columns)

    def final_arrow_types(self, input_schema: Optional[Schema] = None) -> Dict[str, Any]:
        """Return the CPU Arrow types for the final output column."""
        source_dtype = self.source_dtype
        if source_dtype is None:
            source_dtype = _schema_column_dtype(input_schema, self.target_column)

        if self.aggregation not in ("sum", "min", "max", "mean") or not _is_null_dtype(
            source_dtype
        ):
            return {}

        return {self.output_name: pa.null()}

    def _target_accumulator_dtype(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Any:
        """Return the preferred GPU dtype for the target accumulator column.

        This specifies widened reduction dtypes for common dtypes to avoid overflows."""
        dtype = self.source_dtype
        if dtype is not None:
            reduction_dtype = _reduction_dtype(dtype, self.aggregation)
            if reduction_dtype is not None:
                return reduction_dtype
            return _normalize_intermediate_dtype(dtype, self.aggregation)

        dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is not None:
            reduction_dtype = _reduction_dtype(dtype, self.aggregation)
            if reduction_dtype is not None:
                return reduction_dtype
            return _normalize_intermediate_dtype(dtype, self.aggregation)

        dtype = _get_column_dtype(df, self.target_column)
        return _normalize_intermediate_dtype(dtype, self.aggregation)

    def final_output_dtypes(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Any]:
        """Return preferred GPU dtypes for final output columns on empty partitions."""
        if self.aggregation == "count":
            return {self.output_name: "int64"}
        if self.aggregation == "mean":
            return {self.output_name: "float64"}

        acc_dtype = _get_column_dtype(df, self.accumulator_columns[0])
        if acc_dtype is None:
            acc_dtype = self._target_accumulator_dtype(
                df,
                input_schema=input_schema,
            )
        return {self.output_name: acc_dtype}

    def _group_size_and_count(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        target_column: str,
        *,
        size_col: str,
        count_col: str,
    ) -> Tuple[cudf.DataFrame, Any]:
        """Return a cuDF DataFrame with group size and count columns."""
        sizes = _group_size(df, key_columns, size_col)
        count_dtype = _get_column_dtype(sizes, size_col)
        counts = _group_count(df, key_columns, target_column, count_col)
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
        output_dtype: Optional[DTypeLike],
    ) -> Tuple[cudf.DataFrame, Any]:
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
            aggregated = _group_aggregate(
                df,
                key_columns,
                value_column,
                aggregate_name,
                output_column,
            )
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, output_column, output_dtype)
        return result, count_dtype

    def _partial_simple_reduction(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with partially aggregated/reduced values.

        This is called by ``partial_aggregate`` for all aggregations that are not
        ``mean`` or ``count``.
        """
        assert self.target_column is not None

        acc_col = self.accumulator_columns[0]
        size_col = f"{self.accumulator_prefix}_size"
        count_col = f"{self.accumulator_prefix}_count"
        target_dtype = self._target_accumulator_dtype(df, input_schema=input_schema)

        result, _ = self._group_with_optional_reduction(
            df,
            key_columns,
            value_column=self.target_column,
            size_col=size_col,
            count_col=count_col,
            aggregate_name=self.aggregation,
            output_column=acc_col,
            output_dtype=target_dtype,
        )
        self._apply_null_reduction_semantics(
            result, size_col=size_col, count_col=count_col, output_col=acc_col
        )

        return result[list(key_columns) + [acc_col]]

    def _final_simple_reduction(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with final aggregated/reduced values.

        This is called by ``final_aggregate`` for all aggregations that are not
        ``mean`` or ``count``.
        """
        acc_col = self.accumulator_columns[0]
        size_col = f"{self.accumulator_prefix}_partial_size"
        count_col = f"{self.accumulator_prefix}_partial_count"
        acc_dtype = _get_column_dtype(df, acc_col)

        result, _ = self._group_with_optional_reduction(
            df,
            key_columns,
            value_column=acc_col,
            size_col=size_col,
            count_col=count_col,
            aggregate_name=self.aggregation,
            output_column=self.output_name,
            output_dtype=acc_dtype,
        )
        self._apply_null_reduction_semantics(
            result, size_col=size_col, count_col=count_col, output_col=self.output_name
        )

        return result[list(key_columns) + [self.output_name]]

    def _partial_count(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with partial counts.

        This is called by ``partial_aggregate`` for ``count`` aggregations.
        """
        acc_col = self.accumulator_columns[0]
        if self.target_column is None or not self.ignore_nulls:
            return _group_size(df, key_columns, acc_col)

        sizes = _group_size(df, key_columns, acc_col)
        count_dtype = _get_column_dtype(sizes, acc_col)
        counts = _group_count(df, key_columns, self.target_column, acc_col)
        result = sizes[list(key_columns)].merge(
            counts, on=list(key_columns), how="left"
        )
        _fill_missing_count(result, acc_col, count_dtype)
        return result[list(key_columns) + [acc_col]]

    def _final_count(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with final counts.

        This is called by ``final_aggregate`` for ``count`` aggregations.
        """
        acc_col = self.accumulator_columns[0]
        result = _group_aggregate(df, key_columns, acc_col, "sum", self.output_name)
        return result[list(key_columns) + [self.output_name]]


    def _partial_mean(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with partially aggregated/reduced mean values.

        This is called by ``partial_aggregate`` for ``mean`` aggregations.
        """
        assert self.target_column is not None

        sum_col, count_col, null_count_col = self.accumulator_columns
        size_col = f"{self.accumulator_prefix}_size"
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

        return result[list(key_columns) + list(self.accumulator_columns)]

    def _final_mean(
        self, df: cudf.DataFrame, key_columns: Tuple[str, ...]
    ) -> cudf.DataFrame:
        """Return a cuDF DataFrame with final aggregated/reduced mean values.

        This is called by ``final_aggregate`` for ``mean`` aggregations.
        """
        sum_col, count_col, null_count_col = self.accumulator_columns
        final_sum_col = f"{self.accumulator_prefix}_final_sum"
        final_count_col = f"{self.accumulator_prefix}_final_count"
        final_null_count_col = f"{self.accumulator_prefix}_final_null_count"
        sum_dtype = _get_column_dtype(df, sum_col)

        counts = _group_aggregate(df, key_columns, count_col, "sum", final_count_col)
        null_counts = _group_aggregate(
            df, key_columns, null_count_col, "sum", final_null_count_col
        )
        result = counts.merge(null_counts, on=list(key_columns), how="left")

        sums = _group_aggregate(df, key_columns, sum_col, "sum", final_sum_col)
        result = result.merge(sums, on=list(key_columns), how="left")
        _fill_missing_reduction(result, final_sum_col, sum_dtype)

        result[self.output_name] = result[final_sum_col] / result[final_count_col]

        null_mask = result[final_count_col] == 0
        if not self.ignore_nulls:
            null_mask = null_mask | (result[final_null_count_col] > 0)
        result.loc[null_mask, self.output_name] = None

        return result[list(key_columns) + [self.output_name]]


class GPUAggregationPlan:
    """Executable GPU aggregation plan shared by the driver and GPU actors."""

    def __init__(
        self,
        key_columns: Tuple[str, ...],
        specs: Tuple[GPUAggregationSpec, ...],
        input_schema: Optional[Schema] = None,
    ) -> None:
        self._key_columns = key_columns
        self._specs = specs
        self._input_schema = input_schema
        self._is_global = len(key_columns) == 0
        global_key = _GLOBAL_AGGREGATE_KEY
        required_columns = {
            column for spec in specs for column in spec.required_columns
        }
        while global_key in required_columns:
            global_key = f"_{global_key}"
        self._shuffle_key_columns = (global_key,) if self._is_global else key_columns

    @property
    def shuffle_key_columns(self) -> Tuple[str, ...]:
        return self._shuffle_key_columns

    @property
    def output_names(self) -> Tuple[str, ...]:
        return tuple(spec.output_name for spec in self._specs)

    @property
    def required_columns(self) -> Tuple[str, ...]:
        columns = list(self._key_columns)
        for spec in self._specs:
            for column in spec.required_columns:
                if column not in columns:
                    columns.append(column)
        return tuple(columns)

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        columns: List[str] = []
        for spec in self._specs:
            columns.extend(spec.accumulator_columns)
        return tuple(columns)

    def _partial_output_dtypes(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Any]:
        dtypes: Dict[str, Any] = {}
        for column in key_columns:
            if column not in df.columns:
                continue
            dtype = _schema_column_dtype(input_schema, column)
            if dtype is not None:
                dtype = _normalize_intermediate_dtype(dtype, "key")
            else:
                dtype = _get_column_dtype(df, column)
            dtypes[column] = dtype
        for spec in self._specs:
            dtypes.update(
                spec.partial_accumulator_dtypes(df, input_schema=input_schema)
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
            if observed_dtype is None or not isinstance(observed_dtype, pa.DataType):
                continue

            current_dtype = fields.get(column)
            if current_dtype is None or _is_null_dtype(current_dtype):
                fields[column] = observed_dtype

        if not fields:
            return current

        ordered_names = [
            column for column in self.required_columns if column in fields
        ]
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
    ) -> Optional[DTypeLike]:
        dtype = _schema_column_dtype(self._input_schema, column)
        if dtype is not None:
            return dtype
        return _schema_column_dtype(runtime_input_schema, column)

    def _final_arrow_types(
        self, input_schema: Optional[Schema] = None
    ) -> Dict[str, Any]:
        input_schema = self._effective_input_schema(input_schema)
        types: Dict[str, Any] = {}
        for spec in self._specs:
            types.update(spec.final_arrow_types(input_schema=input_schema))
        return types

    def _final_output_dtypes(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Any]:
        input_schema = self._effective_input_schema(input_schema)
        dtypes: Dict[str, Any] = {}

        if not self._is_global:
            for column in self._shuffle_key_columns:
                dtype = self._effective_column_dtype(column, input_schema)
                if dtype is not None:
                    dtype = _normalize_intermediate_dtype(dtype, "key")
                else:
                    dtype = _get_column_dtype(df, column)
                dtypes[column] = dtype

        for spec in self._specs:
            dtypes.update(
                spec.final_output_dtypes(
                    df,
                    input_schema=input_schema,
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
                for spec in self._specs:
                    for column, value in spec.empty_global_partial_values().items():
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
        for spec in self._specs:
            partial = spec.partial_aggregate(
                df, key_columns, input_schema=input_schema
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
        for spec in self._specs:
            finalized = spec.final_aggregate(df, key_columns)
            result = (
                finalized
                if result is None
                else result.merge(finalized, on=list(key_columns), how="outer")
            )

        assert result is not None
        if self._is_global:
            result = result.drop(columns=[self._shuffle_key_columns[0]])

        return result[output_columns]


def _get_custom_gpu_aggregation_spec(
    agg: AggregateFn,
    *,
    output_name: str,
    accumulator_prefix: str,
) -> Optional[GPUAggregationSpec]:
    factory = getattr(agg, "gpu_aggregate_spec", None)
    if factory is None:
        return None

    spec = factory(output_name=output_name, accumulator_prefix=accumulator_prefix)
    if not isinstance(spec, GPUAggregationSpec):
        raise TypeError(
            f"{type(agg).__name__} returned {type(spec).__name__} from its GPU "
            "aggregation spec factory. Expected GPUAggregationSpec."
        )
    return spec


def _get_builtin_gpu_aggregation_spec(
    agg: AggregateFn,
    *,
    output_name: str,
    accumulator_prefix: str,
    input_schema: Optional[Schema] = None,
) -> Optional[GPUAggregationSpec]:
    if not isinstance(agg, AggregateFnV2):
        return None

    target_column = agg.get_target_column()
    ignore_nulls = agg._ignore_nulls
    source_dtype = _schema_column_dtype(input_schema, target_column)

    if isinstance(agg, Count):
        return BuiltinGPUAggregationSpec(
            "count",
            output_name=output_name,
            target_column=target_column,
            ignore_nulls=ignore_nulls,
            accumulator_prefix=accumulator_prefix,
            source_dtype=source_dtype,
        )

    if target_column is None:
        return None

    if isinstance(agg, Sum):
        aggregation = "sum"
    elif isinstance(agg, Min):
        aggregation = "min"
    elif isinstance(agg, Max):
        aggregation = "max"
    elif isinstance(agg, Mean):
        aggregation = "mean"
    else:
        return None

    return BuiltinGPUAggregationSpec(
        aggregation,
        output_name=output_name,
        target_column=target_column,
        ignore_nulls=ignore_nulls,
        accumulator_prefix=accumulator_prefix,
        source_dtype=source_dtype,
    )


def build_gpu_aggregation_plan(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[AggregateFn, ...],
    input_schema: Optional[Schema] = None,
) -> Optional[GPUAggregationPlan]:
    if not aggregation_fns:
        return None

    resolved_names = _resolve_aggregation_names(aggregation_fns)
    specs: List[GPUAggregationSpec] = []

    for index, (agg, output_name) in enumerate(zip(aggregation_fns, resolved_names)):
        accumulator_prefix = f"__ray_gpu_agg_{index}"
        spec = _get_custom_gpu_aggregation_spec(
            agg,
            output_name=output_name,
            accumulator_prefix=accumulator_prefix,
        )
        if spec is None:
            spec = _get_builtin_gpu_aggregation_spec(
                agg,
                output_name=output_name,
                accumulator_prefix=accumulator_prefix,
                input_schema=input_schema,
            )
        if spec is None:
            return None
        specs.append(spec)

    return GPUAggregationPlan(key_columns, tuple(specs), input_schema=input_schema)


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
        self._runtime_input_schema: Optional[pa.Schema] = None

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
            df, input_schema=table.schema
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
            else data_context.default_hash_shuffle_parallelism
        )
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
