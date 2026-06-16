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
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
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


def _cast_cudf_column_dtype(
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
        if len(df) > 0 and not bool(df[column].isnull().all()):
            return

        import cudf

        df[column] = cudf.Series([None] * len(df), dtype=cast_dtype)


def _cudf_column_dtype(df: cudf.DataFrame, column: str) -> Optional[DataType]:
    """Get the DataType of a column from a ``cudf.DataFrame``.

    Returns None if the column is not found.
    """
    if column not in df.columns:
        return None

    raw_dtype = df[column].dtype
    try:
        return DataType.from_cudf(raw_dtype)
    except (AttributeError, ImportError, TypeError):
        return DataType.from_dtype(raw_dtype)


def _schema_column_dtype(
    schema: Optional[Schema], column: Optional[str]
) -> Optional[DataType]:
    """Get the DataType of a column from a ``Schema``.

    Returns None if the column is not found (or None).
    """
    if schema is None or column is None or column not in schema.names:
        return None

    if isinstance(schema, pa.Schema):
        return DataType.from_dtype(schema.field(column).type)

    return DataType.from_dtype(schema.types[schema.names.index(column)])


class GPUAggregateFn(abc.ABC):
    """Extension point for GPU-enabled aggregations.

    GPU aggregate implementations define cuDF partial and final aggregation methods.

    Args:
        name: The name of the aggregation, which will be used as part of the column name
            in the output, e.g. "sum" -> "sum(col)".
        on: The name of the column to perform the aggregation on.
        ignore_nulls: Whether to ignore null values during aggregation.
            For example, should "count" include null rows or not?
        accumulators: The names of (internal) accumulators used by the aggregation.
            For example, "sum" uses "value" while "mean" uses ("sum", "count",
            "null_count").  These will be combined with the accumulator_prefix to form
            the final, unique names of the GPU accumulator columns.
    """

    def __init__(
        self,
        name: str,
        *,
        on: Optional[str],
        ignore_nulls: bool,
        accumulators: Tuple[str, ...] = ("value",),
    ) -> None:
        if not name:
            raise ValueError(
                f"Non-empty string has to be provided as name (got {name})"
            )
        if not accumulators or any(not accumulator for accumulator in accumulators):
            raise ValueError("Accumulators must be non-empty strings.")

        self.name = name
        self.target_column = on
        self.ignore_nulls = ignore_nulls
        self._accumulators = accumulators

    @abc.abstractmethod
    def partial_aggregate(
        self,
        df: Any,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> Any:
        """Aggregate one input block (as a ``cudf.DataFrame``) into GPU accumulator
        columns."""
        ...

    @abc.abstractmethod
    def final_aggregate(
        self,
        df: Any,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> Any:
        """Aggregate shuffled GPU accumulator columns into final output."""
        ...

    def _accumulator_columns(self, accumulator_prefix: str) -> Tuple[str, ...]:
        """Return the final, unique names of the GPU accumulator columns per
        aggregation by concatenating the accumulator_prefix and the accumulator columns.

        The accumulator_prefix is generated by the GPUAggregationPlan to uniquely
        identify each GPU aggregation, e.g. for a single `mean` aggregation with
        accumulator prefix "__ray_gpu_agg_0", the unique accumulator column names
        will be:
        - "__ray_gpu_agg_0_sum"
        - "__ray_gpu_agg_0_count"
        - "__ray_gpu_agg_0_null_count"
        """
        return tuple(
            f"{accumulator_prefix}_{accumulator}" for accumulator in self._accumulators
        )

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        """Return accumulator values for an empty block during a global aggregation
        operation (no key columns).

        This is used to ensure that the GPU aggregation can handle empty blocks
        gracefully.

        Subclasses should override this when all-null accumulator values are not
        semantically correct for empty global input.
        """
        return {
            column: None for column in self._accumulator_columns(accumulator_prefix)
        }

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return dtypes for partial accumulator columns.

        Subclasses should override this when accumulator columns require explicit
        dtype normalization.
        """
        return {
            column: None for column in self._accumulator_columns(accumulator_prefix)
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return dtypes for final cuDF output columns.

        Subclasses should override this when final output columns require explicit
        dtype normalization.
        """
        return {}

    def _final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        """Return Arrow types for final output columns.

        Subclasses should override this when Arrow output normalization requires
        explicit types.
        """
        return {}


def _fill_missing_count(
    result: cudf.DataFrame, count_column: str, dtype: Optional[DataType] = None
) -> None:
    """Fill missing counts with 0 and cast to specified dtype.

    This is used to ensure that the GPU aggregation can handle empty blocks
    gracefully.
    """
    if count_column not in result.columns:
        result[count_column] = 0
    else:
        result[count_column] = result[count_column].fillna(0)
    _cast_cudf_column_dtype(result, count_column, dtype)


def _fill_missing_reduction(
    result: cudf.DataFrame, reduction_column: str, dtype: Optional[DataType] = None
) -> None:
    """Fill any missing reduction values with None and cast to specified dtype.

    This is used to ensure that the GPU aggregation can handle empty blocks
    gracefully.
    """
    if reduction_column not in result.columns:
        result[reduction_column] = None
    _cast_cudf_column_dtype(result, reduction_column, dtype)


def _reduction_dtype(
    source_dtype: Optional[DataType],
    target_column: str,
    aggregation_name: str,
    df: cudf.DataFrame,
    input_schema: Optional[Schema],
) -> Optional[DataType]:
    """Return the dtype to use for a GPU accumulator column for supported built-in
    aggregations.

    This includes type promotion for boolean and integer types."""
    dtype = source_dtype
    if dtype is None:
        dtype = _schema_column_dtype(input_schema, target_column)
    if dtype is None:
        dtype = _cudf_column_dtype(df, target_column)
    if dtype is None:
        return None

    if dtype.is_null_type():
        return DataType.from_numpy("float64" if aggregation_name == "mean" else "int64")

    if aggregation_name in ("sum", "mean"):
        if dtype.is_boolean_type():
            return DataType.from_numpy("int64")
        if dtype.is_integer_type():
            return DataType.from_numpy("uint64" if dtype.is_uint64_type() else "int64")
        if dtype.is_floating_type():
            return DataType.from_numpy("float64")

    if aggregation_name in ("min", "max"):
        if dtype.is_boolean_type():
            return DataType.from_numpy("bool")
        if dtype.is_integer_type():
            return DataType.from_numpy("uint64" if dtype.is_uint64_type() else "int64")
        if dtype.is_floating_type():
            return DataType.from_numpy("float64")

    return dtype


class GPUCount(GPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Count`."""

    def __init__(self, agg: Count, *, source_dtype: Optional[DataType] = None) -> None:
        self.source_dtype = source_dtype
        super().__init__(
            agg.name,
            on=agg.get_target_column(),
            ignore_nulls=agg._ignore_nulls,
            accumulators=("value",),
        )

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
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

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        result = result.rename(columns={result.columns[-1]: output_name})
        return result[list(key_columns) + [output_name]]

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        return {self._accumulator_columns(accumulator_prefix)[0]: 0}

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: DataType.from_numpy(
                "int64"
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("int64")}


class GPUSum(GPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Sum`."""

    def __init__(self, agg: Sum, *, source_dtype: Optional[DataType] = None) -> None:
        self.source_dtype = source_dtype
        super().__init__(
            agg.name,
            on=agg.get_target_column(),
            ignore_nulls=agg._ignore_nulls,
            accumulators=("value",),
        )

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        assert self.target_column is not None
        acc_col = accumulator_columns[0]
        output_dtype = _reduction_dtype(
            self.source_dtype,
            self.target_column,
            "sum",
            df,
            input_schema,
        )
        size_col = f"{acc_col}_size"
        count_col = f"{acc_col}_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[self.target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[acc_col] = None
            _cast_cudf_column_dtype(result, acc_col, output_dtype)
        else:
            aggregated = grouped[self.target_column].sum().reset_index()
            aggregated = aggregated.rename(columns={aggregated.columns[-1]: acc_col})
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, acc_col, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, acc_col] = None
        return result[list(key_columns) + [acc_col]]

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        output_dtype = _cudf_column_dtype(df, acc_col)
        size_col = f"{acc_col}_partial_size"
        count_col = f"{acc_col}_partial_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[acc_col].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[output_name] = None
            _cast_cudf_column_dtype(result, output_name, output_dtype)
        else:
            aggregated = grouped[acc_col].sum().reset_index()
            aggregated = aggregated.rename(
                columns={aggregated.columns[-1]: output_name}
            )
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, output_name, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, output_name] = None
        return result[list(key_columns) + [output_name]]

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                None if self.ignore_nulls else 0
            )
        }

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        assert self.target_column is not None
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                _reduction_dtype(
                    self.source_dtype,
                    self.target_column,
                    "sum",
                    df,
                    input_schema,
                )
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        acc_col = self._accumulator_columns(accumulator_prefix)[0]
        acc_dtype = _cudf_column_dtype(df, acc_col)
        if acc_dtype is None:
            acc_dtype = self._partial_accumulator_dtypes(
                df, accumulator_prefix, input_schema=input_schema
            )[acc_col]
        return {output_name: acc_dtype}

    def _final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        dtype = self.source_dtype
        if dtype is None or dtype.is_null_type():
            dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is None or not dtype.is_null_type():
            return {}
        return {output_name: pa.null()}


class GPUMin(GPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Min`."""

    def __init__(self, agg: Min, *, source_dtype: Optional[DataType] = None) -> None:
        self.source_dtype = source_dtype
        super().__init__(
            agg.name,
            on=agg.get_target_column(),
            ignore_nulls=agg._ignore_nulls,
            accumulators=("value",),
        )

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        assert self.target_column is not None
        acc_col = accumulator_columns[0]
        output_dtype = _reduction_dtype(
            self.source_dtype,
            self.target_column,
            "min",
            df,
            input_schema,
        )
        size_col = f"{acc_col}_size"
        count_col = f"{acc_col}_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[self.target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[acc_col] = None
            _cast_cudf_column_dtype(result, acc_col, output_dtype)
        else:
            aggregated = grouped[self.target_column].min().reset_index()
            aggregated = aggregated.rename(columns={aggregated.columns[-1]: acc_col})
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, acc_col, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, acc_col] = None
        return result[list(key_columns) + [acc_col]]

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        output_dtype = _cudf_column_dtype(df, acc_col)
        size_col = f"{acc_col}_partial_size"
        count_col = f"{acc_col}_partial_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[acc_col].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[output_name] = None
            _cast_cudf_column_dtype(result, output_name, output_dtype)
        else:
            aggregated = grouped[acc_col].min().reset_index()
            aggregated = aggregated.rename(
                columns={aggregated.columns[-1]: output_name}
            )
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, output_name, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, output_name] = None
        return result[list(key_columns) + [output_name]]

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                None if self.ignore_nulls else float("+inf")
            )
        }

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        assert self.target_column is not None
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                _reduction_dtype(
                    self.source_dtype,
                    self.target_column,
                    "min",
                    df,
                    input_schema,
                )
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        acc_col = self._accumulator_columns(accumulator_prefix)[0]
        acc_dtype = _cudf_column_dtype(df, acc_col)
        if acc_dtype is None:
            acc_dtype = self._partial_accumulator_dtypes(
                df, accumulator_prefix, input_schema=input_schema
            )[acc_col]
        return {output_name: acc_dtype}

    def _final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        dtype = self.source_dtype
        if dtype is None or dtype.is_null_type():
            dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is None or not dtype.is_null_type():
            return {}
        return {output_name: pa.null()}


class GPUMax(GPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Max`."""

    def __init__(self, agg: Max, *, source_dtype: Optional[DataType] = None) -> None:
        self.source_dtype = source_dtype
        super().__init__(
            agg.name,
            on=agg.get_target_column(),
            ignore_nulls=agg._ignore_nulls,
            accumulators=("value",),
        )

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        assert self.target_column is not None
        acc_col = accumulator_columns[0]
        output_dtype = _reduction_dtype(
            self.source_dtype,
            self.target_column,
            "max",
            df,
            input_schema,
        )
        size_col = f"{acc_col}_size"
        count_col = f"{acc_col}_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[self.target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[acc_col] = None
            _cast_cudf_column_dtype(result, acc_col, output_dtype)
        else:
            aggregated = grouped[self.target_column].max().reset_index()
            aggregated = aggregated.rename(columns={aggregated.columns[-1]: acc_col})
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, acc_col, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, acc_col] = None
        return result[list(key_columns) + [acc_col]]

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        output_dtype = _cudf_column_dtype(df, acc_col)
        size_col = f"{acc_col}_partial_size"
        count_col = f"{acc_col}_partial_count"
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[acc_col].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[output_name] = None
            _cast_cudf_column_dtype(result, output_name, output_dtype)
        else:
            aggregated = grouped[acc_col].max().reset_index()
            aggregated = aggregated.rename(
                columns={aggregated.columns[-1]: output_name}
            )
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, output_name, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, output_name] = None
        return result[list(key_columns) + [output_name]]

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                None if self.ignore_nulls else float("-inf")
            )
        }

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        assert self.target_column is not None
        return {
            self._accumulator_columns(accumulator_prefix)[0]: (
                _reduction_dtype(
                    self.source_dtype,
                    self.target_column,
                    "max",
                    df,
                    input_schema,
                )
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        acc_col = self._accumulator_columns(accumulator_prefix)[0]
        acc_dtype = _cudf_column_dtype(df, acc_col)
        if acc_dtype is None:
            acc_dtype = self._partial_accumulator_dtypes(
                df, accumulator_prefix, input_schema=input_schema
            )[acc_col]
        return {output_name: acc_dtype}

    def _final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        dtype = self.source_dtype
        if dtype is None or dtype.is_null_type():
            dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is None or not dtype.is_null_type():
            return {}
        return {output_name: pa.null()}


class GPUMean(GPUAggregateFn):
    """GPU implementation for :class:`ray.data.aggregate.Mean`."""

    def __init__(self, agg: Mean, *, source_dtype: Optional[DataType] = None) -> None:
        self.source_dtype = source_dtype
        super().__init__(
            agg.name,
            on=agg.get_target_column(),
            ignore_nulls=agg._ignore_nulls,
            accumulators=("sum", "count", "null_count"),
        )

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        assert self.target_column is not None

        sum_col, count_col, null_count_col = accumulator_columns
        size_col = f"{sum_col}_size"
        output_dtype = _reduction_dtype(
            self.source_dtype,
            self.target_column,
            "mean",
            df,
            input_schema,
        )
        grouped = df.groupby(list(key_columns), dropna=False)

        sizes = grouped.size().reset_index()
        sizes = sizes.rename(columns={sizes.columns[-1]: size_col})
        count_dtype = _cudf_column_dtype(sizes, size_col)

        counts = grouped[self.target_column].count().reset_index()
        counts = counts.rename(columns={counts.columns[-1]: count_col})

        result = sizes.merge(counts, on=list(key_columns), how="left")
        _fill_missing_count(result, count_col, count_dtype)

        if len(result) > 0 and bool((result[count_col] == 0).all()):
            result[sum_col] = None
            _cast_cudf_column_dtype(result, sum_col, output_dtype)
        else:
            aggregated = grouped[self.target_column].sum().reset_index()
            aggregated = aggregated.rename(columns={aggregated.columns[-1]: sum_col})
            result = result.merge(aggregated, on=list(key_columns), how="left")
            _fill_missing_reduction(result, sum_col, output_dtype)

        null_mask = result[count_col] == 0
        if not self.ignore_nulls:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, sum_col] = None

        result[null_count_col] = result[size_col] - result[count_col]
        _cast_cudf_column_dtype(result, null_count_col, count_dtype)

        return result[list(key_columns) + list(accumulator_columns)]

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        sum_col, count_col, null_count_col = accumulator_columns
        final_sum_col = f"{sum_col}_final_sum"
        final_count_col = f"{count_col}_final_count"
        final_null_count_col = f"{null_count_col}_final_null_count"
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

    def _empty_global_partial_values(self, accumulator_prefix: str) -> Dict[str, Any]:
        sum_col, count_col, null_count_col = self._accumulator_columns(
            accumulator_prefix
        )
        return {sum_col: None, count_col: 0, null_count_col: 0}

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        sum_col, count_col, null_count_col = self._accumulator_columns(
            accumulator_prefix
        )
        assert self.target_column is not None
        return {
            sum_col: _reduction_dtype(
                self.source_dtype,
                self.target_column,
                "mean",
                df,
                input_schema,
            ),
            count_col: DataType.from_numpy("int64"),
            null_count_col: DataType.from_numpy("int64"),
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("float64")}

    def _final_arrow_types(
        self,
        output_name: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, pa.DataType]:
        dtype = self.source_dtype
        if dtype is None or dtype.is_null_type():
            dtype = _schema_column_dtype(input_schema, self.target_column)
        if dtype is None or not dtype.is_null_type():
            return {}
        return {output_name: pa.null()}


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
        _cast_cudf_column_dtype(df, column, dtypes.get(column))
    return df


class GPUAggregationPlan:
    """Executable GPU aggregation plan shared by the driver and GPU actors.

    Args:
        key_columns: The key columns to group by.
        gpu_aggregates: The GPU aggregate functions to apply.
        accumulator_prefix: The prefix for intermediate accumulator columns.
        input_schema: The schema of the input data.
    """

    def __init__(
        self,
        key_columns: Tuple[str, ...],
        gpu_aggregates: Tuple[GPUAggregateFn, ...],
        accumulator_prefix: str,
        input_schema: Optional[Schema] = None,
    ) -> None:
        if not accumulator_prefix:
            raise ValueError("Accumulator prefix must be a non-empty string.")

        self._key_columns = key_columns
        self._gpu_aggregates = gpu_aggregates
        self._input_schema = input_schema
        self._is_global = not key_columns
        self._shuffle_key_columns = key_columns

        # Resolve duplicate aggregation names the same way TableBlockAccessor does.
        counts: Dict[str, int] = defaultdict(int)
        resolved_names: List[str] = []
        for agg in gpu_aggregates:
            name = agg.name
            if counts[name] > 0:
                name = TableBlockAccessor._munge_conflict(name, counts[name])
            counts[agg.name] += 1
            resolved_names.append(name)
        self._output_names = tuple(resolved_names)

        # Generate unique accumulator prefixes for each resolved name
        self._accumulator_prefixes = tuple(
            f"{accumulator_prefix}_{index}" for index, _ in enumerate(gpu_aggregates)
        )

        # If global aggregation (w/o key columns), use an artificial shuffle key.
        if self._is_global:
            # filter out empty target columns
            required_columns = {
                agg.target_column
                for agg in gpu_aggregates
                if agg.target_column is not None
            }
            # ensure a unique global key by prepending an underscore if needed
            # (just in case there is a collision)
            global_key = _GLOBAL_AGGREGATE_KEY
            while global_key in required_columns:
                global_key = f"_{global_key}"
            # set the shuffle key to the global key
            self._shuffle_key_columns = (global_key,)

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        """Return all internal accumulator columns for the GPU aggregation plan."""
        columns: List[str] = []
        for agg, accumulator_prefix in zip(
            self._gpu_aggregates, self._accumulator_prefixes
        ):
            columns.extend(agg._accumulator_columns(accumulator_prefix))
        return tuple(columns)

    @property
    def output_names(self) -> Tuple[str, ...]:
        """Return all final output names for the GPU aggregation plan.

        These will be used as the column names for the final output of the GPU
        aggregations in the plan, e.g. "sum(col1)", "mean(col2)", etc.
        """
        return self._output_names

    @property
    def required_columns(self) -> Tuple[str, ...]:
        """Return all required columns for the GPU aggregation plan.

        These include the key columns and aggregation target columns, e.g.
        groupby("col1").sum("col2")
        """
        columns = list(self._key_columns)
        for agg in self._gpu_aggregates:
            target_column = agg.target_column
            if target_column is not None and target_column not in columns:
                columns.append(target_column)
        return tuple(columns)

    @property
    def shuffle_key_columns(self) -> Tuple[str, ...]:
        """Return the shuffle key columns for the GPU aggregation plan."""
        return self._shuffle_key_columns

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
                for agg, accumulator_prefix in zip(
                    self._gpu_aggregates,
                    self._accumulator_prefixes,
                ):
                    empty_values = agg._empty_global_partial_values(accumulator_prefix)
                    for column, value in empty_values.items():
                        values[column] = [value]
                result = cudf_module.DataFrame(values)[
                    list(key_columns) + list(self.accumulator_columns)
                ]
                for column, dtype in self._partial_accumulator_dtypes(
                    df, key_columns, input_schema=input_schema
                ).items():
                    _cast_cudf_column_dtype(result, column, dtype)
                return result
            return _empty_dataframe(
                cudf_module,
                list(key_columns) + list(self.accumulator_columns),
                dtypes=self._partial_accumulator_dtypes(
                    df, key_columns, input_schema=input_schema
                ),
            )

        result = None
        for agg, accumulator_prefix in zip(
            self._gpu_aggregates, self._accumulator_prefixes
        ):
            accumulator_columns = agg._accumulator_columns(accumulator_prefix)
            partial = agg.partial_aggregate(
                df,
                key_columns,
                accumulator_columns,
                input_schema=input_schema,
            )
            result = (
                partial
                if result is None
                else result.merge(partial, on=list(key_columns), how="outer")
            )

        assert result is not None
        for column, dtype in self._partial_accumulator_dtypes(
            df, key_columns, input_schema=input_schema
        ).items():
            _cast_cudf_column_dtype(result, column, dtype)
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
                dtypes=self._final_cudf_dtypes(
                    df,
                    input_schema=input_schema,
                ),
            )

        result = None
        for agg, output_name, accumulator_prefix in zip(
            self._gpu_aggregates, self._output_names, self._accumulator_prefixes
        ):
            accumulator_columns = agg._accumulator_columns(accumulator_prefix)
            finalized = agg.final_aggregate(
                df,
                key_columns,
                accumulator_columns,
                output_name,
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

    def merge_input_schema(
        self, current: Optional[pa.Schema], observed: Optional[Schema]
    ) -> Optional[pa.Schema]:
        """Merge an observed block schema into the current runtime input schema."""
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

    def _effective_column_dtype(
        self, column: str, runtime_input_schema: Optional[Schema] = None
    ) -> Optional[DataType]:
        """Return the dtype for a column in the input schema.

        This method first checks the input schema provided to the GPUAggregationPlan
        constructor, and then falls back to the runtime input schema if provided.
        """
        dtype = _schema_column_dtype(self._input_schema, column)
        if dtype is not None:
            return dtype
        return _schema_column_dtype(runtime_input_schema, column)

    def _effective_input_schema(
        self, runtime_input_schema: Optional[Schema] = None
    ) -> Optional[Schema]:
        """Return the effective input schema for the GPU aggregation plan.

        This method supplies a fallback schema derived from the input schema,
        if the runtime input schema is not provided.
        """
        return (
            runtime_input_schema
            if runtime_input_schema is not None
            else self._input_schema
        )

    def _final_arrow_types(
        self, input_schema: Optional[Schema] = None
    ) -> Dict[str, pa.DataType]:
        """Return the Arrow types for the final output columns of the GPU aggregation plan."""
        input_schema = self._effective_input_schema(input_schema)
        types: Dict[str, pa.DataType] = {}
        for agg, output_name in zip(self._gpu_aggregates, self._output_names):
            types.update(
                agg._final_arrow_types(
                    output_name,
                    input_schema=input_schema,
                )
            )
        return types

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        """Return the cuDF dtypes for the final output columns of the GPU aggregation plan.

        This provides a fallback schema derived from the supplied input schema and runtime input
        schema,
        """
        input_schema = self._effective_input_schema(input_schema)
        dtypes: Dict[str, Optional[DataType]] = {}

        if not self._is_global:
            for column in self._shuffle_key_columns:
                dtype = self._effective_column_dtype(column, input_schema)
                if dtype is None:
                    dtype = _cudf_column_dtype(df, column)
                elif dtype.is_null_type():
                    dtype = DataType.from_numpy("float64")
                dtypes[column] = dtype

        for agg, output_name, accumulator_prefix in zip(
            self._gpu_aggregates, self._output_names, self._accumulator_prefixes
        ):
            dtypes.update(
                {
                    column: DataType.from_dtype(dtype)
                    for column, dtype in agg._final_cudf_dtypes(
                        df,
                        output_name,
                        accumulator_prefix,
                        input_schema=input_schema,
                    ).items()
                }
            )
        return dtypes

    def _partial_accumulator_dtypes(
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
            if dtype is None:
                dtype = _cudf_column_dtype(df, column)
            elif dtype.is_null_type():
                dtype = DataType.from_numpy("float64")
            dtypes[column] = dtype
        for agg, accumulator_prefix in zip(
            self._gpu_aggregates, self._accumulator_prefixes
        ):
            dtypes.update(
                {
                    column: DataType.from_dtype(dtype) if dtype is not None else None
                    for column, dtype in agg._partial_accumulator_dtypes(
                        df,
                        accumulator_prefix,
                        input_schema=input_schema,
                    ).items()
                }
            )
        return dtypes


def build_gpu_aggregation_plan(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[Union[AggregateFn, GPUAggregateFn], ...],
    input_schema: Optional[Schema] = None,
) -> Union[GPUAggregationPlan, str]:
    """Build a GPU aggregation plan.

    Args:
        key_columns: The key columns to group by.
        aggregation_fns: The aggregation functions to apply.
        input_schema: The schema of the input data.

    Returns:
        A GPU aggregation plan if supported, otherwise a fallback reason string.
    """
    if not aggregation_fns:
        # No aggregation functions, no plan needed.
        return "no aggregation functions were provided."

    has_gpu_aggregate = any(isinstance(agg, GPUAggregateFn) for agg in aggregation_fns)
    missing_key_columns = [
        column
        for column in key_columns
        if _schema_column_dtype(input_schema, column) is None
    ]
    if missing_key_columns and not has_gpu_aggregate:
        # Missing key columns in the input schema, fallback to CPU.
        return (
            "missing input schema for key column(s): "
            f"{', '.join(missing_key_columns)}."
        )

    gpu_aggregates: List[GPUAggregateFn] = []

    for agg in aggregation_fns:
        if isinstance(agg, GPUAggregateFn):
            # handle subclasses of GPUAggregateFn as-is (e.g. custom GPU aggregations)
            gpu_aggregate = agg
        else:
            # try to convert built-in GPU aggregation functions to GPU equivalents
            if not isinstance(agg, AggregateFnV2):
                return (
                    f"{type(agg).__name__} is not supported by GPU aggregation "
                    "because it is not an AggregateFnV2."
                )

            target_column = agg.get_target_column()
            source_dtype = _schema_column_dtype(input_schema, target_column)

            if isinstance(agg, Count):
                gpu_aggregate = GPUCount(agg, source_dtype=source_dtype)
            elif target_column is None:
                return (
                    f"{type(agg).__name__} is not supported by GPU aggregation "
                    "without a target column."
                )
            elif isinstance(agg, Sum):
                gpu_aggregate = GPUSum(agg, source_dtype=source_dtype)
            elif isinstance(agg, Min):
                gpu_aggregate = GPUMin(agg, source_dtype=source_dtype)
            elif isinstance(agg, Max):
                gpu_aggregate = GPUMax(agg, source_dtype=source_dtype)
            elif isinstance(agg, Mean):
                gpu_aggregate = GPUMean(agg, source_dtype=source_dtype)
            else:
                # Any unsupported built-in aggregation in the list falls back
                # the entire list to CPU.
                return f"{type(agg).__name__} is not supported by GPU aggregation."

        gpu_aggregates.append(gpu_aggregate)

    return GPUAggregationPlan(
        key_columns,
        tuple(gpu_aggregates),
        accumulator_prefix="__ray_gpu_agg",
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
