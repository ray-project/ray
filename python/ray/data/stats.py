import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.core.arrays.masked import BaseMaskedDtype

from ray.data.aggregate import (
    AggregateFnV2,
    Count,
    Max,
    Mean,
    Min,
    MissingValuePercentage,
    Std,
    ZeroPercentage,
)

if TYPE_CHECKING:
    from ray.data.dataset import Schema


logger = logging.getLogger(__name__)


def _convert_to_pa_type(
    dtype: Union[np.dtype, pd.ArrowDtype, BaseMaskedDtype]
) -> pa.DataType:
    """Convert pandas/numpy dtype to PyArrow dtype.

    Args:
        dtype: A pandas or numpy dtype

    Returns:
        Corresponding PyArrow DataType
    """
    if isinstance(dtype, pd.ArrowDtype):
        return dtype.pyarrow_dtype
    elif isinstance(dtype, pd.StringDtype):
        # StringDtype is not a BaseMaskedDtype, handle separately
        return pa.string()
    elif isinstance(dtype, BaseMaskedDtype):
        dtype = dtype.numpy_dtype
    return pa.from_numpy_dtype(dtype)


def _get_underlying_type(ftype: pa.DataType) -> pa.DataType:
    """Get the underlying Arrow type, handling dictionary and run-end encoding."""
    if pa.types.is_dictionary(ftype):
        return ftype.value_type
    elif pa.types.is_run_end_encoded(ftype):
        return ftype.value_type
    return ftype


def _is_numerical_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is numerical (supports arithmetic operations).

    Includes: integers, floats, decimals, booleans
    """
    underlying = _get_underlying_type(dtype)
    return (
        pa.types.is_integer(underlying)
        or pa.types.is_floating(underlying)
        or pa.types.is_decimal(underlying)
        or pa.types.is_boolean(underlying)
    )


def _is_string_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is string-like.

    Includes: string, large_string, string_view
    """
    underlying = _get_underlying_type(dtype)
    return (
        pa.types.is_string(underlying)
        or pa.types.is_large_string(underlying)
        or pa.types.is_string_view(underlying)
    )


def _is_binary_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is binary.

    Includes: binary, large_binary, binary_view, fixed_size_binary
    """
    underlying = _get_underlying_type(dtype)
    return (
        pa.types.is_binary(underlying)
        or pa.types.is_large_binary(underlying)
        or pa.types.is_binary_view(underlying)
        or pa.types.is_fixed_size_binary(underlying)
    )


def _is_temporal_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is temporal.

    Includes: date, time, timestamp, duration, interval
    """
    underlying = _get_underlying_type(dtype)
    return pa.types.is_temporal(underlying)


def _is_list_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is list-like.

    Includes: list, large_list, fixed_size_list, list_view, large_list_view
    """
    underlying = _get_underlying_type(dtype)
    return (
        pa.types.is_list(underlying)
        or pa.types.is_large_list(underlying)
        or pa.types.is_fixed_size_list(underlying)
        or pa.types.is_list_view(underlying)
        or pa.types.is_large_list_view(underlying)
    )


def _is_union_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is union (dense or sparse)."""
    underlying = _get_underlying_type(dtype)
    return pa.types.is_union(underlying)


def _is_nested_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is nested.

    Includes: list, struct, map, union
    """
    underlying = _get_underlying_type(dtype)
    return pa.types.is_nested(underlying)


def _is_null_dtype(dtype: pa.DataType) -> bool:
    """Check if Arrow dtype is null."""
    underlying = _get_underlying_type(dtype)
    return pa.types.is_null(underlying)


def _get_arrow_dtype_string(dtype: pa.DataType) -> str:
    """Convert Arrow dtype to string representation."""
    return str(dtype)


def _aggregators_for_dtype(column: str, dtype: pa.DataType) -> List[AggregateFnV2]:
    """Get appropriate aggregators for a given arrow dtype.

    Args:
        column: Column name
        dtype: Arrow dtype

    Returns:
        List of aggregators appropriate for this dtype
    """
    # Check types in order of specificity, with most common types first for performance

    if _is_null_dtype(dtype):
        # Null dtype: only count (everything is null)
        return [
            Count(on=column, ignore_nulls=False),
        ]
    elif _is_numerical_dtype(dtype):
        # Numerical dtypes: compute comprehensive statistics
        # Includes: integers, floats, decimals, booleans
        return [
            Count(on=column, ignore_nulls=False),
            Mean(on=column, ignore_nulls=True),
            Min(on=column, ignore_nulls=True),
            Max(on=column, ignore_nulls=True),
            Std(on=column, ignore_nulls=True, ddof=0),
            MissingValuePercentage(on=column),
            ZeroPercentage(on=column, ignore_nulls=True),
        ]
    elif _is_string_dtype(dtype):
        # String dtypes: count and missing values
        # Includes: string, large_string, string_view
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]
    elif _is_temporal_dtype(dtype):
        # Temporal dtypes: count, min, max, missing values
        # Includes: date, time, timestamp, duration, interval
        # Can compute min/max since temporal types are orderable
        return [
            Count(on=column, ignore_nulls=False),
            Min(on=column, ignore_nulls=True),
            Max(on=column, ignore_nulls=True),
            MissingValuePercentage(on=column),
        ]
    elif _is_binary_dtype(dtype):
        # Binary dtypes: count and missing values
        # Includes: binary, large_binary, binary_view, fixed_size_binary
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]
    elif _is_list_dtype(dtype):
        # List dtypes: count and missing values
        # Includes: list, large_list, fixed_size_list, list_view, large_list_view
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]
    elif _is_union_dtype(dtype):
        # Union dtypes (dense or sparse): count and missing values
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]
    elif _is_nested_dtype(dtype):
        # Catch-all for any nested types not explicitly handled above
        # Nested includes: list, struct, map, union variants
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]
    else:
        # Other/unknown types (e.g., extension types): basic statistics
        # Includes: fixed_shape_tensor, opaque, json, uuid, bool8
        logger.info(
            f"Dtype {dtype} for column {column} is not a standard Arrow type, "
            f"computing basic statistics only"
        )
        return [
            Count(on=column, ignore_nulls=False),
            MissingValuePercentage(on=column),
        ]


@dataclass
class DtypeAggregators:
    """Container for columns grouped by arrow dtype and their aggregators."""

    column_to_dtype: Dict[str, str]  # column name -> arrow dtype string
    aggregators: List[AggregateFnV2]


def _dtype_aggregators_for_dataset(
    schema: Optional["Schema"], columns: Optional[List[str]] = None
) -> DtypeAggregators:
    """Generate aggregators for all columns in a dataset, grouped by arrow dtype.

    Args:
        schema: A Ray Schema instance
        columns: A list of columns to include in the summary. If None, all columns will be included.

    Returns:
        DtypeAggregators containing column-to-dtype mapping and aggregators
    """
    if not schema:
        raise ValueError("Dataset must have a schema to determine column types")

    if columns is None:
        columns = schema.names

    # Validate columns exist in schema
    missing_cols = set(columns) - set(schema.names)
    if missing_cols:
        raise ValueError(f"Columns {missing_cols} not found in dataset schema")

    # Get column types - Ray's Schema provides names and types as lists
    column_names = schema.names
    column_types = schema.types

    # Create a mapping of column names to types
    name_to_type = dict(zip(column_names, column_types))

    column_to_dtype = {}
    all_aggs = []

    for name in columns:
        ftype = name_to_type.get(name)
        if ftype is None:
            logger.warning(f"Skipping field {name}: type is None")
            continue

        # Convert pandas/numpy dtype to PyArrow dtype if necessary
        if not isinstance(ftype, pa.DataType):
            try:
                ftype = _convert_to_pa_type(ftype)
            except Exception as e:
                logger.warning(
                    f"Skipping field {name}: could not convert type {ftype} to PyArrow DataType: {e}"
                )
                continue

        # Store the arrow dtype string representation
        dtype_str = _get_arrow_dtype_string(ftype)
        column_to_dtype[name] = dtype_str

        # Get aggregators based on the dtype
        aggs = _aggregators_for_dtype(name, ftype)
        all_aggs.extend(aggs)

    return DtypeAggregators(
        column_to_dtype=column_to_dtype,
        aggregators=all_aggs,
    )
