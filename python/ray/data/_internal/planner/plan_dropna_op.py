"""
Plan DropNa logical operator.

This module implements the physical planning for DropNa operations, which remove
rows with missing values from Ray Data datasets. It supports multiple drop strategies:
- any: Drop rows where any value is missing
- all: Drop rows where all values are missing
- thresh: Drop rows with fewer than threshold non-missing values

The implementation uses PyArrow for efficient columnar operations.
See https://arrow.apache.org/docs/python/api/compute.html for PyArrow compute functions.
"""

from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.dropna_operator import DropNa
from ray.data._internal.planner.plan_udf_map_op import (
    _create_map_transformer_for_block_based_map_op,
    _generate_transform_fn_for_map_block,
    _try_wrap_udf_exception,
    get_compute,
)
from ray.data.context import DataContext


def plan_dropna_op(
    op: DropNa,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan a DropNa logical operator into a physical MapOperator."""
    assert len(physical_children) == 1

    def fn(batch: pa.Table) -> pa.Table:
        try:
            if batch.num_rows == 0:
                return batch

            if op.subset:
                columns_to_check = [
                    col for col in op.subset if col in batch.schema.names
                ]
                if not columns_to_check:
                    return batch
            else:
                columns_to_check = list(batch.schema.names)

            if op.thresh is not None:
                mask = _create_thresh_mask(
                    batch, columns_to_check, op.thresh, op.ignore_values
                )
            else:
                mask = _create_how_mask(
                    batch, columns_to_check, op.how, op.ignore_values
                )

            return _filter_with_schema_preservation(batch, mask)
        except Exception as e:
            _try_wrap_udf_exception(e, batch)

    compute = get_compute(op._compute)
    transform_fn = _generate_transform_fn_for_map_block(fn)
    map_transformer = _create_map_transformer_for_block_based_map_op(transform_fn)

    return MapOperator.create(
        map_transformer,
        physical_children[0],
        data_context,
        name="DropNa",
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
    )


def _is_not_missing(column: pa.Array, ignore_values: List[Any]) -> pa.Array:
    """Check if values in a column are not missing.

    A value is considered missing if it is:
    - null (None)
    - NaN (for floating point types)
    - in the ignore_values list

    Uses PyArrow compute functions for efficient null/NaN checking.
    See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.is_valid

    Args:
        column: PyArrow array to check.
        ignore_values: Additional values to treat as missing.

    Returns:
        Boolean PyArrow array indicating non-missing values.
    """
    # Check for nulls using PyArrow is_valid
    # See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.is_valid
    is_not_null = pc.is_valid(column)
    # For floating point types, also check for NaN
    if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
        is_not_null = pc.and_(is_not_null, pc.invert(pc.is_nan(column)))

    if not ignore_values:
        return is_not_null

    # Check if values are in ignore_values list
    is_not_ignored = _create_not_ignored_mask(column, ignore_values)
    return pc.and_(is_not_null, is_not_ignored)


def _create_not_ignored_mask(column: pa.Array, ignore_values: List[Any]) -> pa.Array:
    """Create mask for values not in ignore_values list.

    Uses PyArrow compute equal for efficient comparison.
    See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.equal

    Args:
        column: PyArrow array to check.
        ignore_values: Values to treat as missing.

    Returns:
        Boolean PyArrow array indicating values not in ignore_values.

    Raises:
        ValueError: If ignore_value cannot be compared with column type.
    """
    if not ignore_values:
        return pa.array([True] * len(column))

    is_ignored_mask: Optional[pa.Array] = None
    for ignore_val in set(ignore_values):
        try:
            ignore_scalar = pa.scalar(ignore_val, type=column.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            try:
                ignore_scalar = pa.scalar(ignore_val).cast(column.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
                raise ValueError(
                    f"Cannot compare ignore_value {ignore_val!r} with column type {column.type}"
                ) from e

        equals_ignore = pc.equal(column, ignore_scalar)
        is_ignored_mask = (
            equals_ignore
            if is_ignored_mask is None
            else pc.or_(is_ignored_mask, equals_ignore)
        )

    if is_ignored_mask is None:
        return pa.array([True] * len(column))
    return pc.invert(is_ignored_mask)


def _create_thresh_mask(
    batch: pa.Table, columns_to_check: List[str], thresh: int, ignore_values: List[Any]
) -> pa.Array:
    """Create mask for threshold-based row dropping.

    Keeps rows with at least 'thresh' non-missing values across checked columns.

    Args:
        batch: PyArrow table to create mask for.
        columns_to_check: Column names to check for missing values.
        thresh: Minimum number of non-missing values required.
        ignore_values: Additional values to treat as missing.

    Returns:
        Boolean PyArrow array indicating rows to keep.
    """
    if not columns_to_check:
        return pa.array([True] * batch.num_rows)

    non_missing_counts: Optional[pa.Array] = None
    for col_name in columns_to_check:
        is_not_missing_int = pc.cast(
            _is_not_missing(batch.column(col_name), ignore_values), pa.int32()
        )
        if non_missing_counts is None:
            non_missing_counts = is_not_missing_int
        else:
            non_missing_counts = pc.add(non_missing_counts, is_not_missing_int)
    return pc.greater_equal(non_missing_counts, pa.scalar(thresh))


def _create_how_mask(
    batch: pa.Table, columns_to_check: List[str], how: str, ignore_values: List[Any]
) -> pa.Array:
    """Create mask for how-based row dropping ('any' or 'all').

    Args:
        batch: PyArrow table to create mask for.
        columns_to_check: Column names to check for missing values.
        how: Drop strategy - 'any' drops rows with any missing value,
            'all' drops rows where all values are missing.
        ignore_values: Additional values to treat as missing.

    Returns:
        Boolean PyArrow array indicating rows to keep.

    Raises:
        ValueError: If 'how' is not 'any' or 'all'.
    """
    if not columns_to_check:
        return pa.array([True] * batch.num_rows)

    mask: Optional[pa.Array] = None
    for col_name in columns_to_check:
        is_not_missing = _is_not_missing(batch.column(col_name), ignore_values)
        if mask is None:
            mask = is_not_missing
        elif how == "any":
            mask = pc.and_(mask, is_not_missing)
        elif how == "all":
            mask = pc.or_(mask, is_not_missing)
        else:
            raise ValueError(f"Invalid 'how' parameter: {how}. Must be 'any' or 'all'.")
    return mask


def _filter_with_schema_preservation(batch: pa.Table, mask: pa.Array) -> pa.Table:
    """Filter table while preserving original schema.

    Uses PyArrow compute filter for efficient row filtering.
    See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.filter

    Args:
        batch: PyArrow table to filter.
        mask: Boolean array indicating rows to keep.

    Returns:
        Filtered PyArrow table with original schema preserved.
    """
    filtered_batch = pc.filter(batch, mask)
    if filtered_batch.schema != batch.schema:
        filtered_batch = filtered_batch.cast(batch.schema)
    return filtered_batch
