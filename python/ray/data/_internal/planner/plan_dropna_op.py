"""
Plan DropNa logical operator.

This module contains the planning logic for converting DropNa logical operators
into physical execution plans using MapOperator.
"""

from typing import List, Optional

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
    """Plan a DropNa logical operator into a physical MapOperator.

    This function converts a DropNa logical operator into a physical execution
    plan that removes rows containing missing values (null/None/NaN) from the
    dataset based on the specified criteria.

    Args:
        op: The DropNa logical operator containing drop configuration.
        physical_children: List containing exactly one input physical operator.
        data_context: The execution context for data processing.

    Returns:
        A MapOperator that performs the dropna operation on input data.

    Raises:
        AssertionError: If physical_children doesn't contain exactly one operator.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    how = op.how
    subset = op.subset
    thresh = op.thresh

    def fn(batch: pa.Table) -> pa.Table:
        """Transform function that drops rows with missing values from a PyArrow table.

        Args:
            batch: Input PyArrow table to process.

        Returns:
            PyArrow table with rows containing missing values removed.
        """
        try:
            if batch.num_rows == 0:
                return batch

            # Determine which columns to check for missing values
            if subset:
                # Filter out any nonexistent columns from subset
                available_columns = set(batch.schema.names)
                columns_to_check = [col for col in subset if col in available_columns]
                # If subset is provided but no valid columns found, return original batch
                if not columns_to_check:
                    return batch
            else:
                columns_to_check = list(batch.schema.names)

            # Create mask for rows to keep
            if thresh is not None:
                # Count non-missing values per row
                mask = _create_thresh_mask(batch, columns_to_check, thresh)
            else:
                # Create mask based on how parameter ("any" or "all")
                mask = _create_how_mask(batch, columns_to_check, how)

            # Filter the table and preserve schema
            return _filter_with_schema_preservation(batch, mask)

        except Exception as e:
            _try_wrap_udf_exception(e, batch)

    compute = get_compute(op._compute)
    transform_fn = _generate_transform_fn_for_map_block(fn)
    map_transformer = _create_map_transformer_for_block_based_map_op(
        transform_fn,
    )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name="DropNa",
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
    )


def _is_not_missing(column: pa.Array) -> pa.Array:
    """Check if values in a column are not missing (not null and not NaN).

    Args:
        column: The PyArrow array to check for missing values.

    Returns:
        Boolean array indicating which values are not missing.

    Examples:
        .. doctest::

            >>> import pyarrow as pa
            >>> import numpy as np
            >>> column = pa.array([1.0, None, np.nan, 4.0])
            >>> mask = _is_not_missing(column)
            >>> mask.to_pylist()
            [True, None, False, True]
    """
    is_not_null = pc.is_valid(column)

    # For floating point columns, also check for NaN
    if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
        is_not_nan = pc.invert(pc.is_nan(column))
        return pc.and_(is_not_null, is_not_nan)
    else:
        return is_not_null


def _create_thresh_mask(
    batch: pa.Table, columns_to_check: List[str], thresh: int
) -> pa.Array:
    """Create mask for threshold-based row dropping.

    Counts non-missing values across specified columns and keeps rows that
    have at least 'thresh' non-missing values.

    Args:
        batch: The PyArrow table to analyze.
        columns_to_check: List of column names to check for missing values.
        thresh: Minimum number of non-missing values required to keep a row.

    Returns:
        Boolean array indicating which rows to keep.
    """
    # Count non-missing values across specified columns
    non_missing_counts: Optional[pa.Array] = None

    for col_name in columns_to_check:
        column = batch.column(col_name)
        is_not_missing = _is_not_missing(column)
        is_not_missing_int = pc.cast(is_not_missing, pa.int32())

        if non_missing_counts is None:
            non_missing_counts = is_not_missing_int
        else:
            non_missing_counts = pc.add(non_missing_counts, is_not_missing_int)

    # Keep rows with at least thresh non-missing values
    return pc.greater_equal(non_missing_counts, pa.scalar(thresh))


def _create_how_mask(
    batch: pa.Table, columns_to_check: List[str], how: str
) -> pa.Array:
    """Create mask for how-based row dropping ('any' or 'all').

    Args:
        batch: The PyArrow table to analyze.
        columns_to_check: List of column names to check for missing values.
        how: Either 'any' (drop if any column has missing values) or
             'all' (drop only if all columns have missing values).

    Returns:
        Boolean array indicating which rows to keep.

    Raises:
        ValueError: If 'how' parameter is not 'any' or 'all'.
    """
    mask: Optional[pa.Array] = None

    for col_name in columns_to_check:
        column = batch.column(col_name)
        is_not_missing = _is_not_missing(column)

        if mask is None:
            mask = is_not_missing
        else:
            if how == "any":
                # Keep rows where ALL checked columns are not missing
                mask = pc.and_(mask, is_not_missing)
            elif how == "all":
                # Keep rows where ANY checked column is not missing
                mask = pc.or_(mask, is_not_missing)
            else:
                raise ValueError(
                    f"Invalid 'how' parameter: {how}. Must be 'any' or 'all'."
                )

    return mask


def _filter_with_schema_preservation(batch: pa.Table, mask: pa.Array) -> pa.Table:
    """Filter table while preserving original schema.

    Applies the boolean mask to filter rows while ensuring the output table
    maintains the same schema as the input, even for empty results.

    Args:
        batch: The PyArrow table to filter.
        mask: Boolean array indicating which rows to keep.

    Returns:
        Filtered PyArrow table with preserved schema.
    """
    filtered_batch = pc.filter(batch, mask)

    # Ensure schema is preserved (important for empty results)
    if filtered_batch.schema != batch.schema:
        try:
            filtered_batch = filtered_batch.cast(batch.schema)
        except pa.ArrowInvalid:
            # If casting fails, create empty table with original schema
            if filtered_batch.num_rows == 0:
                filtered_batch = pa.table(
                    {
                        name: pa.array([], type=field.type)
                        for name, field in zip(batch.schema.names, batch.schema)
                    }
                )

    return filtered_batch
