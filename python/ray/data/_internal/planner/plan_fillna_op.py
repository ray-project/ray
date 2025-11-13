"""
Plan FillNa logical operator.

This module implements the physical planning for FillNa operations, which fill
missing values in Ray Data datasets. It supports multiple fill methods:
- value: Fill with specified values
- forward: Forward fill (propagate last valid observation forward)
- backward: Backward fill (propagate next valid observation backward)
- interpolate: Linear interpolation (numeric columns only)

The implementation uses PyArrow for efficient columnar operations.
See https://arrow.apache.org/docs/python/api/compute.html for PyArrow compute functions.
"""

from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import MapTransformCallable
from ray.data._internal.logical.operators.fillna_operator import FillNa
from ray.data._internal.planner.plan_udf_map_op import (
    _create_map_transformer_for_block_based_map_op,
    _try_wrap_udf_exception,
    get_compute,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext


def plan_fillna_op(
    op: FillNa,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan a FillNa logical operator into a physical MapOperator."""
    assert len(physical_children) == 1

    # For forward/backward fill, we need stateful processing across batches
    if op.method in ["forward", "backward"]:
        transform_fn = _create_stateful_directional_fill_fn(op)
    else:
        # For value and interpolate methods, process batches independently
        def fn(batch: pa.Table) -> pa.Table:
            try:
                if batch.num_rows == 0:
                    return batch

                columns_to_fill = op.subset if op.subset else batch.schema.names

                # Validate dict keys exist in dataset
                if isinstance(op.value, dict):
                    invalid_keys = [
                        k for k in op.value.keys() if k not in batch.schema.names
                    ]
                    if invalid_keys:
                        raise ValueError(
                            f"fill_value dict contains keys not in dataset: {invalid_keys}"
                        )

                # Validate subset doesn't contain duplicates
                if op.subset and len(op.subset) != len(set(op.subset)):
                    raise ValueError("subset parameter contains duplicate column names")

                new_columns: Dict[str, pa.Array] = {}

                for col_name in batch.schema.names:
                    column = batch.column(col_name)
                    if col_name in columns_to_fill:
                        if op.method == "value":
                            if isinstance(op.value, dict):
                                fill_value = op.value.get(col_name)
                                if fill_value is not None:
                                    new_columns[col_name] = _fill_column(
                                        column, fill_value
                                    )
                                else:
                                    new_columns[col_name] = column
                            else:
                                new_columns[col_name] = _fill_column(column, op.value)
                        elif op.method == "interpolate":
                            new_columns[col_name] = _fill_column_interpolate(
                                column, op.limit
                            )
                        else:
                            raise ValueError(f"Unsupported fill method: {op.method}")
                    else:
                        new_columns[col_name] = column

                return pa.table(new_columns)
            except Exception as e:
                _try_wrap_udf_exception(e, batch)

        from ray.data._internal.planner.plan_udf_map_op import (
            _generate_transform_fn_for_map_block,
        )

        transform_fn = _generate_transform_fn_for_map_block(fn)

    compute = get_compute(op._compute)
    map_transformer = _create_map_transformer_for_block_based_map_op(transform_fn)

    return MapOperator.create(
        map_transformer,
        physical_children[0],
        data_context,
        name="FillNa",
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
    )


def _fill_column(column: pa.Array, fill_value: Any) -> pa.Array:
    """Fill missing values in a single PyArrow column.

    Uses PyArrow compute functions for efficient null filling.
    See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.fill_null

    Args:
        column: PyArrow array to fill nulls in.
        fill_value: Value to use for filling missing entries.

    Returns:
        PyArrow array with nulls filled.

    Raises:
        ValueError: If fill_value cannot be converted to column type.
    """
    # Early exit: check if column has any nulls or NaNs
    if column.null_count == 0:
        if not (pa.types.is_floating(column.type) or pa.types.is_decimal(column.type)):
            return column
        if pc.sum(pc.is_nan(column)).as_py() == 0:
            return column

    if pa.types.is_null(column.type):
        return pa.array([fill_value] * len(column))

    # Create scalar for fill value, handling type conversion
    # See https://arrow.apache.org/docs/python/api/scalars.html
    if pa.types.is_string(column.type):
        fill_scalar = pa.scalar(str(fill_value), type=column.type)
    else:
        try:
            fill_scalar = pa.scalar(fill_value, type=column.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            try:
                fill_scalar = pa.scalar(fill_value).cast(column.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                raise ValueError(
                    f"Cannot convert fill_value {fill_value!r} to column type {column.type}"
                ) from None

    # Fill nulls using PyArrow compute function
    # See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.fill_null
    filled_column = pc.fill_null(column, fill_scalar)
    # Also fill NaN values for floating point types
    if pa.types.is_floating(filled_column.type):
        nan_mask = pc.is_nan(filled_column)
        filled_column = pc.if_else(nan_mask, fill_scalar, filled_column)

    return filled_column


def _validate_fillna_batch(
    batch: pa.Table, op: FillNa, columns_to_fill: List[str]
) -> None:
    """Validate batch for fillna operation.

    Args:
        batch: PyArrow table to validate.
        op: FillNa operator with parameters.
        columns_to_fill: List of column names to fill.

    Raises:
        ValueError: If validation fails.
    """
    schema_names = batch.schema.names

    # Validate dict keys exist in dataset
    if isinstance(op.value, dict):
        invalid_keys = [k for k in op.value.keys() if k not in schema_names]
        if invalid_keys:
            raise ValueError(
                f"fill_value dict contains keys not in dataset: {invalid_keys}"
            )

    # Validate subset doesn't contain duplicates
    if op.subset and len(op.subset) != len(set(op.subset)):
        raise ValueError("subset parameter contains duplicate column names")


def _find_first_non_null_index(
    column: pa.Array, start: int = 0, end: Optional[int] = None
) -> Optional[int]:
    """Find the first non-null, non-NaN value in a column.

    Args:
        column: PyArrow array to search.
        start: Starting index (inclusive).
        end: Ending index (exclusive). If None, searches to end.

    Returns:
        Index of first valid value, or None if not found.
    """
    if end is None:
        end = len(column)

    for i in range(start, end):
        if not pc.is_null(column[i]).as_py():
            # For floating point types, also check for NaN
            if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
                if not pc.is_nan(column[i]).as_py():
                    return i
            else:
                return i
    return None


def _find_last_non_null_index(
    column: pa.Array, start: Optional[int] = None, end: Optional[int] = None
) -> Optional[int]:
    """Find the last non-null, non-NaN value in a column.

    Args:
        column: PyArrow array to search.
        start: Starting index (inclusive). If None, starts from end.
        end: Ending index (exclusive). If None, searches from start.

    Returns:
        Index of last valid value, or None if not found.
    """
    if start is None:
        start = len(column) - 1
    if end is None:
        end = -1

    for i in range(start, end, -1):
        if not pc.is_null(column[i]).as_py():
            # For floating point types, also check for NaN
            if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
                if not pc.is_nan(column[i]).as_py():
                    return i
            else:
                return i
    return None


def _create_fill_scalar(
    fill_value: Any, column_type: pa.DataType
) -> Optional[pa.Scalar]:
    """Create a PyArrow scalar from fill value, handling type conversion.

    Args:
        fill_value: Value to convert to scalar.
        column_type: Target PyArrow data type.

    Returns:
        PyArrow scalar, or None if conversion fails.
    """
    try:
        return pa.scalar(fill_value, type=column_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        try:
            return pa.scalar(fill_value).cast(column_type)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return None


def _fill_leading_nulls(
    column: pa.Array, fill_scalar: pa.Scalar, first_non_null_idx: int
) -> pa.Array:
    """Fill leading nulls in a column up to the first non-null index.

    Uses PyArrow compute if_else for conditional filling.
    See https://arrow.apache.org/docs/python/api/compute.html#arrow.compute.if_else

    Args:
        column: PyArrow array to fill.
        fill_scalar: Scalar value to use for filling.
        first_non_null_idx: Index of first non-null value.

    Returns:
        PyArrow array with leading nulls filled.
    """
    leading_nulls = pa.array(
        [
            pc.is_null(column[i]).as_py() if i < first_non_null_idx else False
            for i in range(len(column))
        ]
    )
    return pc.if_else(leading_nulls, fill_scalar, column)


def _create_stateful_directional_fill_fn(
    op: FillNa,
) -> MapTransformCallable[Block, Block]:
    """Create a stateful transform function for forward/backward fill across batches.

    Forward fill propagates the last valid value forward to fill nulls.
    Backward fill propagates the next valid value backward to fill nulls.

    For backward fill, we need to buffer all batches and process in reverse order
    because backward fill requires knowledge of future values.

    Args:
        op: FillNa operator with method, subset, and limit parameters.

    Returns:
        Transform function that processes blocks with stateful fill logic.
    """
    columns_to_fill = op.subset
    method = op.method
    limit = op.limit

    def transform_fn(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        # State: track last valid value per column (for forward fill)
        last_valid_values: Dict[str, Any] = {}

        # For backward fill, we need to buffer batches to apply fills correctly
        # Backward fill fills missing values with the NEXT valid value, so we need
        # to process batches in reverse order and fill leading nulls with values from
        # the "next" batch (which is the previous batch in reverse order)
        if method == "backward":
            # Buffer all batches
            batch_list = []
            for block in blocks:
                batch = BlockAccessor.for_block(block).to_block()
                batch_list.append(batch)

            # Process batches in reverse order
            result_batches = []
            next_first_valid: Dict[str, Any] = {}

            for batch_idx in range(len(batch_list) - 1, -1, -1):
                batch = batch_list[batch_idx]
                if batch.num_rows == 0:
                    result_batches.append(BlockAccessor.for_block(batch).to_block())
                    continue

                try:
                    schema_names = batch.schema.names
                    cols_to_fill = columns_to_fill if columns_to_fill else schema_names

                    # Validate batch
                    _validate_fillna_batch(batch, op, cols_to_fill)

                    new_columns: Dict[str, pa.Array] = {}

                    for col_name in schema_names:
                        column = batch.column(col_name)
                        if col_name in cols_to_fill:
                            # Fill leading nulls with first valid value from next batch
                            if col_name in next_first_valid:
                                fill_value = next_first_valid[col_name]
                                fill_scalar = _create_fill_scalar(
                                    fill_value, column.type
                                )

                                if fill_scalar is not None:
                                    first_non_null_idx = _find_first_non_null_index(
                                        column
                                    )
                                    if (
                                        first_non_null_idx is not None
                                        and first_non_null_idx > 0
                                    ):
                                        column = _fill_leading_nulls(
                                            column, fill_scalar, first_non_null_idx
                                        )

                            # Apply backward fill within batch
                            filled_column = _fill_column_directional(
                                column, method, limit
                            )

                            # Track first valid value for next batch (previous in reverse order)
                            first_idx = _find_first_non_null_index(filled_column)
                            if first_idx is not None:
                                next_first_valid[col_name] = filled_column[
                                    first_idx
                                ].as_py()

                            new_columns[col_name] = filled_column
                        else:
                            new_columns[col_name] = column

                    result_batch = pa.table(new_columns)
                    result_batches.append(
                        BlockAccessor.for_block(result_batch).to_block()
                    )
                except Exception as e:
                    _try_wrap_udf_exception(e, batch)

            # Yield batches in reverse order (to restore original order)
            for batch in reversed(result_batches):
                yield batch

        else:  # forward fill
            for block in blocks:
                batch = BlockAccessor.for_block(block).to_block()
                if batch.num_rows == 0:
                    yield BlockAccessor.for_block(batch).to_block()
                    continue

                try:
                    schema_names = batch.schema.names
                    cols_to_fill = columns_to_fill if columns_to_fill else schema_names

                    # Validate batch
                    _validate_fillna_batch(batch, op, cols_to_fill)

                    new_columns: Dict[str, pa.Array] = {}

                    for col_name in schema_names:
                        column = batch.column(col_name)
                        if col_name in cols_to_fill:
                            # Fill beginning of batch with last valid value from previous batch
                            if col_name in last_valid_values:
                                fill_value = last_valid_values[col_name]
                                fill_scalar = _create_fill_scalar(
                                    fill_value, column.type
                                )

                                if fill_scalar is not None:
                                    first_non_null_idx = _find_first_non_null_index(
                                        column
                                    )
                                    if (
                                        first_non_null_idx is not None
                                        and first_non_null_idx > 0
                                    ):
                                        column = _fill_leading_nulls(
                                            column, fill_scalar, first_non_null_idx
                                        )

                            # Apply forward fill within batch
                            filled_column = _fill_column_directional(
                                column, method, limit
                            )

                            # Update last valid value for next batch
                            last_idx = _find_last_non_null_index(filled_column)
                            if last_idx is not None:
                                last_valid_values[col_name] = filled_column[
                                    last_idx
                                ].as_py()

                            new_columns[col_name] = filled_column
                        else:
                            new_columns[col_name] = column

                    result_batch = pa.table(new_columns)
                    yield BlockAccessor.for_block(result_batch).to_block()
                except Exception as e:
                    _try_wrap_udf_exception(e, batch)

    return transform_fn


def _fill_column_directional(
    column: pa.Array, method: str, limit: Optional[int] = None
) -> pa.Array:
    """Fill missing values using forward or backward fill within a single batch.

    Uses pandas fillna for forward/backward fill as PyArrow doesn't have native
    support for these operations. See pandas documentation:
    https://pandas.pydata.org/docs/reference/api/pandas.Series.fillna.html

    Args:
        column: PyArrow array to fill.
        method: Fill method ("forward" or "backward").
        limit: Maximum number of consecutive missing values to fill.

    Returns:
        PyArrow array with nulls filled using directional method.
    """
    if column.null_count == 0:
        return column
    pd_series = column.to_pandas()
    fill_method = "ffill" if method == "forward" else "bfill"
    filled_series = pd_series.fillna(method=fill_method, limit=limit)
    return pa.array(filled_series, type=column.type)


def _fill_column_interpolate(column: pa.Array, limit: Optional[int] = None) -> pa.Array:
    """Fill missing values using linear interpolation.

    Uses pandas interpolate for linear interpolation as PyArrow doesn't have native
    support for interpolation. See pandas documentation:
    https://pandas.pydata.org/docs/reference/api/pandas.Series.interpolate.html

    Args:
        column: PyArrow array to interpolate (must be numeric).
        limit: Maximum number of consecutive missing values to interpolate.

    Returns:
        PyArrow array with nulls filled using linear interpolation.
    """
    if column.null_count == 0:
        return column
    if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
        return column
    pd_series = column.to_pandas()
    filled_series = pd_series.interpolate(method="linear", limit=limit)
    return pa.array(filled_series, type=column.type)
