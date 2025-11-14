"""
Plan FillNa logical operator.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional

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

    if op.method in ["forward", "backward"]:
        transform_fn = _create_stateful_directional_fill_fn(op)
    else:

        def fn(batch: pa.Table) -> pa.Table:
            try:
                if batch.num_rows == 0:
                    return batch

                columns_to_fill = op.subset if op.subset else batch.schema.names

                if isinstance(op.value, dict):
                    invalid_keys = [
                        k for k in op.value.keys() if k not in batch.schema.names
                    ]
                    if invalid_keys:
                        raise ValueError(
                            f"fill_value dict contains keys not in dataset: {invalid_keys}"
                        )

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
    """Fill missing values in a PyArrow column."""
    if column.null_count == 0:
        if not (pa.types.is_floating(column.type) or pa.types.is_decimal(column.type)):
            return column
        if pc.sum(pc.is_nan(column)).as_py() == 0:
            return column

    if pa.types.is_null(column.type):
        return pa.array([fill_value] * len(column))

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

    filled_column = pc.fill_null(column, fill_scalar)
    if pa.types.is_floating(filled_column.type):
        nan_mask = pc.is_nan(filled_column)
        filled_column = pc.if_else(nan_mask, fill_scalar, filled_column)

    return filled_column


def _validate_batch(batch: pa.Table, op: FillNa) -> None:
    """Validate batch for fillna operation."""
    schema_names = batch.schema.names
    if isinstance(op.value, dict):
        invalid_keys = [k for k in op.value.keys() if k not in schema_names]
        if invalid_keys:
            raise ValueError(
                f"fill_value dict contains keys not in dataset: {invalid_keys}"
            )
    if op.subset and len(op.subset) != len(set(op.subset)):
        raise ValueError("subset parameter contains duplicate column names")


def _create_fill_scalar(
    fill_value: Any, column_type: pa.DataType
) -> Optional[pa.Scalar]:
    """Create PyArrow scalar from fill value."""
    try:
        return pa.scalar(fill_value, type=column_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        try:
            return pa.scalar(fill_value).cast(column_type)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return None


def _find_first_valid_index(column: pa.Array) -> Optional[int]:
    """Find first non-null, non-NaN index."""
    for i in range(len(column)):
        if not pc.is_null(column[i]).as_py():
            if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
                if not pc.is_nan(column[i]).as_py():
                    return i
            else:
                return i
    return None


def _find_last_valid_index(column: pa.Array) -> Optional[int]:
    """Find last non-null, non-NaN index."""
    for i in range(len(column) - 1, -1, -1):
        if not pc.is_null(column[i]).as_py():
            if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
                if not pc.is_nan(column[i]).as_py():
                    return i
            else:
                return i
    return None


def _fill_leading_nulls(
    column: pa.Array, fill_scalar: pa.Scalar, first_valid_idx: int
) -> pa.Array:
    """Fill leading nulls/NaNs up to first valid index."""
    is_floating = pa.types.is_floating(column.type) or pa.types.is_decimal(column.type)
    leading_nulls = pa.array(
        [
            (
                pc.is_null(column[i]).as_py()
                or (is_floating and pc.is_nan(column[i]).as_py())
            )
            if i < first_valid_idx
            else False
            for i in range(len(column))
        ]
    )
    return pc.if_else(leading_nulls, fill_scalar, column)


def _process_batch_for_directional_fill(
    batch: pa.Table,
    op: FillNa,
    columns_to_fill: List[str],
    carry_over_values: Dict[str, Any],
    method: str,
    limit: Optional[int],
    track_valid_fn: Callable[[pa.Array], Optional[int]],
) -> tuple[pa.Table, Dict[str, Any]]:
    """Process a batch for directional fill, returning filled batch and updated carry-over values."""
    _validate_batch(batch, op)
    new_columns: Dict[str, pa.Array] = {}
    new_carry_over: Dict[str, Any] = {}

    for col_name in batch.schema.names:
        column = batch.column(col_name)
        if col_name in columns_to_fill:
            if col_name in carry_over_values:
                fill_scalar = _create_fill_scalar(
                    carry_over_values[col_name], column.type
                )
                if fill_scalar is not None:
                    first_valid_idx = _find_first_valid_index(column)
                    if first_valid_idx is not None and first_valid_idx > 0:
                        column = _fill_leading_nulls(
                            column, fill_scalar, first_valid_idx
                        )

            filled_column = _fill_column_directional(column, method, limit)
            valid_idx = track_valid_fn(filled_column)
            if valid_idx is not None:
                new_carry_over[col_name] = filled_column[valid_idx].as_py()

            new_columns[col_name] = filled_column
        else:
            new_columns[col_name] = column

    return pa.table(new_columns), new_carry_over


def _create_stateful_directional_fill_fn(
    op: FillNa,
) -> MapTransformCallable[Block, Block]:
    """Create stateful transform for forward/backward fill across batches."""
    columns_to_fill = op.subset
    method = op.method
    limit = op.limit

    def transform_fn(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        last_valid_values: Dict[str, Any] = {}

        if method == "backward":
            batch_list = [BlockAccessor.for_block(block).to_block() for block in blocks]
            result_batches = []
            next_first_valid: Dict[str, Any] = {}

            for batch_idx in range(len(batch_list) - 1, -1, -1):
                batch = batch_list[batch_idx]
                if batch.num_rows == 0:
                    result_batches.append(BlockAccessor.for_block(batch).to_block())
                    continue

                try:
                    cols_to_fill = (
                        columns_to_fill if columns_to_fill else batch.schema.names
                    )
                    result_batch, new_carry_over = _process_batch_for_directional_fill(
                        batch,
                        op,
                        cols_to_fill,
                        next_first_valid,
                        method,
                        limit,
                        _find_first_valid_index,
                    )
                    next_first_valid = new_carry_over
                    result_batches.append(
                        BlockAccessor.for_block(result_batch).to_block()
                    )
                except Exception as e:
                    _try_wrap_udf_exception(e, batch)

            for batch in reversed(result_batches):
                yield batch

        else:  # forward fill
            for block in blocks:
                batch = BlockAccessor.for_block(block).to_block()
                if batch.num_rows == 0:
                    yield BlockAccessor.for_block(batch).to_block()
                    continue

                try:
                    cols_to_fill = (
                        columns_to_fill if columns_to_fill else batch.schema.names
                    )
                    result_batch, new_carry_over = _process_batch_for_directional_fill(
                        batch,
                        op,
                        cols_to_fill,
                        last_valid_values,
                        method,
                        limit,
                        _find_last_valid_index,
                    )
                    last_valid_values = new_carry_over
                    yield BlockAccessor.for_block(result_batch).to_block()
                except Exception as e:
                    _try_wrap_udf_exception(e, batch)

    return transform_fn


def _fill_column_directional(
    column: pa.Array, method: str, limit: Optional[int] = None
) -> pa.Array:
    """Fill missing values using forward or backward fill within a batch."""
    if column.null_count == 0:
        if not (pa.types.is_floating(column.type) or pa.types.is_decimal(column.type)):
            return column
        if pc.sum(pc.is_nan(column)).as_py() == 0:
            return column
    pd_series = column.to_pandas()
    if method == "forward":
        filled_series = pd_series.ffill(limit=limit)
    else:
        filled_series = pd_series.bfill(limit=limit)
    return pa.array(filled_series, type=column.type)


def _fill_column_interpolate(column: pa.Array, limit: Optional[int] = None) -> pa.Array:
    """Fill missing values using linear interpolation."""
    if column.null_count == 0:
        if not (pa.types.is_floating(column.type) or pa.types.is_decimal(column.type)):
            return column
        if pc.sum(pc.is_nan(column)).as_py() == 0:
            return column
    if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
        return column
    pd_series = column.to_pandas()
    filled_series = pd_series.interpolate(method="linear", limit=limit)
    return pa.array(filled_series, type=column.type)
