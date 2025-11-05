"""
Plan FillNa logical operator.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.fillna_operator import FillNa
from ray.data._internal.planner.plan_udf_map_op import (
    _create_map_transformer_for_block_based_map_op,
    _generate_transform_fn_for_map_block,
    get_compute,
)
from ray.data.context import DataContext


def plan_fillna_op(
    op: FillNa,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan a FillNa logical operator into a physical MapOperator."""
    assert len(physical_children) == 1

    def fn(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch

        columns_to_fill = op.subset if op.subset else batch.schema.names
        new_columns: Dict[str, pa.Array] = {}

        for col_name in batch.schema.names:
            column = batch.column(col_name)
            if col_name in columns_to_fill:
                if op.method == "value":
                    if isinstance(op.value, dict):
                        fill_value = op.value.get(col_name)
                        if fill_value is not None:
                            new_columns[col_name] = _fill_column(column, fill_value)
                        else:
                            new_columns[col_name] = column
                    else:
                        new_columns[col_name] = _fill_column(column, op.value)
                elif op.method in ["forward", "backward"]:
                    new_columns[col_name] = _fill_column_directional(column, op.method, op.limit)
                elif op.method == "interpolate":
                    new_columns[col_name] = _fill_column_interpolate(column, op.limit)
                else:
                    raise ValueError(f"Unsupported fill method: {op.method}")
            else:
                new_columns[col_name] = column

        return pa.table(new_columns)

    compute = get_compute(op._compute)
    transform_fn = _generate_transform_fn_for_map_block(fn)
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
    """Fill missing values in a single PyArrow column."""
    try:
        if pa.types.is_null(column.type):
            return pa.array([fill_value] * len(column))

        if pa.types.is_string(column.type):
            fill_scalar = pa.scalar(str(fill_value), type=column.type)
        else:
            try:
                fill_scalar = pa.scalar(fill_value, type=column.type)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                fill_scalar = pa.scalar(fill_value)
                try:
                    fill_scalar = fill_scalar.cast(column.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    pass

        filled_column = pc.fill_null(column, fill_scalar)
        if pa.types.is_floating(filled_column.type):
            nan_mask = pc.is_nan(filled_column)
            filled_column = pc.if_else(nan_mask, fill_scalar, filled_column)

        return filled_column
    except Exception:
        return column


def _fill_column_directional(
    column: pa.Array, method: str, limit: Optional[int] = None
) -> pa.Array:
    """Fill missing values using forward or backward fill."""
    try:
        pd_series = column.to_pandas()
        fill_method = "ffill" if method == "forward" else "bfill"
        filled_series = pd_series.fillna(method=fill_method, limit=limit)
        return pa.array(filled_series, type=column.type)
    except Exception:
        return column


def _fill_column_interpolate(column: pa.Array, limit: Optional[int] = None) -> pa.Array:
    """Fill missing values using linear interpolation."""
    try:
        if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
            return column
        pd_series = column.to_pandas()
        filled_series = pd_series.interpolate(method="linear", limit=limit)
        return pa.array(filled_series, type=column.type)
    except Exception:
        return column
