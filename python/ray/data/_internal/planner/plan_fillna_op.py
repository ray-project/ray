"""
Plan FillNa logical operator.
"""

from typing import List

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.operators.fillna_operator import FillNa
from ray.data._internal.planner.map_operator import MapOperator
from ray.data._internal.planner.plan_udf_map_op import (
    _create_map_transformer_for_block_based_map_op,
    _generate_transform_fn_for_map_block,
    _try_wrap_udf_exception,
    get_compute,
)
from ray.data.context import DataContext


def plan_fillna_op(
    op: FillNa,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan a FillNa logical operator."""
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    value = op.value
    subset = op.subset

    def fn(batch: pa.Table) -> pa.Table:
        try:
            if batch.num_rows == 0:
                return batch

            # If no subset specified, apply to all columns
            columns_to_fill = subset if subset else batch.schema.names

            # Create a new table with filled values
            new_columns = {}

            for col_name in batch.schema.names:
                column = batch.column(col_name)

                if col_name in columns_to_fill:
                    if isinstance(value, dict):
                        # Column-specific fill values
                        fill_value = value.get(col_name)
                        if fill_value is not None:
                            new_columns[col_name] = _fill_column(column, fill_value)
                        else:
                            new_columns[col_name] = column
                    else:
                        # Scalar fill value for all columns
                        new_columns[col_name] = _fill_column(column, value)
                else:
                    new_columns[col_name] = column

            return pa.table(new_columns)

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
        name="FillNa",
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
    )


def _fill_column(column, fill_value):
    """Fill a column with a value using PyArrow's built-in capabilities."""
    try:
        # For null type columns, let PyArrow infer the type from the fill value
        if pa.types.is_null(column.type):
            # Create a new array with the fill value repeated for each row
            fill_array = pa.array([fill_value] * len(column))
            return fill_array
        
        # For regular columns, try to create a scalar with the column's type
        try:
            fill_scalar = pa.scalar(fill_value, type=column.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # If type conversion fails, let PyArrow handle it by inferring from value
            fill_scalar = pa.scalar(fill_value)
            # Try to cast to column type, but if it fails, PyArrow will handle the type promotion
            try:
                fill_scalar = fill_scalar.cast(column.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                # If casting fails, use the inferred type
                pass

        # Use PyArrow's fill_null to handle null values
        filled_column = pc.fill_null(column, fill_scalar)
        
        # For floating point columns, also handle NaN values
        if pa.types.is_floating(filled_column.type):
            nan_mask = pc.is_nan(filled_column)
            filled_column = pc.if_else(nan_mask, fill_scalar, filled_column)
        
        return filled_column

    except Exception:
        # If all else fails, return original column
        return column 