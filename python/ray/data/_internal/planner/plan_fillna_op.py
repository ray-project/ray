"""
Plan FillNa logical operator.
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
                    schema_names = batch.schema.names
                    cols_to_fill = columns_to_fill if columns_to_fill else schema_names

                    if isinstance(op.value, dict):
                        invalid_keys = [
                            k for k in op.value.keys() if k not in schema_names
                        ]
                        if invalid_keys:
                            raise ValueError(
                                f"fill_value dict contains keys not in dataset: {invalid_keys}"
                            )
                    if op.subset and len(op.subset) != len(set(op.subset)):
                        raise ValueError(
                            "subset parameter contains duplicate column names"
                        )

                    new_columns: Dict[str, pa.Array] = {}
                    for col_name in schema_names:
                        column = batch.column(col_name)
                        if col_name in cols_to_fill:
                            if col_name in next_first_valid:
                                fill_value = next_first_valid[col_name]
                                try:
                                    fill_scalar = pa.scalar(
                                        fill_value, type=column.type
                                    )
                                except (pa.ArrowInvalid, pa.ArrowTypeError):
                                    try:
                                        fill_scalar = pa.scalar(fill_value).cast(
                                            column.type
                                        )
                                    except (
                                        pa.ArrowInvalid,
                                        pa.ArrowNotImplementedError,
                                    ):
                                        fill_scalar = None

                                if fill_scalar is not None:
                                    first_non_null_idx = None
                                    for i in range(len(column)):
                                        if not pc.is_null(column[i]).as_py():
                                            if pa.types.is_floating(
                                                column.type
                                            ) or pa.types.is_decimal(column.type):
                                                if not pc.is_nan(column[i]).as_py():
                                                    first_non_null_idx = i
                                                    break
                                            else:
                                                first_non_null_idx = i
                                                break

                                    if (
                                        first_non_null_idx is not None
                                        and first_non_null_idx > 0
                                    ):
                                        leading_nulls = pa.array(
                                            [
                                                pc.is_null(column[i]).as_py()
                                                if i < first_non_null_idx
                                                else False
                                                for i in range(len(column))
                                            ]
                                        )
                                        column = pc.if_else(
                                            leading_nulls, fill_scalar, column
                                        )

                            filled_column = _fill_column_directional(
                                column, method, limit
                            )

                            for i in range(len(filled_column)):
                                if not pc.is_null(filled_column[i]).as_py():
                                    if pa.types.is_floating(
                                        filled_column.type
                                    ) or pa.types.is_decimal(filled_column.type):
                                        if not pc.is_nan(filled_column[i]).as_py():
                                            next_first_valid[col_name] = filled_column[
                                                i
                                            ].as_py()
                                            break
                                    else:
                                        next_first_valid[col_name] = filled_column[
                                            i
                                        ].as_py()
                                        break

                            new_columns[col_name] = filled_column
                        else:
                            new_columns[col_name] = column

                    result_batch = pa.table(new_columns)
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
                    schema_names = batch.schema.names
                    cols_to_fill = columns_to_fill if columns_to_fill else schema_names

                    if isinstance(op.value, dict):
                        invalid_keys = [
                            k for k in op.value.keys() if k not in schema_names
                        ]
                        if invalid_keys:
                            raise ValueError(
                                f"fill_value dict contains keys not in dataset: {invalid_keys}"
                            )
                    if op.subset and len(op.subset) != len(set(op.subset)):
                        raise ValueError(
                            "subset parameter contains duplicate column names"
                        )

                    new_columns: Dict[str, pa.Array] = {}
                    for col_name in schema_names:
                        column = batch.column(col_name)
                        if col_name in cols_to_fill:
                            if col_name in last_valid_values:
                                fill_value = last_valid_values[col_name]
                                try:
                                    fill_scalar = pa.scalar(
                                        fill_value, type=column.type
                                    )
                                except (pa.ArrowInvalid, pa.ArrowTypeError):
                                    try:
                                        fill_scalar = pa.scalar(fill_value).cast(
                                            column.type
                                        )
                                    except (
                                        pa.ArrowInvalid,
                                        pa.ArrowNotImplementedError,
                                    ):
                                        fill_scalar = None

                                if fill_scalar is not None:
                                    first_non_null_idx = None
                                    for i in range(len(column)):
                                        if not pc.is_null(column[i]).as_py():
                                            if pa.types.is_floating(
                                                column.type
                                            ) or pa.types.is_decimal(column.type):
                                                if not pc.is_nan(column[i]).as_py():
                                                    first_non_null_idx = i
                                                    break
                                            else:
                                                first_non_null_idx = i
                                                break

                                    if (
                                        first_non_null_idx is not None
                                        and first_non_null_idx > 0
                                    ):
                                        leading_nulls = pa.array(
                                            [
                                                pc.is_null(column[i]).as_py()
                                                if i < first_non_null_idx
                                                else False
                                                for i in range(len(column))
                                            ]
                                        )
                                        column = pc.if_else(
                                            leading_nulls, fill_scalar, column
                                        )

                            filled_column = _fill_column_directional(
                                column, method, limit
                            )

                            for i in range(len(filled_column) - 1, -1, -1):
                                if not pc.is_null(filled_column[i]).as_py():
                                    if pa.types.is_floating(
                                        filled_column.type
                                    ) or pa.types.is_decimal(filled_column.type):
                                        if not pc.is_nan(filled_column[i]).as_py():
                                            last_valid_values[col_name] = filled_column[
                                                i
                                            ].as_py()
                                            break
                                    else:
                                        last_valid_values[col_name] = filled_column[
                                            i
                                        ].as_py()
                                        break

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
    """Fill missing values using forward or backward fill within a batch."""
    if column.null_count == 0:
        return column
    pd_series = column.to_pandas()
    fill_method = "ffill" if method == "forward" else "bfill"
    filled_series = pd_series.fillna(method=fill_method, limit=limit)
    return pa.array(filled_series, type=column.type)


def _fill_column_interpolate(column: pa.Array, limit: Optional[int] = None) -> pa.Array:
    """Fill missing values using linear interpolation."""
    if column.null_count == 0:
        return column
    if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
        return column
    pd_series = column.to_pandas()
    filled_series = pd_series.interpolate(method="linear", limit=limit)
    return pa.array(filled_series, type=column.type)
