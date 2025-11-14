"""
Plan DropNa logical operator.
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

            filtered_batch = pc.filter(batch, mask)
            if filtered_batch.schema != batch.schema:
                filtered_batch = filtered_batch.cast(batch.schema)
            return filtered_batch
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
    """Check if values are not missing (null, NaN, or in ignore_values)."""
    is_not_null = pc.is_valid(column)
    if pa.types.is_floating(column.type) or pa.types.is_decimal(column.type):
        is_not_null = pc.and_(is_not_null, pc.invert(pc.is_nan(column)))

    if not ignore_values:
        return is_not_null

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
    return pc.and_(is_not_null, pc.invert(is_ignored_mask))


def _create_thresh_mask(
    batch: pa.Table, columns_to_check: List[str], thresh: int, ignore_values: List[Any]
) -> pa.Array:
    """Create mask for threshold-based row dropping."""
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
    """Create mask for how-based row dropping ('any' or 'all')."""
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
