import inspect
import logging
from typing import List, Optional

import pyarrow as pa

from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    AsyncCallableClassUDFMapTransformFn,
    BlocksToBatchesMapTransformFn,
    BlocksToRowsMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    CallableClassUDFMapTransformFn,
    FunctionUDFMapTransformFn,
    MapTransformer,
    MapTransformFn,
    MapTransformFnDataType,
    MapTransformFnOpType,
    MapTransformUDFContext,
    _handle_debugger_exception,
)
from ray.data._internal.logical.operators.map_operator import (
    AbstractUDFMap,
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
    Project,
    StreamingRepartition,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    CallableClass,
)
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def plan_project_op(
    op: Project,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    columns = op.cols
    columns_rename = op.cols_rename

    def fn(block: Block) -> Block:
        try:
            if not BlockAccessor.for_block(block).num_rows():
                return block
            if columns:
                block = BlockAccessor.for_block(block).select(columns)
            if columns_rename:
                block = block.rename_columns(
                    [columns_rename.get(col, col) for col in block.schema.names]
                )
            return block
        except Exception as e:
            _handle_debugger_exception(e, block)

    compute = get_compute(op._compute)
    udf_context = MapTransformUDFContext(
        op_fn=fn,
    )
    transform_fn = FunctionUDFMapTransformFn(
        udf_context,
        input_type=MapTransformFnDataType.Block,
        output_type=MapTransformFnDataType.Block,
        map_transform_fn_type=MapTransformFnOpType.MapBlocks,
    )
    map_transformer = _create_map_transformer_for_block_based_map_op(
        transform_fn,
    )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op._compute)
    transform_fn = BuildOutputBlocksMapTransformFn.for_blocks()
    transform_fn.set_target_num_rows_per_block(op.target_num_rows_per_block)
    map_transformer = MapTransformer([transform_fn])
    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_filter_op(
    op: Filter,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    expression = op._filter_expr
    compute = get_compute(op._compute)
    if expression is not None:

        def filter_batch_fn(block: "pa.Table") -> "pa.Table":
            try:
                return block.filter(expression)
            except Exception as e:
                _handle_debugger_exception(e, block)

        udf_context = MapTransformUDFContext(
            op_fn=filter_batch_fn,
        )
        map_transform_fn = FunctionUDFMapTransformFn(
            udf_context,
            input_type=MapTransformFnDataType.Batch,
            output_type=MapTransformFnDataType.Batch,
            map_transform_fn_type=MapTransformFnOpType.MapBatches,
        )
        map_transformer = _create_map_transformer_for_map_batches_op(
            map_transform_fn,
            batch_size=None,
            batch_format="pyarrow",
            zero_copy_batch=True,
        )
    else:
        udf_context = MapTransformUDFContext(
            op_fn=op._fn,
            fn_args=op._fn_args or (),
            fn_kwargs=op._fn_kwargs or {},
            fn_constructor_args=op._fn_constructor_args or (),
            fn_constructor_kwargs=op._fn_constructor_kwargs or {},
            is_async=inspect.isasyncgenfunction(op._fn.__call__),
        )
        if isinstance(udf_context.op_fn, CallableClass):
            map_transform_fn = CallableClassUDFMapTransformFn(
                udf_context,
                input_type=MapTransformFnDataType.Row,
                output_type=MapTransformFnDataType.Row,
                map_transform_fn_type=MapTransformFnOpType.Filter,
            )
        else:
            map_transform_fn = FunctionUDFMapTransformFn(
                udf_context,
                input_type=MapTransformFnDataType.Row,
                output_type=MapTransformFnDataType.Row,
                map_transform_fn_type=MapTransformFnOpType.Filter,
            )
        map_transformer = _create_map_transformer_for_row_based_map_op(
            map_transform_fn,
        )

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )


def plan_udf_map_op(
    op: AbstractUDFMap,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractUDFMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    compute = get_compute(op._compute)
    op_fn = op._fn
    udf_context = MapTransformUDFContext(
        op_fn=op_fn,
        fn_args=op._fn_args or (),
        fn_kwargs=op._fn_kwargs or {},
        fn_constructor_args=op._fn_constructor_args or (),
        fn_constructor_kwargs=op._fn_constructor_kwargs or {},
        is_async=inspect.isasyncgenfunction(op_fn.__call__),
    )
    if isinstance(op_fn, CallableClass):
        if not udf_context.is_async:
            transform_fn_cls = CallableClassUDFMapTransformFn
        else:
            transform_fn_cls = AsyncCallableClassUDFMapTransformFn
    else:
        transform_fn_cls = FunctionUDFMapTransformFn

    if isinstance(op, MapBatches):
        map_transform_fn = transform_fn_cls(
            udf_context=udf_context,
            input_type=MapTransformFnDataType.Batch,
            output_type=MapTransformFnDataType.Batch,
            map_transform_fn_type=MapTransformFnOpType.MapBatches,
        )
        map_transformer = _create_map_transformer_for_map_batches_op(
            map_transform_fn,
            op._batch_size,
            op._batch_format,
            op._zero_copy_batch,
        )
    else:
        if isinstance(op, MapRows):
            map_transform_fn = transform_fn_cls(
                udf_context=udf_context,
                input_type=MapTransformFnDataType.Row,
                output_type=MapTransformFnDataType.Row,
                map_transform_fn_type=MapTransformFnOpType.MapRows,
            )
        elif isinstance(op, FlatMap):
            map_transform_fn = transform_fn_cls(
                udf_context=udf_context,
                input_type=MapTransformFnDataType.Row,
                output_type=MapTransformFnDataType.Row,
                map_transform_fn_type=MapTransformFnOpType.FlatMap,
            )
            map_transform_fn = transform_fn_cls(
                udf_context=udf_context,
                input_type=MapTransformFnDataType.Row,
                output_type=MapTransformFnDataType.Row,
                map_transform_fn_type=MapTransformFnOpType.FlatMap,
            )
        else:
            raise ValueError(f"Found unknown logical operator during planning: {op}")

        map_transformer = _create_map_transformer_for_row_based_map_op(map_transform_fn)

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        data_context,
        name=op.name,
        target_max_block_size=None,
        compute_strategy=compute,
        min_rows_per_bundle=op._min_rows_per_bundled_input,
        ray_remote_args_fn=op._ray_remote_args_fn,
        ray_remote_args=op._ray_remote_args,
    )


# Following are util functions for creating `MapTransformer`s.


def _create_map_transformer_for_map_batches_op(
    map_transform_fn: MapTransformFn,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    zero_copy_batch: bool = False,
) -> MapTransformer:
    """Create a MapTransformer for a map_batches operator."""
    transform_fns = [
        # Convert input blocks to batches.
        BlocksToBatchesMapTransformFn(
            batch_size=batch_size,
            batch_format=batch_format,
            zero_copy_batch=zero_copy_batch,
        ),
        # Apply the UDF.
        map_transform_fn,
        # Convert output batches to blocks.
        BuildOutputBlocksMapTransformFn.for_batches(),
    ]
    return MapTransformer(transform_fns)


def _create_map_transformer_for_row_based_map_op(
    map_transform_fn: MapTransformFn,
) -> MapTransformer:
    """Create a MapTransformer for a row-based map operator
    (e.g. map, flat_map, filter)."""
    transform_fns = [
        # Convert input blocks to rows.
        BlocksToRowsMapTransformFn.instance(),
        # Apply the UDF.
        map_transform_fn,
        # Convert output rows to blocks.
        BuildOutputBlocksMapTransformFn.for_rows(),
    ]
    return MapTransformer(transform_fns)


def _create_map_transformer_for_block_based_map_op(
    map_transform_fn: MapTransformFn,
) -> MapTransformer:
    """Create a MapTransformer for a block-based map operator."""
    transform_fns = [
        # Apply the UDF.
        map_transform_fn,
        BuildOutputBlocksMapTransformFn.for_blocks(),
    ]
    return MapTransformer(transform_fns)
