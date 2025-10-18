from typing import List

from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.streaming_repartition_operator import (
    StreamingRepartitionOperator,
)
from ray.data._internal.logical.operators.streaming_repartition import (
    StreamingRepartition,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.context import DataContext


def plan_streaming_repartition_op(
    op: StreamingRepartition,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    if op.enforce_target_num_rows_per_block:
        return StreamingRepartitionOperator(
            target_num_rows_per_block=op.target_num_rows_per_block,
            input_op=input_physical_dag,
            data_context=data_context,
        )
    else:
        # Create a no-op transform that is just coalescing/slicing the incoming
        # blocks
        transform_fn = BlockMapTransformFn(
            lambda blocks, ctx: blocks,
            output_block_size_option=OutputBlockSizeOption.of(
                target_num_rows_per_block=op.target_num_rows_per_block
            ),
        )

        map_transformer = MapTransformer([transform_fn])
        compute = get_compute(op._compute)
        # Disable fusion for streaming repartition with the downstream op.
        return MapOperator.create(
            map_transformer,
            input_physical_dag,
            data_context,
            name=op.name,
            compute_strategy=compute,
            supports_fusion=False,
        )
