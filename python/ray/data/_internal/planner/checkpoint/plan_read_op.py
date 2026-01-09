import functools
from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
)
from ray.data._internal.logical.operators import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data.checkpoint.util import (
    filter_checkpointed_rows_for_blocks,
)
from ray.data.context import DataContext


def plan_read_op_with_checkpoint_filter(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    physical_op = plan_read_op(op, physical_children, data_context)

    # TODO avoid modifying in-place
    physical_op._map_transformer.add_transform_fns(
        [
            BlockMapTransformFn(
                functools.partial(
                    filter_checkpointed_rows_for_blocks,
                ),
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size,
                ),
            ),
        ]
    )

    return physical_op
