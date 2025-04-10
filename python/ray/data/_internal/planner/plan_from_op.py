from typing import List

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_blocks_operator import FromBlocks
from ray.data.context import DataContext


def plan_from_op(
    op: FromBlocks,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    def input_data_factory(num_output_blocks: int) -> List[RefBundle]:
        return [
            RefBundle(
                [(ray.put(block), metadata)],
                # `owns_blocks` is False because this op may be shared by multiple
                # Datasets.
                owns_blocks=False,
            )
            for block, metadata in zip(op.input_blocks, op.input_metadata)
        ]

    assert len(physical_children) == 0
    return InputDataBuffer(
        data_context,
        input_data_factory=input_data_factory,
    )
