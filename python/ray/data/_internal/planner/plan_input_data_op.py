from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.input_data_operator import InputData


def plan_input_data_op(
    op: InputData, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for InputData."""
    assert len(physical_children) == 0

    return InputDataBuffer(
        input_data=op.input_data, input_data_factory=op.input_data_factory
    )
