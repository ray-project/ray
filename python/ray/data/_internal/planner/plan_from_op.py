from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_operators import AbstractFrom


def plan_from_op(
    op: AbstractFrom, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 0
    return InputDataBuffer(op.input_data)
