from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_operators import AbstractFrom


def plan_from_op(op: AbstractFrom) -> PhysicalOperator:
    return InputDataBuffer(op.input_data)
