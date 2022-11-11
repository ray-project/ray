from typing import List, Iterator

from ray.data.block import Block
from ray.data._internal.execution.interfaces import (
    RefBundle,
    OneToOneOperator,
    BufferOperator,
    PhysicalOperator,
)
from ray.data._internal.compute import BlockTransform


class InputDataBuffer(BufferOperator):
    """Defines the input data for the operator DAG."""

    def __init__(self, input_data: List[RefBundle]):
        self._input_data = input_data
        super().__init__([])

    def has_next(self) -> bool:
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        return self._input_data.pop(0)


class MapOperator(OneToOneOperator):
    """Defines a simple map operation over blocks."""

    def __init__(self, block_transform: BlockTransform, input_op: PhysicalOperator):
        self._block_transform = block_transform
        super().__init__([input_op])

    def execute_one(self, block_bundle: Iterator[Block], _) -> Iterator[Block]:
        def apply_transform(fn, block_bundle):
            for b in block_bundle:
                yield fn(b)

        return apply_transform(self._block_transform, block_bundle)
