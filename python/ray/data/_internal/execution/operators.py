from typing import List, Iterator, Optional

from ray.data.block import Block
from ray.data._internal.execution.interfaces import (
    RefBundle,
    OneToOneOperator,
    ExchangeOperator,
    PhysicalOperator,
)
from ray.data._internal.compute import BlockTransform
from ray.data._internal.execution.util import _make_ref_bundles


class InputDataBuffer(ExchangeOperator):
    """Defines the input data for the operator DAG."""

    def __init__(self, input_data: List[RefBundle]):
        self._input_data = input_data
        self._num_outputs = len(input_data)
        super().__init__("Input", [])

    def has_next(self) -> bool:
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        return self._input_data.pop(0)

    def num_outputs_total(self) -> Optional[int]:
        return self._num_outputs


class MapOperator(OneToOneOperator):
    """Defines a simple map operation over blocks."""

    def __init__(
        self,
        block_transform: BlockTransform,
        input_op: PhysicalOperator,
        name: str = "Map",
    ):
        self._block_transform = block_transform
        super().__init__(name, [input_op])

    def execute_one(self, block_bundle: Iterator[Block], _) -> Iterator[Block]:
        def apply_transform(fn, block_bundle):
            for b in block_bundle:
                yield fn(b)

        return apply_transform(self._block_transform, block_bundle)


# For testing only.
def _from_dataset_read_tasks(ds) -> PhysicalOperator:
    read_tasks = ds._plan._snapshot_blocks._tasks
    inputs = InputDataBuffer(_make_ref_bundles([[r] for r in read_tasks]))

    def do_read(block):
        for read_task in block:
            for output_block in read_task():
                return output_block  # TODO handle remaining blocks

    return MapOperator(do_read, inputs, name="DoRead")
