from typing import Any, Dict, Iterator

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import Block
from ray.data.datasource.datasource import Reader


class Read(LogicalOperator):
    """Logical operator for read."""

    def __init__(
        self,
        reader: Reader,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        read_args: Dict[str, Any] = None,
    ):
        super().__init__("Read", [])
        self._reader = reader
        self._parallelism = parallelism
        self._ray_remote_args = ray_remote_args
        self._read_args = read_args


def plan_read_op(op: Read) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read."""
    inputs = InputDataBuffer(reader=op._reader, read_parallelism=op._parallelism)

    def do_read(blocks: Iterator[Block]) -> Iterator[Block]:
        for read_task in blocks:
            yield from read_task()

    return MapOperator(do_read, inputs, name="DoRead")
