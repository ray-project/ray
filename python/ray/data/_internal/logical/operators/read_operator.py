from typing import Any, Dict, Iterator, List

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.execution.interfaces import RefBundle, PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import ReadTask
from ray.data.datasource.datasource import Datasource


class Read(LogicalOperator):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        read_args: Dict[str, Any] = None,
    ):
        super().__init__("Read", [])
        self._datasource = datasource
        self._parallelism = parallelism
        self._ray_remote_args = ray_remote_args
        self._read_args = read_args


def plan_read_op(op: Read) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read."""

    def get_input_data() -> List[RefBundle]:
        reader = op._datasource.create_reader(**op._read_args)
        read_tasks = reader.get_read_tasks(op._parallelism)
        return [
            RefBundle(
                [
                    (
                        # TODO(chengsu): figure out a better way to pass read
                        # tasks other than ray.put().
                        ray.put(read_task),
                        BlockMetadata(
                            num_rows=1,
                            size_bytes=len(cloudpickle.dumps(read_task)),
                            schema=None,
                            input_files=[],
                            exec_stats=None,
                        ),
                    )
                ],
                owns_blocks=True,
            )
            for read_task in read_tasks
        ]

    inputs = InputDataBuffer(input_data_factory=get_input_data)

    def do_read(blocks: Iterator[ReadTask]) -> Iterator[Block]:
        for read_task in blocks:
            yield from read_task()

    return MapOperator(do_read, inputs, name="DoRead")
