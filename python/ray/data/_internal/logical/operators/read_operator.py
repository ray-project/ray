from typing import Any, Dict, Iterator

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.execution.interfaces import RefBundle, PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import Block, BlockMetadata
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

    def get_physical_dag(self) -> PhysicalOperator:
        """Get the corresponding DAG of physical operators for read."""
        assert not self.input_dependencies, self.input_dependencies

        read_tasks = self._reader.get_read_tasks(self._parallelism)

        inputs = InputDataBuffer(
            [
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
        )

        def do_read(blocks: Iterator[Block]) -> Iterator[Block]:
            for read_task in blocks:
                yield from read_task()

        return MapOperator(do_read, inputs, name="DoRead")
