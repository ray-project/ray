from typing import List, Optional

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.stats import StatsDict
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Reader


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG.

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        reader: Optional[Reader] = None,
        read_parallelism: Optional[int] = None,
    ):
        """Create an InputDataBuffer.

        Args:
            input_data: The list of bundles to output from this operator.
            reader: The reader to read input data, if input_data is None.
            parallelism: The parallelism to read input data, if input_data is None.
        """
        if input_data is not None:
            assert reader is None and read_parallelism is None
            self._input_data = input_data
            self._is_input_initialized = True
            self._initialize_metadata()
        else:
            assert reader is not None and read_parallelism is not None
            self._reader = reader
            self._parallelism = read_parallelism
            self._is_input_initialized = False
        super().__init__("Input", [])

    def has_next(self) -> bool:
        if not self._is_input_initialized:
            self._initialize_input()
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        if not self._is_input_initialized:
            self._initialize_input()
        return self._input_data.pop(0)

    def num_outputs_total(self) -> Optional[int]:
        if not self._is_input_initialized:
            self._initialize_input()
        return self._num_outputs

    def get_stats(self) -> StatsDict:
        if not self._is_input_initialized:
            self._initialize_input()
        return {}

    def add_input(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _initialize_input(self):
        assert not self._is_input_initialized

        # TODO(chengsu): execute `Reader.get_read_tasks()` in a remote task.
        read_tasks = self._reader.get_read_tasks(self._parallelism)
        self._input_data = [
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
        self._is_input_initialized = True
        self._initialize_metadata()

    def _initialize_metadata(self):
        assert self._input_data is not None and self._is_input_initialized

        self._num_outputs = len(self._input_data)
        block_metadata = []
        for bundle in self._input_data:
            block_metadata.extend([m for (_, m) in bundle.blocks])
        self._stats = {
            "input": block_metadata,
        }
