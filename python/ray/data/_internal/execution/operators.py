from typing import List, Iterator, Optional, Any, Dict, Callable

import ray
from ray.data.block import Block
from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExchangeOperator,
    PhysicalOperator,
)
from ray.data._internal.compute import BlockTransform
from ray.data._internal.execution.util import _make_ref_bundles
from ray.data._internal.execution.one_to_one_state import OneToOneOperatorState


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


class OneToOneOperator(PhysicalOperator):
    """A streaming operator that maps inputs 1:1 to outputs.

    Subclasses need only define a single `execute_one` method that runs in a single
    process, leaving the implementation of parallel and distributed execution to the
    Executor implementation.

    Subclasses:
        Read
        Map
        Write
        SortReduce
        WholeStage
    """

    def __init__(self, name: str, input_dependencies: List["PhysicalOperator"]):
        super().__init__(name, input_dependencies)
        self._execution_state = OneToOneOperatorState(self)

    def get_transform_fn(
        self,
    ) -> Callable[[Iterator[Block], Dict[str, Any]], Iterator[Block]]:
        """Return the block transformation to run on a worker process.

        This callable must be serializable as it will be sent to remote processes.

        Returns:
            A callable taking the following inputs:
                block_bundle: Iterator over input blocks of a RefBundle. Typically,
                    this will yield only a single block, unless the transformation has
                    multiple inputs, e.g., in the SortReduce or ZipBlocks cases. It is
                    an iterator instead of a list for memory efficiency.
                input_metadata: Extra metadata provided from the upstream operator.
        """
        raise NotImplementedError

    def compute_strategy(self) -> ComputeStrategy:
        """Return the compute strategy to use for executing these tasks.

        Supported strategies: {TaskPoolStrategy, ActorPoolStrategy}.
        """
        return TaskPoolStrategy()

    def ray_remote_args(self) -> Dict[str, Any]:
        """Return extra ray remote args to use for execution."""
        return {}

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, input_index
        self._execution_state.add_input(refs)

    def inputs_done(self, input_index: int) -> None:
        pass

    def has_next(self) -> bool:
        return len(self._execution_state.outputs) > 0

    def get_next(self) -> RefBundle:
        return self._execution_state.outputs.pop(0)

    def get_tasks(self) -> List[ray.ObjectRef]:
        return list(self._execution_state.tasks)

    def notify_task_completed(self, task: ray.ObjectRef) -> None:
        self._execution_state.task_completed(task)

    def release_unused_resources(self) -> None:
        self._execution_state.release_unused_resources()


class MapOperator(OneToOneOperator):
    """Defines a simple map operation over blocks."""

    def __init__(
        self,
        block_transform: BlockTransform,
        input_op: PhysicalOperator,
        name: str = "Map",
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        self._block_transform = block_transform
        self._strategy = compute_strategy or TaskPoolStrategy()
        self._remote_args = (ray_remote_args or {}).copy()
        super().__init__(name, [input_op])

    def get_transform_fn(self):
        transform = self._block_transform

        def execute_one(block_bundle: Iterator[Block], _) -> Iterator[Block]:
            def apply_transform(fn, block_bundle):
                for b in block_bundle:
                    yield fn(b)

            return apply_transform(transform, block_bundle)

        return execute_one

    def compute_strategy(self):
        return self._strategy

    def ray_remote_args(self):
        return self._remote_args


# For testing only.
def _from_dataset_read_tasks(ds) -> PhysicalOperator:
    read_tasks = ds._plan._snapshot_blocks._tasks
    inputs = InputDataBuffer(_make_ref_bundles([[r] for r in read_tasks]))

    def do_read(block):
        for read_task in block:
            for output_block in read_task():
                return output_block  # TODO handle remaining blocks

    return MapOperator(do_read, inputs, name="DoRead")
