from typing import Callable, Optional, List, Dict, Any, TYPE_CHECKING

import ray
from ray.data._internal.execution.interfaces import (
    RefBundle,
)
from ray.data.block import Block, BlockAccessor, BlockExecStats
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_operator import MapOperator


@ray.remote(num_returns="dynamic")
def _map_task(fn: Callable, input_metadata: Dict[str, Any], *blocks: List[Block]):
    """Remote function for a single operator task.

    Args:
        fn: The callable that takes (Iterator[Block], input_metadata) as input and
            returns Iterator[Block] as output.
        input_metadata: The input metadata from the task ref bundle.
        blocks: The concrete block values from the task ref bundle.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    output_metadata = []
    stats = BlockExecStats.builder()
    for b_out in fn(blocks, input_metadata):
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        output_metadata.append(m_out)
        yield b_out
        stats = BlockExecStats.builder()
    yield output_metadata


class _TaskState:
    """Tracks the driver-side state for an MapOperator task.

    Attributes:
        inputs: The input ref bundle.
        output: The output ref bundle that is set when the task completes.
    """

    def __init__(self, inputs: RefBundle):
        self.inputs: RefBundle = inputs
        self.output: Optional[RefBundle] = None


class MapOperatorTasksImpl:
    def __init__(self, op: "MapOperator"):
        self._transform_fn = op.get_transform_fn()
        self._ray_remote_args = op.ray_remote_args()
        self._tasks: Dict[ObjectRef[ObjectRefGenerator], _TaskState] = {}
        self._tasks_by_output_order: Dict[int, _TaskState] = {}
        self._next_task_index = 0
        self._next_output_index = 0

    def add_input(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        generator_ref = _map_task.options(**self._ray_remote_args).remote(
            self._transform_fn, bundle.input_metadata, *input_blocks
        )
        task = _TaskState(bundle)
        self._tasks[generator_ref] = task
        self._tasks_by_output_order[self._next_task_index] = task
        self._next_task_index += 1

    def task_completed(self, ref: ObjectRef[ObjectRefGenerator]) -> None:
        task = self._tasks.pop(ref)
        all_refs = list(ray.get(ref))
        block_refs = all_refs[:-1]
        block_metas = ray.get(all_refs[-1])
        assert len(block_metas) == len(block_refs), (block_refs, block_metas)
        task.output = RefBundle(list(zip(block_refs, block_metas)), owns_blocks=True)
        # TODO(ekl) this isn't strictly correct if multiple operators depend on this
        # bundle, but it doesn't happen in linear dags for now.
        task.inputs.destroy_if_owned()

    def has_next(self) -> bool:
        i = self._next_output_index
        return (
            i in self._tasks_by_output_order
            and self._tasks_by_output_order[i].output is not None
        )

    def get_next(self) -> bool:
        i = self._next_output_index
        self._next_output_index += 1
        return self._tasks_by_output_order.pop(i).output

    def get_tasks(self) -> List[ray.ObjectRef]:
        return list(self._tasks)
