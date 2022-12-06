from typing import Callable, Optional, List, TYPE_CHECKING
from types import GeneratorType

import ray
from ray.data._internal.execution.interfaces import (
    RefBundle,
)
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data._internal.execution.operators import OneToOneOperator


@ray.remote(num_returns=2)
def _transform_one(fn: Callable, block: Block) -> (Block, BlockMetadata):
    print("CALL FN", [block])
    [out] = list(fn([block], {}))
    if isinstance(out, GeneratorType):
        out = list(out)
    print("OUTPUT", out)
    return out, BlockAccessor.for_block(out).get_metadata([], None)


# TODO: handle block splitting?
class _Task:
    def __init__(self, block_ref: ObjectRef):
        self.block_ref = block_ref
        self.output: Optional[RefBundle] = None


class OneToOneOperatorState:
    def __init__(self, op: "OneToOneOperator"):
        self._transform_fn = op.get_transform_fn()
        self._compute_strategy = op.compute_strategy()
        self._ray_remote_args = op.ray_remote_args()
        self._tasks: Dict[ObjectRef, _Task] = {}
        self._tasks_by_output_order: Dict[int, _Task] = {}
        self._next_task_index = 0
        self._next_output_index = 0

    def add_input(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        for in_b in input_blocks:
            out_b, out_m = _transform_one.remote(self._transform_fn, in_b)
            task = _Task(out_b)
            self._tasks[out_m] = task
            self._tasks_by_output_order[self._next_task_index] = task
            self._next_task_index += 1

    def task_completed(self, ref: ObjectRef) -> None:
        task = self._tasks.pop(ref)
        block_meta = ray.get(ref)
        task.output = RefBundle([(task.block_ref, block_meta)])

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

    def release_unused_resources(self) -> None:
        pass
