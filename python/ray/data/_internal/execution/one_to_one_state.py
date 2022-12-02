from typing import Any, Dict, Callable

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.execution.interfaces import (
    RefBundle,
)
from ray.types import ObjectRef


@ray.remote(num_returns=2)
def _transform_one(fn: Callable, block: Block) -> (Block, BlockMetadata):
    [out] = list(fn([block], {}))
    return out, BlockAccessor.for_block(out).get_metadata([], None)


# TODO: handle block splitting?
class _Task:
    def __init__(self, block_ref: ray.ObjectRef):
        self.block_ref = ray.ObjectRef


class OneToOneOperatorState:
    def __init__(self, op: OneToOneOperator):
        self._transform = op.get_transform_fn()
        self._compute_strategy = op.compute_strategy()
        self._ray_remote_args = op.ray_remote_args()
        self.outputs = []
        self.tasks = {}

    def add_input(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        for in_b in input_blocks:
            out_b, out_m = _transform_one.remote(self._transform_fn, in_b)
            self.tasks[out_m] = _Task(out_b)

    def task_completed(self, ref: ray.ObjectRef) -> None:
        task = self.tasks.pop(ref)
        block_meta = ray.get(ref)
        self.outputs.append(RefBundle([(task.block_ref, block_meta)]))

    def release_unused_resources(self) -> None:
        pass
