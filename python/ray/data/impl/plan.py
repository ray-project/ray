from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats

from typing import Callable


class ExecutionPlan:
    def __init__(self, in_blocks: BlockList):
        self._in_blocks = in_blocks
        self._ops = []
        self._out_blocks = None
        self._stats = None

    def with_op(self, op: "Op"):
        copy = ExecutionPlan(self._in_blocks)
        copy._ops = self._ops.copy()
        copy._ops.append(op)
        return copy

    def execute(self) -> BlockList:
        # TODO: add optimizations:
        # 1. task fusion
        # 2. "move" block references into op()
        if self._out_blocks is None:
            blocks = self._in_blocks
            for op in self._ops:
                blocks = op(blocks)
            self._out_blocks = blocks
        return self._out_blocks

    def stats(self) -> DatasetStats:
        self.execute()
        return self._stats  # TODO fill this in


class Op:
    pass


class OneToOneOp(Op):
    def __init__(
        self, block_fn: Callable[[Block], Block], compute: str, ray_remote_args: dict
    ):
        self.block_fn = block_fn
        self.compute = compute
        self.ray_remote_args = ray_remote_args

    def __call__(self, blocks: BlockList) -> BlockList:
        compute = get_compute(self.compute)
        blocks = compute.apply(self.block_fn, self.ray_remote_args, blocks)
        return blocks


class AllToAllOp(Op):
    def __init__(self, fn: Callable[[BlockList], BlockList]):
        self.fn = fn

    def __call__(self, blocks: BlockList) -> BlockList:
        return self.fn(blocks)
