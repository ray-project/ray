from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats

from typing import Callable, Tuple
import uuid


class ExecutionPlan:
    def __init__(self, in_blocks: BlockList, stats: DatasetStats):
        self._in_blocks = in_blocks
        self._out_blocks = None
        self._in_stats = stats
        self._out_stats = None
        self._ops = []

    def with_op(self, op: "Op"):
        copy = ExecutionPlan(self._in_blocks, self._in_stats)
        copy._ops = self._ops.copy()
        copy._ops.append(op)
        return copy

    def execute(self) -> BlockList:
        # TODO: add optimizations:
        # 1. task fusion
        # 2. "move" block references into op()
        if self._out_blocks is None:
            blocks = self._in_blocks
            stats = self._in_stats
            for op in self._ops:
                stats_builder = stats.child_builder(op.name)
                blocks, stage_info = op(blocks)
                if stage_info:
                    stats = stats_builder.build_multistage(stage_info)
                else:
                    stats = stats_builder.build(blocks)
                stats.dataset_uuid = uuid.uuid4().hex
            self._out_blocks = blocks
            self._out_stats = stats
        return self._out_blocks

    def stats(self) -> DatasetStats:
        self.execute()
        return self._out_stats


class Op:
    pass


class OneToOneOp(Op):
    def __init__(
        self,
        name: str,
        block_fn: Callable[[Block], Block],
        compute: str,
        ray_remote_args: dict,
    ):
        self.name = name
        self.block_fn = block_fn
        self.compute = compute
        self.ray_remote_args = ray_remote_args

    def __call__(self, blocks: BlockList) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        blocks = compute.apply(self.block_fn, self.ray_remote_args, blocks)
        return blocks, {}


class AllToAllOp(Op):
    def __init__(self, name: str, fn: Callable[[BlockList], Tuple[BlockList, dict]]):
        self.name = name
        self.fn = fn

    def __call__(self, blocks: BlockList) -> Tuple[BlockList, dict]:
        return self.fn(blocks)
