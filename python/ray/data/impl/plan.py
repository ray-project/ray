from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats

from typing import Callable, Tuple, Optional, Union, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    import pyarrow


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

    def est_num_blocks(self) -> int:
        if self._out_blocks:
            return self._out_blocks.initial_num_blocks()
        for op in self._ops[::-1]:
            if op.num_blocks is not None:
                return op.num_blocks
        return self._in_blocks.initial_num_blocks()

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        if self._ops:
            if fetch_if_missing:
                self.execute()
            blocks = self._out_blocks
        else:
            blocks = self._in_blocks
        metadata = blocks.get_metadata() if blocks else []
        # Some blocks could be empty, in which case we cannot get their schema.
        # TODO(ekl) validate schema is the same across different blocks.
        for m in metadata:
            if m.schema is not None:
                return m.schema
        if not fetch_if_missing:
            return None
        # Need to synchronously fetch schema.
        return blocks.ensure_schema_for_first_block()

    def meta_count(self) -> Optional[int]:
        if self._ops:
            blocks = self._out_blocks
        else:
            blocks = self._in_blocks
        metadata = blocks.get_metadata() if blocks else None
        if metadata and metadata[0].num_rows is not None:
            return sum(m.num_rows for m in metadata)
        else:
            return None

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
    def __init__(self, name: str, num_blocks: Optional[int]):
        self.name = name
        self.num_blocks = num_blocks

    def __call__(self, blocks: BlockList) -> Tuple[BlockList, dict]:
        raise NotImplementedError


class OneToOneOp(Op):
    def __init__(
        self,
        name: str,
        block_fn: Callable[[Block], Block],
        compute: str,
        ray_remote_args: dict,
    ):
        super().__init__(name, None)
        self.block_fn = block_fn
        self.compute = compute
        self.ray_remote_args = ray_remote_args

    def __call__(self, blocks: BlockList) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        blocks = compute.apply(self.block_fn, self.ray_remote_args, blocks)
        return blocks, {}


class AllToAllOp(Op):
    def __init__(
        self,
        name: str,
        num_blocks: Optional[int],
        fn: Callable[[BlockList], Tuple[BlockList, dict]],
    ):
        super().__init__(name, num_blocks)
        self.fn = fn

    def __call__(self, blocks: BlockList) -> Tuple[BlockList, dict]:
        return self.fn(blocks)
