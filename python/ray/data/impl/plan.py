from typing import Callable, Tuple, Optional, Union, Iterable, TYPE_CHECKING
from collections import OrderedDict
import uuid

if TYPE_CHECKING:
    import pyarrow

import ray
from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats
from ray.data.impl.lazy_block_list import LazyBlockList

OPTIMIZE_FUSE = True
OPTIMIZE_FUSE_READ = False
OPTIMIZE_FUSE_SHUFFLE = True


class ExecutionPlan:
    def __init__(self, in_blocks: BlockList, stats: DatasetStats):
        self._in_blocks = in_blocks
        self._out_blocks = None
        self._in_stats = stats
        self._out_stats = None
        self._stages = []
        self._dataset_uuid = uuid.uuid4().hex
        if not stats.dataset_uuid:
            stats.dataset_uuid = self._dataset_uuid

    def with_stage(self, stage: "Stage"):
        if self._out_blocks:
            copy = ExecutionPlan(self._out_blocks, self._out_stats)
            copy._stages = [stage]
        else:
            copy = ExecutionPlan(self._in_blocks, self._in_stats)
            copy._stages = self._stages.copy()
            copy._stages.append(stage)
        return copy

    def initial_num_blocks(self) -> int:
        if self._out_blocks:
            return self._out_blocks.initial_num_blocks()
        for stage in self._stages[::-1]:
            if stage.num_blocks is not None:
                return stage.num_blocks
        return self._in_blocks.initial_num_blocks()

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        if self._stages:
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
        if self._stages:
            blocks = self._out_blocks
        else:
            blocks = self._in_blocks
        metadata = blocks.get_metadata() if blocks else None
        if metadata and metadata[0].num_rows is not None:
            return sum(m.num_rows for m in metadata)
        else:
            return None

    def execute(self, clear_input_blocks: bool = True) -> BlockList:
        # TODO: add optimizations:
        # 1. task fusion of OneToOne
        # 2. task fusion of OneToOne to AlltoAll
        # 3. clear input blocks
        if self._out_blocks is None:
            self._optimize()
            blocks = self._in_blocks
            stats = self._in_stats
            for stage in self._stages:
                stats_builder = stats.child_builder(stage.name)
                blocks, stage_info = stage(blocks, clear_input_blocks)
                if stage_info:
                    stats = stats_builder.build_multistage(stage_info)
                else:
                    stats = stats_builder.build(blocks)
                stats.dataset_uuid = uuid.uuid4().hex
            self._out_blocks = blocks
            self._out_stats = stats
            self._out_stats.dataset_uuid = self._dataset_uuid
        return self._out_blocks

    def _optimize(self):
        if OPTIMIZE_FUSE:
            if OPTIMIZE_FUSE_READ:
                self._rewrite_read_stages()
            self._fuse_one_to_one_stages()

    def _rewrite_read_stages(self) -> None:
        """Rewrites read stages into one-to-one stages."""
        if self._stages and isinstance(self._in_blocks, LazyBlockList):
            blocks = []
            metadata = []
            for i, read_task in enumerate(self._in_blocks._read_tasks):
                blocks.append(ray.put([read_task]))
                metadata.append(self._in_blocks._metadata[i])
            self._in_blocks = BlockList(blocks, metadata)
            self._in_stats = DatasetStats(stages=OrderedDict(), parent=None)

            def block_fn(block: Block) -> Iterable[Block]:
                [read_task] = block
                for tmp1 in read_task._read_fn():
                    yield tmp1

            # TODO: use num_cpus=1 by default for read and pass in remote args?
            self._stages.insert(0, OneToOneStage("read", block_fn, None, {}))

    def _fuse_one_to_one_stages(self) -> None:
        """Fuses compatible one-to-one stages."""
        optimized_stages = []
        prev_stage = None
        for stage in self._stages:
            if prev_stage is None:
                prev_stage = stage
            elif stage.can_fuse(prev_stage):
                prev_stage = stage.fuse(prev_stage)
            else:
                optimized_stages.append(prev_stage)
                prev_stage = stage
        if prev_stage:
            optimized_stages.append(prev_stage)
            prev_stage = None
        self._stages = optimized_stages

    def clear(self) -> None:
        self._out_blocks = None
        self._out_stats = None

    def stats(self) -> DatasetStats:
        self.execute()
        return self._out_stats


class Stage:
    def __init__(self, name: str, num_blocks: Optional[int]):
        self.name = name
        self.num_blocks = num_blocks

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        raise NotImplementedError

    def can_fuse(self, other: "Stage"):
        raise NotImplementedError

    def fuse(self, other: "Stage"):
        raise NotImplementedError


class OneToOneStage(Stage):
    def __init__(
        self,
        name: str,
        block_fn: Callable[[Block], Block],
        compute: str,
        ray_remote_args: dict,
    ):
        super().__init__(name, None)
        self.block_fn = block_fn
        self.compute = compute or "tasks"
        self.ray_remote_args = ray_remote_args

    def can_fuse(self, prev: Stage):
        if not isinstance(prev, OneToOneStage):
            return False
        if prev.compute != self.compute:
            return False
        if prev.ray_remote_args != self.ray_remote_args:
            return False
        return True

    def fuse(self, prev: Stage):
        name = prev.name + "->" + self.name
        fn1 = prev.block_fn
        fn2 = self.block_fn

        def block_fn(block: Block) -> Iterable[Block]:
            for tmp1 in fn1(block):
                for tmp2 in fn2(tmp1):
                    yield tmp2

        return OneToOneStage(name, block_fn, self.compute, self.ray_remote_args)

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        blocks = compute.apply(
            self.block_fn, self.ray_remote_args, blocks, clear_input_blocks
        )
        assert isinstance(blocks, BlockList), blocks
        return blocks, {}


class AllToAllStage(Stage):
    def __init__(
        self,
        name: str,
        num_blocks: Optional[int],
        fn: Callable[[BlockList, bool], Tuple[BlockList, dict]],
        supports_block_udf: bool = False,
        block_udf=None,
    ):
        super().__init__(name, num_blocks)
        self.fn = fn
        self.supports_block_udf = supports_block_udf
        self.block_udf = block_udf

    def can_fuse(self, prev: Stage):
        if not OPTIMIZE_FUSE_SHUFFLE:
            return False
        if not self.supports_block_udf:
            return False
        if not isinstance(prev, OneToOneStage):
            return False
        if prev.compute != "tasks":
            return False
        if prev.ray_remote_args != {}:
            return False
        return True

    def fuse(self, prev: Stage):
        assert self.supports_block_udf
        name = prev.name + "->" + self.name
        return AllToAllStage(name, self.num_blocks, self.fn, True, prev.block_fn)

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        blocks, stage_info = self.fn(blocks, clear_input_blocks, self.block_udf)
        assert isinstance(blocks, BlockList), blocks
        return blocks, stage_info
