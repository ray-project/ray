from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats
from ray.data.impl.progress_bar import Signal

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

    def execute(
        self,
        clear_input_blocks: bool = True,
        signal: Optional[Signal] = None,
    ) -> BlockList:
        # TODO: add optimizations:
        # 1. task fusion of OneToOne
        # 2. task fusion of OneToOne to AlltoAll
        # 3. clear input blocks
        if self._out_blocks is None:
            blocks = self._in_blocks
            stats = self._in_stats
            for stage in self._stages:
                stats_builder = stats.child_builder(stage.name)
                blocks, stage_info = stage(blocks, clear_input_blocks, signal=signal)
                if stage_info:
                    stats = stats_builder.build_multistage(stage_info)
                else:
                    stats = stats_builder.build(blocks)
                stats.dataset_uuid = uuid.uuid4().hex
            self._out_blocks = blocks
            self._out_stats = stats
            self._out_stats.dataset_uuid = self._dataset_uuid
        return self._out_blocks

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
        self.compute = compute
        self.ray_remote_args = ray_remote_args

    def __call__(
        self,
        blocks: BlockList,
        clear_input_blocks: bool,
        signal: Optional[Signal] = None,
    ) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        blocks = compute.apply(
            self.block_fn,
            self.ray_remote_args,
            blocks,
            clear_input_blocks,
            signal=signal,
        )
        assert isinstance(blocks, BlockList), blocks
        return blocks, {}


class AllToAllStage(Stage):
    def __init__(
        self,
        name: str,
        num_blocks: Optional[int],
        fn: Callable[[BlockList, bool, Callable], Tuple[BlockList, dict]],
        supports_block_udf: bool = False,
        block_udf=None,
    ):
        super().__init__(name, num_blocks)
        self.fn = fn
        self.supports_block_udf = supports_block_udf
        self.block_udf = block_udf

    def __call__(
        self,
        blocks: BlockList,
        clear_input_blocks: bool,
        signal: Optional[Signal] = None,
    ) -> Tuple[BlockList, dict]:
        # TODO(swang): Use the signal to interrupt execution if needed.
        blocks, stage_info = self.fn(blocks, clear_input_blocks, self.block_udf)
        assert isinstance(blocks, BlockList), blocks
        return blocks, stage_info
