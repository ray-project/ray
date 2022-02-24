import copy
from typing import Callable, Tuple, Optional, Union, Iterable, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    import pyarrow

from ray.data.context import DatasetContext
from ray.data.block import Block
from ray.data.datasource import ReadTask
from ray.data.impl.block_list import BlockList
from ray.data.impl.compute import get_compute
from ray.data.impl.stats import DatasetStats
from ray.data.impl.lazy_block_list import LazyBlockList

# Scheduling strategy can be inherited from prev stage if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


class ExecutionPlan:
    """A lazy execution plan for a Dataset."""

    def __init__(self, in_blocks: BlockList, stats: DatasetStats, dataset_uuid=None):
        """Create a plan with no transformation stages.

        Args:
            in_blocks: Base list of blocks.
            stats: Stats for the base blocks.
        """
        self._in_blocks = in_blocks
        self._in_stats = stats
        # A computed snapshot of some prefix of stages.
        self._snapshot_blocks = None
        self._snapshot_stats = None
        self._snapshot_stage_idx = None
        self._stages = []
        self._dataset_uuid = dataset_uuid or uuid.uuid4().hex
        if not stats.dataset_uuid:
            stats.dataset_uuid = self._dataset_uuid

    def with_stage(self, stage: "Stage") -> "ExecutionPlan":
        """Return a copy of this plan with the given stage appended.

        Args:
            stage: The stage to append.

        Returns:
            A new ExecutionPlan with this stage appended.
        """
        copy = self.copy()
        copy._stages.append(stage)
        return copy

    def copy(self) -> "ExecutionPlan":
        """Create a shallow copy of this execution plan.

        This copy can be executed without mutating the original, but clearing the copy
        will also clear the original.

        Returns:
            A shallow copy of this execution plan.
        """
        plan_copy = ExecutionPlan(self._in_blocks, self._in_stats)
        if self._snapshot_blocks is not None:
            # Copy over the existing snapshot.
            plan_copy._snapshot_blocks = self._snapshot_blocks
            plan_copy._snapshot_stats = self._snapshot_stats
            plan_copy._snapshot_stage_idx = self._snapshot_stage_idx
        plan_copy._stages = self._stages.copy()
        return plan_copy

    def deep_copy(self, preserve_uuid: bool = False) -> "ExecutionPlan":
        """Create a deep copy of this execution plan.

        This copy can be executed AND cleared without mutating the original.

        Args:
            preserve_uuid: Whether to preserve the original UUID in the copy.

        Returns:
            A deep copy of this execution plan.
        """
        dataset_uuid = None
        if preserve_uuid:
            dataset_uuid = self._dataset_uuid
        plan_copy = ExecutionPlan(
            self._in_blocks.copy(), copy.copy(self._in_stats), dataset_uuid=dataset_uuid
        )
        if self._snapshot_blocks:
            # Copy over the existing snapshot.
            snapshot_blocks = self._snapshot_blocks
            if isinstance(snapshot_blocks, BlockList):
                snapshot_blocks = snapshot_blocks.copy()
            plan_copy._snapshot_blocks = snapshot_blocks
            plan_copy._snapshot_stats = copy.copy(self._snapshot_stats)
            plan_copy._snapshot_stage_idx = self._snapshot_stage_idx
        plan_copy._stages = self._stages.copy()
        return plan_copy

    def initial_num_blocks(self) -> int:
        """Get the estimated number of blocks after applying all plan stages."""
        if self._has_final_stage_snapshot():
            return self._snapshot_blocks.initial_num_blocks()
        for stage in self._stages[::-1]:
            if stage.num_blocks is not None:
                return stage.num_blocks
        if self._snapshot_blocks is not None:
            return self._snapshot_blocks.initial_num_blocks()
        if self._in_blocks is not None:
            return self._in_blocks.initial_num_blocks()
        return None

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Get the schema after applying all plan stages.

        Args:
            fetch_if_missing: Whether to execute the plan to fetch the schema.

        Returns:
            The schema of the output dataset.
        """
        if self._stages:
            if fetch_if_missing:
                self.execute()
        if self._snapshot_blocks is not None:
            # Snapshot is guaranteed to be the output of the final stage after executing
            # the plan.
            blocks = self._snapshot_blocks
        else:
            blocks = self._in_blocks
        metadata = blocks.get_metadata() if blocks else []
        # Some blocks could be empty, in which case we cannot get their schema.
        # TODO(ekl) validate schema is the same across different blocks.
        for m in metadata:
            if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
                return m.schema
        if not fetch_if_missing:
            return None
        # Need to synchronously fetch schema.
        return blocks.ensure_schema_for_first_block() if blocks else None

    def meta_count(self) -> Optional[int]:
        """Get the number of rows after applying all plan stages if possible.

        This method will never trigger any computation.

        Returns:
            The number of records of the result Dataset, or None.
        """
        if self._stages:
            blocks = self._snapshot_blocks
        else:
            blocks = self._in_blocks
        metadata = blocks.get_metadata() if blocks else None
        if metadata and metadata[0].num_rows is not None:
            return sum(m.num_rows for m in metadata)
        else:
            return None

    def execute(self, clear_input_blocks: bool = True) -> BlockList:
        """Execute this plan.

        Args:
            clear_input_blocks: Whether to assume ownership of the input blocks,
                allowing them to be dropped from memory during execution.

        Returns:
            The blocks of the output dataset.
        """
        if not self._has_final_stage_snapshot():
            self._optimize()
            if self._snapshot_blocks is None:
                # If no snapshot exists, we have to execute the full plan from the
                # beginning.
                blocks = self._in_blocks
                stats = self._in_stats
                if not self.has_lazy_input():
                    # If input blocks are non-lazy, unlink them here so we can eagerly
                    # reclaim the input block memory after the first stage is done
                    # executing.
                    self._in_blocks = None
                stages_to_execute = self._stages
            else:
                # If snapshot exists, we only have to execute the plan from the
                # snapshot.
                blocks = self._snapshot_blocks
                stats = self._snapshot_stats
                # Unlink the snapshot blocks from the plan so we can eagerly reclaim the
                # snapshot block memory after the first stage is done executing.
                self._snapshot_blocks = None
                stages_to_execute = self._stages[self._snapshot_stage_idx + 1 :]
            for stage in stages_to_execute:
                stats_builder = stats.child_builder(stage.name)
                blocks, stage_info = stage(blocks, clear_input_blocks)
                if stage_info:
                    stats = stats_builder.build_multistage(stage_info)
                else:
                    stats = stats_builder.build(blocks)
                stats.dataset_uuid = uuid.uuid4().hex
            # Set the snapshot to the output of the final stage.
            self._snapshot_blocks = blocks
            self._snapshot_stats = stats
            self._snapshot_stats.dataset_uuid = self._dataset_uuid
            self._snapshot_stage_idx = len(self._stages) - 1
        return self._snapshot_blocks

    def clear(self) -> None:
        """Clear all cached block references of this plan, including input blocks.

        This will render the plan un-executable unless the root is a LazyBlockList."""
        if self._should_clear_input():
            self._in_blocks.clear()
        self._snapshot_blocks = None
        self._snapshot_stats = None
        self._snapshot_stage_idx = None

    def _should_clear_input(self) -> bool:
        """Return whether the input blocks should be cleared."""
        return isinstance(self._in_blocks, LazyBlockList) or (
            isinstance(self._in_blocks, BlockList)
            and not isinstance(self._in_blocks._blocks[0], ReadTask)
        )

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        return isinstance(self._in_blocks, LazyBlockList) or (
            isinstance(self._in_blocks, BlockList)
            and isinstance(self._in_blocks._blocks[0], ReadTask)
        )

    def stats(self) -> DatasetStats:
        """Return stats for this plan, forcing execution if needed."""
        self.execute()
        return self._snapshot_stats

    def _optimize(self) -> None:
        """Apply stage fusion optimizations, updating this plan."""
        context = DatasetContext.get_current()
        if context.optimize_fuse_stages:
            if context.optimize_fuse_read_stages:
                self._rewrite_read_stages()
            self._fuse_one_to_one_stages()

    def _has_final_stage_snapshot(self) -> bool:
        """Whether this plan has a computed snapshot for the final stage, i.e. for the
        output of this plan.
        """
        return self._has_snapshot_at_stage(len(self._stages) - 1)

    def _is_stage_before_snapshot(self, stage_idx: int) -> bool:
        """Whether the provided stage is before this plan's snapshot."""
        return (
            self._snapshot_blocks is not None
            and self._snapshot_stage_idx is not None
            and stage_idx < self._snapshot_stage_idx
        )

    def _has_snapshot_at_stage(self, stage_idx: int) -> bool:
        """Whether this plan has a snapshot for the provided stage."""
        return (
            self._snapshot_blocks is not None
            and self._snapshot_stage_idx is not None
            and stage_idx == self._snapshot_stage_idx
        )

    def _rewrite_read_stages(self) -> None:
        """Rewrites read stages into one-to-one stages."""
        if self._stages and self._has_read_stage():
            block_list, stage = self._rewrite_read_stage()
            self._in_blocks = block_list
            self._in_stats = DatasetStats(stages={}, parent=None)
            self._stages.insert(0, stage)
            if self._snapshot_blocks is not None:
                # If the snapshot exists, update it's index to account for the new read
                # stage.
                self._snapshot_stage_idx += 1

    def _has_read_stage(self) -> bool:
        """Whether this plan has a read stage for its input."""
        return isinstance(self._in_blocks, LazyBlockList) and hasattr(
            self._in_blocks, "_read_tasks"
        )

    def _is_read_stage(self) -> bool:
        """Whether this plan is a bare read stage."""
        return self._has_read_stage() and not self._stages

    def _rewrite_read_stage(self) -> Tuple[BlockList, "Stage"]:
        """Rewrite the read stage to a OneToOne stage over read tasks as input.

        For example, suppose the plan was [Read -> MapBatches(Fn)]. These stages cannot
        be fused, since read stages are handled specially.

        After rewriting to [GetReadTasks -> MapBatches(DoRead) -> MapBatches(Fn)],
        now we can fuse the latter two MapBatches stages into a single OneToOne stage:
        [GetReadTasks -> MapBatches(DoRead -> Fn)].
        """
        # Generate the "GetReadTasks" stage blocks.
        remote_args = self._in_blocks._read_remote_args
        read_tasks = []
        metadata = []
        for i, read_task in enumerate(self._in_blocks._read_tasks):
            # TODO(Clark): Remove this shoe-horning of the read tasks into a BlockList.
            read_tasks.append(read_task)
            metadata.append(self._in_blocks._metadata[i])
        block_list = BlockList(read_tasks, metadata)

        def block_fn(read_task: ReadTask) -> Iterable[Block]:
            for tmp1 in read_task._read_fn():
                yield tmp1

        return block_list, OneToOneStage("read", block_fn, "tasks", remote_args)

    def _fuse_one_to_one_stages(self) -> None:
        """Fuses compatible one-to-one stages."""
        optimized_stages = []
        prev_stage = None
        new_snapshot_stage_idx = self._snapshot_stage_idx
        for idx, stage in enumerate(self._stages):
            if prev_stage is None:
                prev_stage = stage
            elif stage.can_fuse(prev_stage) and self._has_snapshot_at_stage(idx - 1):
                # Only fuse the stages if they can be fused and if the previous stage
                # isn't already cached as a snapshot.
                prev_stage = stage.fuse(prev_stage)
                if self._is_stage_before_snapshot(idx):
                    # If this stage fusing is happening before the snapshot, we need to
                    # adjust the snapshot index.
                    new_snapshot_stage_idx -= 1
            else:
                optimized_stages.append(prev_stage)
                prev_stage = stage
        if prev_stage:
            optimized_stages.append(prev_stage)
            prev_stage = None
        self._snapshot_stage_idx = new_snapshot_stage_idx
        self._stages = optimized_stages


class Stage:
    """Represents a Dataset transform stage (e.g., map or shuffle)."""

    def __init__(self, name: str, num_blocks: Optional[int]):
        self.name = name
        self.num_blocks = num_blocks

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        """Execute this stage against the given blocks."""
        raise NotImplementedError

    def can_fuse(self, other: "Stage") -> bool:
        """Return whether this can be fused with another stage."""
        raise NotImplementedError

    def fuse(self, other: "Stage") -> "Stage":
        """Fuse this stage with a compatible stage."""
        raise NotImplementedError


class OneToOneStage(Stage):
    """A stage that transforms blocks independently (e.g., map or filter)."""

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
        self.ray_remote_args = ray_remote_args or {}

    def can_fuse(self, prev: Stage):
        if not isinstance(prev, OneToOneStage):
            return False
        if prev.compute != self.compute:
            return False
        for key in INHERITABLE_REMOTE_ARGS:
            remote_args = self.ray_remote_args.copy()
            if key in prev.ray_remote_args:
                remote_args[key] = prev.ray_remote_args[key]
        if prev.ray_remote_args != remote_args:
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

        return OneToOneStage(name, block_fn, prev.compute, prev.ray_remote_args)

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        blocks = compute._apply(
            self.block_fn, self.ray_remote_args, blocks, clear_input_blocks
        )
        assert isinstance(blocks, BlockList), blocks
        return blocks, {}


class AllToAllStage(Stage):
    """A stage that transforms blocks holistically (e.g., shuffle)."""

    def __init__(
        self,
        name: str,
        num_blocks: Optional[int],
        fn: Callable[[BlockList, bool, Callable], Tuple[BlockList, dict]],
        supports_block_udf: bool = False,
        block_udf=None,
        remote_args=None,
    ):
        super().__init__(name, num_blocks)
        self.fn = fn
        self.supports_block_udf = supports_block_udf
        self.block_udf = block_udf
        self.ray_remote_args = remote_args or {}

    def can_fuse(self, prev: Stage):
        context = DatasetContext.get_current()
        # TODO(ekl) also support fusing shuffle stages to subsequent 1:1 stages.
        if not context.optimize_fuse_shuffle_stages:
            return False
        if not self.supports_block_udf:
            return False
        if not isinstance(prev, OneToOneStage):
            return False
        if prev.compute != "tasks":
            return False
        if any(k not in INHERITABLE_REMOTE_ARGS for k in prev.ray_remote_args):
            return False
        return True

    def fuse(self, prev: Stage):
        assert self.supports_block_udf
        name = prev.name + "->" + self.name
        return AllToAllStage(
            name, self.num_blocks, self.fn, True, prev.block_fn, prev.ray_remote_args
        )

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool
    ) -> Tuple[BlockList, dict]:
        blocks, stage_info = self.fn(
            blocks, clear_input_blocks, self.block_udf, self.ray_remote_args
        )
        assert isinstance(blocks, BlockList), blocks
        return blocks, stage_info
