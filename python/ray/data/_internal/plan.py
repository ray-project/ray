import copy
from typing import (
    Callable,
    List,
    Tuple,
    Optional,
    Union,
    Iterator,
    Iterable,
    TYPE_CHECKING,
)
import uuid

if TYPE_CHECKING:
    import pyarrow

import ray
from ray.data.context import DatasetContext
from ray.data.block import Block
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import get_compute
from ray.data._internal.stats import DatasetStats
from ray.data._internal.lazy_block_list import LazyBlockList

# Scheduling strategy can be inherited from prev stage if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


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

    def __repr__(self):
        return f'{type(self).__name__}("{self.name}")'

    def __str__(self):
        return repr(self)


class ExecutionPlan:
    """A lazy execution plan for a Dataset."""

    # Implementation Notes:
    #
    # This lazy execution plan takes in an input block list and builds up a chain of
    # BlockList --> BlockList stages. When execution is triggered, it tries to fuse
    # together stages in order to reduce Ray task overhead and data copies.
    #
    # Internally, the execution plan holds two block lists:
    #   * _in_blocks: The (possibly lazy) input block list.
    #   * _snapshot_blocks: A snapshot of a computed block list, where this snapshot
    #     is the cached output of executing some prefix in the stage chain.
    #
    # The stages in this execution plan are partitioned into two subchains: before the
    # snapshot and after the snapshot. When the snapshot exists from a previous
    # execution, any future executions will only have to execute the "after the
    # snapshot" subchain, using the snapshot as the input to that subchain.

    def __init__(self, in_blocks: BlockList, stats: DatasetStats, dataset_uuid=None):
        """Create a plan with no transformation stages.

        Args:
            in_blocks: Base list of blocks.
            stats: Stats for the base blocks.
            dataset_uuid: Dataset's UUID.
        """
        self._in_blocks = in_blocks
        self._in_stats = stats
        # A computed snapshot of some prefix of stages.
        self._snapshot_blocks = None
        self._snapshot_stats = None
        # Chains of stages.
        self._stages_before_snapshot = []
        self._stages_after_snapshot = []
        # Cache of optimized stages.
        self._last_optimized_stages = None

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
        copy._stages_after_snapshot.append(stage)
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
        plan_copy._stages_before_snapshot = self._stages_before_snapshot.copy()
        plan_copy._stages_after_snapshot = self._stages_after_snapshot.copy()
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
        in_blocks = self._in_blocks
        if isinstance(in_blocks, BlockList):
            in_blocks = in_blocks.copy()
        plan_copy = ExecutionPlan(
            in_blocks, copy.copy(self._in_stats), dataset_uuid=dataset_uuid
        )
        if self._snapshot_blocks:
            # Copy over the existing snapshot.
            plan_copy._snapshot_blocks = self._snapshot_blocks.copy()
            plan_copy._snapshot_stats = copy.copy(self._snapshot_stats)
        plan_copy._stages_before_snapshot = self._stages_before_snapshot.copy()
        plan_copy._stages_after_snapshot = self._stages_after_snapshot.copy()
        return plan_copy

    def initial_num_blocks(self) -> int:
        """Get the estimated number of blocks after applying all plan stages."""
        if self.has_computed_output():
            return self._snapshot_blocks.initial_num_blocks()
        for stage in self._stages_after_snapshot[::-1]:
            if stage.num_blocks is not None:
                return stage.num_blocks
        if self._snapshot_blocks is not None:
            return self._snapshot_blocks.initial_num_blocks()
        for stage in self._stages_before_snapshot[::-1]:
            if stage.num_blocks is not None:
                return stage.num_blocks
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
        if self._stages_after_snapshot:
            if fetch_if_missing:
                self.execute()
            else:
                return None
        # Snapshot is now guaranteed to be the output of the final stage or None.
        blocks = self._snapshot_blocks
        if not blocks:
            return None
        # Don't force fetching in case it's a lazy block list, in which case we
        # don't want to trigger full execution for a schema read. If we want to
        # trigger execution to get schema, we'll trigger read tasks progressively
        # until a viable schema is available, below.
        metadata = blocks.get_metadata(fetch_if_missing=False)
        # Some blocks could be empty, in which case we cannot get their schema.
        # TODO(ekl) validate schema is the same across different blocks.
        for m in metadata:
            if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
                return m.schema
        if not fetch_if_missing:
            return None
        # Synchronously fetch the schema.
        # For lazy block lists, this launches read tasks and fetches block metadata
        # until we find valid block schema.
        for _, m in blocks.iter_blocks_with_metadata():
            if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
                return m.schema
        return None

    def meta_count(self) -> Optional[int]:
        """Get the number of rows after applying all plan stages if possible.

        This method will never trigger any computation.

        Returns:
            The number of records of the result Dataset, or None.
        """
        if self._stages_after_snapshot:
            return None
        # Snapshot is now guaranteed to be the output of the final stage or None.
        blocks = self._snapshot_blocks
        metadata = blocks.get_metadata() if blocks else None
        if metadata and all(m.num_rows is not None for m in metadata):
            return sum(m.num_rows for m in metadata)
        else:
            return None

    def execute(
        self,
        allow_clear_input_blocks: bool = True,
        force_read: bool = False,
    ) -> BlockList:
        """Execute this plan.

        Args:
            allow_clear_input_blocks: Whether we should try to clear the input blocks
                for each stage.
            force_read: Whether to force the read stage to fully execute.

        Returns:
            The blocks of the output dataset.
        """
        if not self.has_computed_output():
            blocks, stats, stages = self._optimize()
            for stage_idx, stage in enumerate(stages):
                if allow_clear_input_blocks:
                    clear_input_blocks = self._should_clear_input_blocks(
                        blocks, stage_idx
                    )
                else:
                    clear_input_blocks = False
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
            self._stages_before_snapshot += self._stages_after_snapshot
            self._stages_after_snapshot = []
        if _is_lazy(self._snapshot_blocks) and force_read:
            self._snapshot_blocks = self._snapshot_blocks.compute_to_blocklist()
        return self._snapshot_blocks

    def clear_block_refs(self) -> None:
        """Clear all cached block references of this plan, including input blocks.

        This will render the plan un-executable unless the root is a LazyBlockList."""
        self._in_blocks.clear()
        self._snapshot_blocks = None
        self._snapshot_stats = None
        # We're erasing the snapshot, so put all stages into the "after snapshot"
        # bucket.
        self._stages_after_snapshot = (
            self._stages_before_snapshot + self._stages_after_snapshot
        )
        self._stages_before_snapshot = []

    def stats(self) -> DatasetStats:
        """Return stats for this plan, forcing execution if needed."""
        self.execute()
        return self._snapshot_stats

    def _should_clear_input_blocks(
        self,
        blocks: BlockList,
        stage_idx: int,
    ):
        """Whether the provided blocks should be cleared when passed into the stage.

        Args:
            blocks: The blocks that we may want to clear.
            stage_idx: The position of the stage in the optimized after-snapshot chain.
        """
        if stage_idx != 0 or self._stages_before_snapshot:
            # Not the first stage, always clear stage input blocks.
            return True
        elif isinstance(blocks, LazyBlockList):
            # Always clear lazy input blocks since they can be recomputed.
            return True
        else:
            # Otherwise, we have non-lazy input blocks that's the source of this
            # execution plan, so we don't clear these.
            return False

    def _optimize(self) -> Tuple[BlockList, DatasetStats, List[Stage]]:
        """Apply stage fusion optimizations, returning an updated source block list and
        associated stats, and a set of optimized stages.
        """
        context = DatasetContext.get_current()
        blocks, stats, stages = self._get_source_blocks_and_stages()
        if context.optimize_fuse_stages:
            if context.optimize_fuse_read_stages:
                # If using a lazy datasource, rewrite read stage into one-to-one stage
                # so it can be fused into downstream stages.
                blocks, stats, stages = _rewrite_read_stages(
                    blocks, stats, stages, self._dataset_uuid
                )
            stages = _fuse_one_to_one_stages(stages)
            self._last_optimized_stages = stages
        return blocks, stats, stages

    def _get_source_blocks_and_stages(
        self,
    ) -> Tuple[BlockList, DatasetStats, List[Stage]]:
        """Get the source blocks, corresponding stats, and the stages for plan
        execution.

        If a computed snapshot exists and has not been cleared, return the snapshot
        blocks and stats; otherwise, return the input blocks and stats that the plan was
        created with.
        """
        stages = self._stages_after_snapshot.copy()
        if self._snapshot_blocks is not None:
            if not self._snapshot_blocks.is_cleared():
                # If snapshot exists, we only have to execute the plan from the
                # snapshot.
                blocks = self._snapshot_blocks
                stats = self._snapshot_stats
                # Unlink the snapshot blocks from the plan so we can eagerly reclaim the
                # snapshot block memory after the first stage is done executing.
                self._snapshot_blocks = None
            else:
                # Snapshot exists but has been cleared, so we need to recompute from the
                # source (input blocks).
                blocks = self._in_blocks
                stats = self._in_stats
                stages = self._stages_before_snapshot + self._stages_after_snapshot
        else:
            # If no snapshot exists, we have to execute the full plan from the
            # beginning.
            blocks = self._in_blocks
            stats = self._in_stats
            if not self.has_lazy_input():
                # If not a lazy datasource, unlink the input blocks from the plan so we
                # can eagerly reclaim the input block memory after the first stage is
                # done executing.
                self._in_blocks = None
        return blocks, stats, stages

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        return _is_lazy(self._in_blocks)

    def is_read_stage(self) -> bool:
        """Return whether this plan only consists of a read stage."""
        return (
            self.has_lazy_input()
            and not self._stages_before_snapshot
            and not self._stages_after_snapshot
            and (
                not self._snapshot_blocks
                or isinstance(self._snapshot_blocks, LazyBlockList)
            )
        )

    def has_computed_output(self) -> bool:
        """Whether this plan has a computed snapshot for the final stage, i.e. for the
        output of this plan.
        """
        return (
            self._snapshot_blocks is not None
            and not self._stages_after_snapshot
            and not self._snapshot_blocks.is_cleared()
        )


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
        if not _are_remote_args_compatible(prev.ray_remote_args, self.ray_remote_args):
            return False
        return True

    def fuse(self, prev: Stage):
        if not self.can_fuse(prev):
            raise ValueError(
                f"Tried to fuse {prev} with {self}, but these are not fusable."
            )
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
            self.block_fn, self.ray_remote_args, blocks, clear_input_blocks, self.name
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
        if not self.can_fuse(prev):
            raise ValueError(
                f"Tried to fuse {prev} with {self}, but these are not fusable."
            )
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


def _rewrite_read_stages(
    blocks: BlockList,
    stats: DatasetStats,
    stages: List[Stage],
    dataset_uuid: str,
) -> Tuple[BlockList, DatasetStats, List[Stage]]:
    """Rewrites read stages into one-to-one stages, if needed."""
    if _is_lazy(blocks) and stages:
        blocks, stats, stage = _rewrite_read_stage(blocks)
        stats.dataset_uuid = dataset_uuid
        stages.insert(0, stage)
    return blocks, stats, stages


def _rewrite_read_stage(
    in_blocks: LazyBlockList,
) -> Tuple[BlockList, DatasetStats, Stage]:
    """Rewrite the read stage to a OneToOne stage over read tasks as input.

    For example, suppose the plan was [Read -> MapBatches(Fn)]. These stages cannot
    be fused, since read stages are handled specially.
    After rewriting to [GetReadTasks -> MapBatches(DoRead) -> MapBatches(Fn)],
    now we can fuse the latter two MapBatches stages into a single OneToOne stage:
    [GetReadTasks -> MapBatches(DoRead -> Fn)].

    Args:
        blocks: Lazy block list representing read stage.

    Returns:
        Non-lazy block list containing read tasks for not-yet-read block partitions,
        new stats for the block list, and the new one-to-one read stage.
    """
    # Generate the "GetReadTasks" stage blocks.
    remote_args = in_blocks._remote_args
    blocks, metadata = [], []
    for read_task in in_blocks._tasks:
        blocks.append(ray.put(read_task._read_fn))
        metadata.append(read_task.get_metadata())
    block_list = BlockList(blocks, metadata)

    def block_fn(read_fn: Callable[[], Iterator[Block]]) -> Iterator[Block]:
        for block in read_fn():
            yield block

    stage = OneToOneStage("read", block_fn, "tasks", remote_args)
    stats = DatasetStats(stages={}, parent=None)
    return block_list, stats, stage


def _fuse_one_to_one_stages(stages: List[Stage]) -> List[Stage]:
    """Fuses compatible one-to-one stages.

    Args:
        stages: Stages to try to fuse.

    Returns:
        Fused stages.
    """
    fused_stages = []
    prev_stage = None
    for idx, stage in enumerate(stages):
        if prev_stage is None:
            prev_stage = stage
        elif stage.can_fuse(prev_stage):
            prev_stage = stage.fuse(prev_stage)
        else:
            fused_stages.append(prev_stage)
            prev_stage = stage
    if prev_stage:
        fused_stages.append(prev_stage)
        prev_stage = None
    return fused_stages


def _are_remote_args_compatible(prev_args, next_args):
    """Check if Ray remote arguments are compatible for merging."""
    remote_args = next_args.copy()
    for key in INHERITABLE_REMOTE_ARGS:
        if key in prev_args:
            remote_args[key] = prev_args[key]
    if prev_args != remote_args:
        return False
    return True


def _is_lazy(blocks: BlockList) -> bool:
    """Whether the provided block list is lazy."""
    return isinstance(blocks, LazyBlockList)
