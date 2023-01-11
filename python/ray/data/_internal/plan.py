import copy
import functools
import itertools
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import ray
from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas
from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import (
    UDF,
    ActorPoolStrategy,
    BlockTransform,
    CallableClass,
    ComputeStrategy,
    get_compute,
    is_task_compute,
)
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.stats import DatasetStats, DatasetStatsSummary
from ray.data.block import Block
from ray.data.context import DatasetContext

if TYPE_CHECKING:
    import pyarrow


# Scheduling strategy can be inherited from prev stage if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


logger = DatasetLogger(__name__)


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

    def __init__(
        self,
        in_blocks: BlockList,
        stats: DatasetStats,
        dataset_uuid=None,
        *,
        run_by_consumer: bool,
    ):
        """Create a plan with no transformation stages.

        Args:
            in_blocks: Base list of blocks.
            stats: Stats for the base blocks.
            dataset_uuid: Dataset's UUID.
            run_by_consumer: Whether this plan is invoked to run by the consumption
            APIs (e.g. .iter_batches()).
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

        self._run_by_consumer = run_by_consumer

    def __repr__(self) -> str:
        return (
            f"ExecutionPlan("
            f"dataset_uuid={self._dataset_uuid}, "
            f"run_by_consumer={self._run_by_consumer}, "
            f"in_blocks={self._in_blocks}, "
            f"stages_before_snapshot={self._stages_before_snapshot}, "
            f"stages_after_snapshot={self._stages_after_snapshot}, "
            f"snapshot_blocks={self._snapshot_blocks})"
        )

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
        plan_copy = ExecutionPlan(
            self._in_blocks, self._in_stats, run_by_consumer=self._run_by_consumer
        )
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
            in_blocks,
            copy.copy(self._in_stats),
            dataset_uuid=dataset_uuid,
            run_by_consumer=self._run_by_consumer,
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
        from ray.data._internal.stage_impl import RandomizeBlocksStage

        if self._stages_after_snapshot:
            if fetch_if_missing:
                if isinstance(self._stages_after_snapshot[-1], RandomizeBlocksStage):
                    # TODO(ekl): this is a hack to optimize the case where we have a
                    # trailing randomize block stages. That stage has no effect and
                    # so we don't need to execute all blocks to get the schema.
                    a = self._stages_after_snapshot.pop()
                    try:
                        self.execute()
                    finally:
                        self._stages_after_snapshot.append(a)
                else:
                    self.execute()
            elif len(self._stages_after_snapshot) == 1 and isinstance(
                self._stages_after_snapshot[-1], RandomizeBlocksStage
            ):
                # If RandomizeBlocksStage is last stage, we execute it (regardless of
                # the fetch_if_missing), since RandomizeBlocksStage is just changing
                # the order of references (hence super cheap).
                self.execute()
            else:
                return None
        elif self._in_blocks is not None and self._snapshot_blocks is None:
            # If the plan only has input blocks, we execute it, so snapshot has output.
            # This applies to newly created dataset. For example, initial dataset from
            # read, and output datasets of Dataset.split().
            self.execute()
        # Snapshot is now guaranteed to be the output of the final stage or None.
        blocks = self._snapshot_blocks
        if not blocks:
            return None

        # Only trigger the execution of first block in case it's a lazy block list.
        # Don't trigger full execution for a schema read.
        if isinstance(blocks, LazyBlockList):
            blocks.compute_first_block()
            blocks.ensure_metadata_for_first_block()

        metadata = blocks.get_metadata(fetch_if_missing=False)
        # Some blocks could be empty, in which case we cannot get their schema.
        # TODO(ekl) validate schema is the same across different blocks.

        # First check if there are blocks with computed schemas, then unify
        # valid schemas from all such blocks.
        schemas_to_unify = []
        for m in metadata:
            if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
                schemas_to_unify.append(m.schema)
        if schemas_to_unify:
            # Check valid pyarrow installation before attempting schema unification
            try:
                import pyarrow as pa
            except ImportError:
                pa = None
            # If the result contains PyArrow schemas, unify them
            if pa is not None and any(
                isinstance(s, pa.Schema) for s in schemas_to_unify
            ):
                return unify_schemas(schemas_to_unify)
            # Otherwise, if the resulting schemas are simple types (e.g. int),
            # return the first schema.
            return schemas_to_unify[0]
        if not fetch_if_missing:
            return None
        # Synchronously fetch the schema.
        # For lazy block lists, this launches read tasks and fetches block metadata
        # until we find the first valid block schema. This is to minimize new
        # computations when fetching the schema.
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
            context = DatasetContext.get_current()

            # Read stage is handled with the legacy execution impl for now.
            if (
                context.new_execution_backend
                and not self.is_read_stage_equivalent()
                and self._stages_after_snapshot
            ):
                from ray.data._internal.execution.bulk_executor import BulkExecutor
                from ray.data._internal.execution.interfaces import ExecutionOptions
                from ray.data._internal.execution.legacy_compat import (
                    execute_to_legacy_block_list,
                )

                executor = BulkExecutor(ExecutionOptions())
                blocks = execute_to_legacy_block_list(
                    executor,
                    self,
                    allow_clear_input_blocks=allow_clear_input_blocks,
                    dataset_uuid=self._dataset_uuid,
                )
                # TODO(ekl) we shouldn't need to set this in the future once we move
                # to a fully lazy execution model, unless .cache() is used. The reason
                # we need it right now is since the user may iterate over a Dataset
                # multiple times after fully executing it once.
                if not self._run_by_consumer:
                    blocks._owned_by_consumer = False
                stats = executor.get_stats()
                stats_summary_string = stats.to_summary().to_string(
                    include_parent=False
                )
                logger.get_logger(log_to_stdout=context.enable_auto_log_stats).info(
                    stats_summary_string,
                )

            else:
                blocks, stats, stages = self._optimize()

                for stage_idx, stage in enumerate(stages):
                    if allow_clear_input_blocks:
                        clear_input_blocks = self._should_clear_input_blocks(
                            blocks, stage_idx
                        )
                    else:
                        clear_input_blocks = False
                    stats_builder = stats.child_builder(stage.name)
                    blocks, stage_info = stage(
                        blocks, clear_input_blocks, self._run_by_consumer
                    )
                    if stage_info:
                        stats = stats_builder.build_multistage(stage_info)
                    else:
                        stats = stats_builder.build(blocks)
                    stats.dataset_uuid = self._dataset_uuid
                    stats_summary_string = stats.to_summary().to_string(
                        include_parent=False,
                    )
                    logger.get_logger(log_to_stdout=context.enable_auto_log_stats).info(
                        stats_summary_string,
                    )

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

    def stats_summary(self) -> DatasetStatsSummary:
        self.execute()
        return self._snapshot_stats.to_summary()

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
        if context.optimize_reorder_stages:
            stages = _reorder_stages(stages)
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
        return blocks, stats, stages

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        return _is_lazy(self._in_blocks)

    def is_read_stage_equivalent(self) -> bool:
        """Return whether this plan can be executed as only a read stage."""
        from ray.data._internal.stage_impl import RandomizeBlocksStage

        context = DatasetContext.get_current()
        remaining_stages = self._stages_after_snapshot
        if (
            context.optimize_fuse_stages
            and remaining_stages
            and isinstance(remaining_stages[0], RandomizeBlocksStage)
        ):
            remaining_stages = remaining_stages[1:]
        return (
            self.has_lazy_input()
            and not self._stages_before_snapshot
            and not remaining_stages
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


def _pack_args(
    self_fn_args: Iterable[Any],
    self_fn_kwargs: Dict[str, Any],
    prev_fn_args: Iterable[Any],
    prev_fn_kwargs: Dict[str, Any],
) -> Tuple[
    Tuple[Any],
    Callable[
        [Tuple[Any]],
        Tuple[
            Tuple[Any],
            Dict[str, Any],
            Tuple[Any],
            Dict[str, Any],
        ],
    ],
]:
    """Pack the (kw)args from two stages into a single, flat positional args tuple that
    can be given to a Ray task, ensuring resoultion of each argument.
    This function returns this args tuple along with a function that will unpack this
    flat args tuple back into it's original args and kwargs structure.
    """
    if not self_fn_args:
        self_fn_args = tuple()
    if not self_fn_kwargs:
        self_fn_kwargs = {}
    if not prev_fn_args:
        prev_fn_args = tuple()
    if not prev_fn_kwargs:
        prev_fn_kwargs = {}
    # Offsets into flat args tuple.
    offsets = list(
        itertools.accumulate(
            [
                len(self_fn_args),
                len(prev_fn_args),
                len(self_fn_kwargs),
                len(prev_fn_kwargs),
            ]
        )
    )
    # Keys for the kwargs.
    keys = list(self_fn_kwargs.keys()) + list(prev_fn_kwargs.keys())

    fn_args = (
        self_fn_args
        + prev_fn_args
        + tuple(self_fn_kwargs.values())
        + tuple(prev_fn_kwargs.values())
    )

    def unpack(
        fn_args: List[Any],
    ) -> Tuple[List[Any], Dict[str, Any], List[Any], Dict[str, Any]]:
        self_fn_args = fn_args[: offsets[0]]
        prev_fn_args = fn_args[offsets[0] : offsets[1]]
        self_fn_kwargs = fn_args[offsets[1] : offsets[2]]
        prev_fn_kwargs = fn_args[offsets[2] :]
        prev_key_offset = offsets[2] - offsets[1]
        self_fn_kwargs = {k: v for k, v in zip(keys[:prev_key_offset], self_fn_kwargs)}
        prev_fn_kwargs = {k: v for k, v in zip(keys[prev_key_offset:], prev_fn_kwargs)}
        return self_fn_args, self_fn_kwargs, prev_fn_args, prev_fn_kwargs

    return fn_args, unpack


class OneToOneStage(Stage):
    """A stage that transforms blocks independently (e.g., map or filter)."""

    def __init__(
        self,
        name: str,
        block_fn: BlockTransform,
        compute: Union[str, ComputeStrategy],
        ray_remote_args: dict,
        target_block_size: Optional[int] = None,
        fn: Optional[UDF] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name, None)
        self.block_fn = block_fn
        self.compute = compute or "tasks"
        self.ray_remote_args = ray_remote_args or {}
        self.target_block_size = target_block_size
        self.fn = fn
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self.fn_constructor_args = fn_constructor_args
        self.fn_constructor_kwargs = fn_constructor_kwargs

    def can_fuse(self, prev: Stage):
        if not isinstance(prev, OneToOneStage):
            return False
        # Allow fusing tasks->actors if the resources are compatible (read->map), but
        # not the other way around. The latter will be used as the compute if fused.
        if is_task_compute(self.compute) and prev.compute != self.compute:
            return False
        if (
            isinstance(self.fn, CallableClass)
            and isinstance(prev.fn, CallableClass)
            and (
                prev.fn != self.fn
                or (
                    prev.fn_constructor_args != self.fn_constructor_args
                    or prev.fn_constructor_kwargs != self.fn_constructor_kwargs
                )
            )
        ):
            # Fusing callable classes is only supported if they are the same function
            # AND their construction arguments are the same.
            # TODO(Clark): Support multiple callable classes instantiating in the same
            # actor worker constructor.
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
        prev_fn = prev.fn
        if isinstance(self.fn, CallableClass) and isinstance(prev_fn, CallableClass):
            assert self.fn == prev_fn
            assert (
                prev.fn_constructor_args == self.fn_constructor_args
                and prev.fn_constructor_kwargs == self.fn_constructor_kwargs
            )
            # If both UDFs are callable classes, they must be equal and have the same
            # construction args, so we tell the previous stage to reuse the passed
            # (instantiated) callable class UDF that's provided to the block function.
            use_outer_fn = True
            prev_fn = None
        else:
            # Otherwise, we're either fusing two non-callable class UDFs, or a
            # non-callable class UDF with a callable class UDF. In either case, prev
            # will be a non-callable class UDF, so we use it within the block function.
            use_outer_fn = False

        # Package args into a flat positional args list.
        fn_args, unpack_args = _pack_args(
            self.fn_args,
            self.fn_kwargs,
            prev.fn_args,
            prev.fn_kwargs,
        )

        block_fn1 = prev.block_fn
        block_fn2 = self.block_fn
        if prev.target_block_size is not None and self.target_block_size is not None:
            target_block_size = max(prev.target_block_size, self.target_block_size)
        elif prev.target_block_size is not None:
            target_block_size = prev.target_block_size
        else:
            target_block_size = self.target_block_size

        def block_fn(
            blocks: Iterable[Block],
            fn: UDF,
            *fn_args,
            **fn_kwargs,
        ) -> Iterable[Block]:
            assert not fn_kwargs, fn_kwargs
            # Unpack flat position args list into
            self_fn_args, self_fn_kwargs, prev_fn_args, prev_fn_kwargs = unpack_args(
                fn_args
            )
            self_fn_args = self_fn_args if fn is None else (fn,) + self_fn_args
            if use_outer_fn:
                prev_fn_ = fn
            else:
                prev_fn_ = prev_fn
            prev_fn_args = (
                prev_fn_args if prev_fn_ is None else (prev_fn_,) + prev_fn_args
            )
            blocks = block_fn1(blocks, *prev_fn_args, **prev_fn_kwargs)
            return block_fn2(blocks, *self_fn_args, **self_fn_kwargs)

        return OneToOneStage(
            name,
            block_fn,
            self.compute,
            prev.ray_remote_args,
            target_block_size=target_block_size,
            fn=self.fn,
            fn_args=fn_args,
            fn_kwargs={},
            fn_constructor_args=self.fn_constructor_args,
            fn_constructor_kwargs=self.fn_constructor_kwargs,
        )

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool, run_by_consumer: bool
    ) -> Tuple[BlockList, dict]:
        compute = get_compute(self.compute)
        assert (
            self.fn_constructor_args is None and self.fn_constructor_kwargs is None
        ) or isinstance(compute, ActorPoolStrategy)

        if blocks._owned_by_consumer:
            assert (
                run_by_consumer
            ), "Blocks owned by consumer can only be consumed by consumer"

        blocks = compute._apply(
            self.block_fn,
            self.ray_remote_args,
            blocks,
            clear_input_blocks,
            name=self.name,
            target_block_size=self.target_block_size,
            fn=self.fn,
            fn_args=self.fn_args,
            fn_kwargs=self.fn_kwargs,
            fn_constructor_args=self.fn_constructor_args,
            fn_constructor_kwargs=self.fn_constructor_kwargs,
        )
        assert isinstance(blocks, BlockList), blocks
        blocks._owned_by_consumer = run_by_consumer
        return blocks, {}


class AllToAllStage(Stage):
    """A stage that transforms blocks holistically (e.g., shuffle)."""

    def __init__(
        self,
        name: str,
        num_blocks: Optional[int],
        fn: Callable[[BlockList, bool, Callable], Tuple[BlockList, dict]],
        supports_block_udf: bool = False,
        block_udf: Optional[BlockTransform] = None,
        remote_args: Optional[Dict[str, Any]] = None,
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
        if not is_task_compute(prev.compute):
            return False
        if not _are_remote_args_compatible(prev.ray_remote_args, self.ray_remote_args):
            return False
        return True

    def fuse(self, prev: Stage):
        if not self.can_fuse(prev):
            raise ValueError(
                f"Tried to fuse {prev} with {self}, but these are not fusable."
            )
        assert self.supports_block_udf
        assert prev.fn_constructor_args is None and prev.fn_constructor_kwargs is None
        name = prev.name + "->" + self.name
        prev_fn_args = prev.fn_args or tuple()
        prev_fn_args = prev_fn_args if prev.fn is None else (prev.fn,) + prev_fn_args
        prev_fn_kwargs = prev.fn_kwargs or {}
        prev_block_fn = prev.block_fn
        if self.block_udf is None:

            def block_udf(blocks: Iterable[Block]) -> Iterable[Block]:
                yield from prev_block_fn(blocks, *prev_fn_args, **prev_fn_kwargs)

        else:
            self_block_udf = self.block_udf

            def block_udf(blocks: Iterable[Block]) -> Iterable[Block]:
                blocks = prev_block_fn(
                    blocks,
                    *prev_fn_args,
                    **prev_fn_kwargs,
                )
                yield from self_block_udf(blocks)

        return AllToAllStage(
            name, self.num_blocks, self.fn, True, block_udf, prev.ray_remote_args
        )

    def __call__(
        self, blocks: BlockList, clear_input_blocks: bool, run_by_consumer: bool
    ) -> Tuple[BlockList, dict]:
        from ray.data._internal.stage_impl import RandomizeBlocksStage

        in_blocks_owned_by_consumer = blocks._owned_by_consumer
        if in_blocks_owned_by_consumer:
            assert (
                run_by_consumer
            ), "Blocks owned by consumer can only be consumed by consumer"
        blocks, stage_info = self.fn(
            blocks, clear_input_blocks, self.block_udf, self.ray_remote_args
        )
        assert isinstance(blocks, BlockList), blocks

        # RandomizeBlocksStage is an in-place transformation, so the ownership
        # of blocks doesn't change.
        if isinstance(self, RandomizeBlocksStage):
            blocks._owned_by_consumer = in_blocks_owned_by_consumer
        else:
            blocks._owned_by_consumer = run_by_consumer

        return blocks, stage_info


def _rewrite_read_stages(
    blocks: BlockList,
    stats: DatasetStats,
    stages: List[Stage],
    dataset_uuid: str,
) -> Tuple[BlockList, DatasetStats, List[Stage]]:
    """Rewrites read stages into one-to-one stages, if needed."""
    if _is_lazy(blocks) and stages:
        blocks, stats, stages = _rewrite_read_stage(blocks, stages)
        stats.dataset_uuid = dataset_uuid
    return blocks, stats, stages


def _rewrite_read_stage(
    in_blocks: LazyBlockList, stages: List[Stage]
) -> Tuple[BlockList, DatasetStats, List[Stage]]:
    """Rewrite the read stage to a OneToOne stage over read tasks as input.

    For example, suppose the plan was [Read -> MapBatches(Fn)]. These stages cannot
    be fused, since read stages are handled specially.
    After rewriting to [GetReadTasks -> MapBatches(DoRead) -> MapBatches(Fn)],
    now we can fuse the latter two MapBatches stages into a single OneToOne stage:
    [GetReadTasks -> MapBatches(DoRead -> Fn)].

    Args:
        blocks: Lazy block list representing read stage.
        stages: List of current stages.

    Returns:
        Non-lazy block list containing read tasks for not-yet-read block partitions,
        new stats for the block list, and the new list of stages.
    """
    from ray.data._internal.stage_impl import RandomizeBlocksStage

    # Generate the "GetReadTasks" stage blocks.
    remote_args = in_blocks._remote_args
    blocks, metadata = [], []
    for read_task in in_blocks._tasks:
        blocks.append(ray.put(read_task._read_fn))
        metadata.append(read_task.get_metadata())
    block_list = BlockList(
        blocks, metadata, owned_by_consumer=in_blocks._owned_by_consumer
    )

    @_adapt_for_multiple_blocks
    def block_fn(read_fn: Callable[[], Iterator[Block]]) -> Iterator[Block]:
        for block in read_fn():
            yield block

    name = "read"

    # Fuse downstream randomize stage with the read stage if possible. This is needed
    # when .window() is called right after read->randomize, since it forces execution.
    has_randomize = stages and isinstance(stages[0], RandomizeBlocksStage)
    if has_randomize:
        if stages and isinstance(stages[0], RandomizeBlocksStage):
            block_list, _ = stages[0].do_randomize(block_list)
            stages = stages[1:]
        name += "->randomize_block_order"

    stage = OneToOneStage(
        name,
        block_fn,
        "tasks",
        remote_args,
    )
    stats = DatasetStats(stages={}, parent=None)
    stages.insert(0, stage)
    return block_list, stats, stages


def _reorder_stages(stages: List[Stage]) -> List[Stage]:
    """Reorder randomize stages to the end to enable better stage fusion.

    This applies to RandomizeBlockOrder stages specifically (issue #26057).

    Args:
        stages: Stages to try to reorder.

    Returns:
        Reordered stages.
    """
    from ray.data._internal.stage_impl import RandomizeBlocksStage

    output: List[Stage] = []
    reorder_buf: List[RandomizeBlocksStage] = []

    for s in stages:
        if isinstance(s, RandomizeBlocksStage):
            # Buffer it for later reordering.
            reorder_buf.append(s)
        else:
            # Barrier: flush the reorder buffer.
            if isinstance(s, AllToAllStage):
                output.extend(reorder_buf)
                reorder_buf = []
            output.append(s)

    output.extend(reorder_buf)
    return output


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
    prev_args = _canonicalize(prev_args)
    next_args = _canonicalize(next_args)
    remote_args = next_args.copy()
    for key in INHERITABLE_REMOTE_ARGS:
        if key in prev_args:
            remote_args[key] = prev_args[key]
    if prev_args != remote_args:
        return False
    return True


def _canonicalize(remote_args: dict) -> dict:
    """Returns canonical form of given remote args."""
    remote_args = remote_args.copy()
    if "num_cpus" not in remote_args or remote_args["num_cpus"] is None:
        remote_args["num_cpus"] = 1
    if "num_gpus" not in remote_args or remote_args["num_gpus"] is None:
        remote_args["num_gpus"] = 0
    resources = remote_args.get("resources", {})
    for k, v in list(resources.items()):
        if v is None or v == 0.0:
            del resources[k]
    remote_args["resources"] = resources
    return remote_args


def _is_lazy(blocks: BlockList) -> bool:
    """Whether the provided block list is lazy."""
    return isinstance(blocks, LazyBlockList)


def _adapt_for_multiple_blocks(
    fn: Callable[..., Iterable[Block]],
) -> Callable[..., Iterable[Block]]:
    @functools.wraps(fn)
    def wrapper(blocks: Iterable[Block], *args, **kwargs):
        for block in blocks:
            yield from fn(block, *args, **kwargs)

    return wrapper
