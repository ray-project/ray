import abc
import logging
import pickle
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import ray
from .ref_bundle import BlockEntry, RefBundle
from ray._raylet import ObjectRefGenerator
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolInfo,
    AutoscalingActorPool,
)
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics
from ray.data._internal.logical.interfaces import LogicalOperator, Operator
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.stats import StatsDict, Timer
from ray.data.block import (
    Block,
    BlockMetadata,
    BlockMetadataWithSchema,
    TaskExecWorkerStats,
)
from ray.data.context import DataContext
from ray.experimental.locations import get_local_object_locations

if TYPE_CHECKING:

    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)

# Timeout for waiting for metadata object to become available (in seconds)
METADATA_WAIT_TIMEOUT_S = 0.1

# TODO(hchen): Ray Core should have a common interface for these two types.
Waitable = Union[ray.ObjectRef, ObjectRefGenerator]


@dataclass(frozen=True)
class ObjectStoreUsage:
    """Per-op object store accounting.

    Attributes:
        internal: Bytes held by this op's currently-running tasks
            (outputs not yet yielded to the object store).
        outputs: Bytes this op has produced that are still live in
            the object store — its internal output queue, its
            ``OpState`` external output queue, and the downstream
            eligible ops' inputs.
    """

    internal: int
    outputs: int


class OpTask(ABC):
    """Abstract class that represents a task that is created by an PhysicalOperator.

    The task can be either a regular task or an actor task.
    """

    def __init__(
        self,
        task_index: int,
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        self._task_index: int = task_index
        self._task_resource_bundle: Optional[ExecutionResources] = task_resource_bundle

    def task_index(self) -> int:
        """Return the index of the task."""
        return self._task_index

    def get_requested_resource_bundle(self) -> Optional[ExecutionResources]:
        return self._task_resource_bundle

    @abstractmethod
    def get_waitable(self) -> Waitable:
        """Return the ObjectRef or ObjectRefGenerator to wait on."""
        ...

    def _cancel(self, force: bool):
        is_actor_task = not self.get_task_id().actor_id().is_nil()

        ray.cancel(
            self.get_waitable(),
            recursive=True,
            # NOTE: Actor tasks can't be force-cancelled
            force=force and not is_actor_task,
        )

    def get_task_id(self) -> ray.TaskID:
        object_ref = self.get_waitable()

        # Get generator's `ObjectRef`
        if isinstance(object_ref, ObjectRefGenerator):
            object_ref = object_ref._generator_ref
        return object_ref.task_id()


@dataclass(frozen=True)
class TaskExecDriverStats:
    """Task's execution stats reported from the driver"""

    task_output_backpressure_s: float


TaskDoneCallbackType = Callable[
    [Optional[Exception], Optional[TaskExecWorkerStats], Optional[TaskExecDriverStats]],
    None,
]


@dataclass
class DeferredEmit:
    """A pulled (block_ref, meta_ref) pair whose ``RefBundle`` emit is
    deferred until its metadata has been fetched.

    Populated by :meth:`DataOpTask.on_data_ready`; consumed by the
    ``MetadataPrefetcher``, which fetches ``meta_ref`` on a background
    thread and emits the pair (in per-op append order) once the bytes
    are available. The fetched bytes are carried by the prefetcher, not
    on this object.

    The budget size used inside ``on_data_ready`` comes from the block's
    local ``object_size`` (a local-only lookup, no RPC; pairs whose size
    isn't known yet are not consumed).
    """

    task: "DataOpTask"
    block_ref: "ray.ObjectRef[Block]"
    meta_ref: "ray.ObjectRef[BlockMetadata]"


class TaskGeneratorState(Enum):
    """Lifecycle of a ``DataOpTask``'s streaming generator, as seen by the
    data driver. Advances strictly ACTIVE -> DRAINED -> FINISHED."""

    # The generator task is still running and the driver hasn't pulled all of
    # its (block_ref, meta_ref) pairs yet.
    ACTIVE = auto()
    # The generator is exhausted (or the task failed) and the driver has pulled
    # all its pairs, but their metadata hasn't all been emitted yet.
    DRAINED = auto()
    # All pulled pairs' metadata has been emitted; the task is truly done and
    # its ``task_done_callback`` has fired.
    FINISHED = auto()


class DataOpTask(OpTask):
    """Represents an OpTask that handles Block data."""

    def __init__(
        self,
        task_index: int,
        streaming_gen: ObjectRefGenerator,
        output_ready_callback: Callable[[RefBundle], None] = lambda bundle: None,
        task_done_callback: TaskDoneCallbackType = lambda exc, worker_stats, driver_stats: None,
        block_ready_callback: Callable[
            [ray.ObjectRef[Block]], None
        ] = lambda block_ref: None,
        metadata_ready_callback: Callable[
            [ray.ObjectRef[BlockMetadata]], None
        ] = lambda metadata_ref: None,
        task_resource_bundle: Optional[ExecutionResources] = None,
        operator_name: str = "Unknown",
    ):
        """Create a DataOpTask
        Args:
            task_index: Index of the task. Used for callbacks.
            streaming_gen: The streaming generator of this task. It should yield blocks.
            output_ready_callback: The callback to call when a new RefBundle is output
                from the generator.
            task_done_callback: The callback to call when the task is done.
            block_ready_callback: A callback that's invoked when a new block reference
                is ready. This is exposed as a seam for testing.
            metadata_ready_callback: A callback that's invoked when a new block metadata
                reference is ready. This is exposed as a seam for testing.
            task_resource_bundle: The execution resources of this task.
            operator_name: The name of the physical operator that created this task.
                Used for logging the operator name in warnings/errors.
        """
        super().__init__(task_index, task_resource_bundle)
        # TODO(hchen): Right now, the streaming generator is required to yield a Block
        # and a BlockMetadata each time. We should unify task submission with an unified
        # interface. So each individual operator don't need to take care of the
        # BlockMetadata.
        self._streaming_gen = streaming_gen
        self._output_ready_callback = output_ready_callback
        self._task_done_callback = task_done_callback
        self._block_ready_callback = block_ready_callback
        self._metadata_ready_callback = metadata_ready_callback
        self._operator_name = operator_name

        # If the generator hasn't produced block metadata yet, or if the block metadata
        # object isn't available after we get a reference, we need store the pending
        # references and wait until Ray (re)constructs the block metadata. Either case
        # can happen if a node dies after producing a block.
        self._pending_block_ref: ray.ObjectRef[Block] = ray.ObjectRef.nil()
        self._pending_meta_ref: ray.ObjectRef[BlockMetadata] = ray.ObjectRef.nil()

        self._last_block_meta: Optional[BlockMetadata] = None

        # Generator lifecycle (see ``TaskGeneratorState``). The driver moves it
        # ACTIVE -> DRAINED (generator exhausted/failed, all pairs pulled) ->
        # FINISHED (all pairs' metadata emitted, ``task_done_callback`` fired).
        # Completion is postponed to FINISHED — rather than fired at DRAINED —
        # so a task's output is delivered before its completion is signalled.
        self._state: TaskGeneratorState = TaskGeneratorState.ACTIVE

        # If the task failed, the exception to pass to ``task_done_callback``
        # when it transitions to FINISHED (None for normal completion).
        self._task_error: Optional[Exception] = None

        # Count of this task's pulled pairs not yet emitted by the async
        # ``MetadataPrefetcher``. The DRAINED -> FINISHED transition happens
        # only once this reaches 0 (all of the task's bundles emitted).
        self._pending_emit_count: int = 0

        self._start_output_backpressure_s: Optional[float] = None
        self._total_output_backpressure_s: float = 0

    def get_waitable(self) -> ObjectRefGenerator:
        return self._streaming_gen

    def on_data_ready(
        self,
        max_bytes_to_read: Optional[int],
        deferred_emits: List[DeferredEmit],
    ) -> int:
        """Pull ready ``(block_ref, meta_ref)`` pairs from the streaming
        generator and append them to ``deferred_emits``; the
        ``MetadataPrefetcher`` later fetches each pair's metadata off-thread
        and emits the ``RefBundle``.

        A *deferred emit* is a pulled pair whose ``RefBundle`` we do **not**
        build here — we never call ``ray.get`` in this method. Our streaming
        generators yield a block ref then its metadata ref, in that order, so
        each loop pulls the two refs and defers them.

        Two refs aren't always both available, and this method handles those
        cases gracefully rather than blocking:
        - block ref not yet yielded -> stop, retry next call;
        - metadata ref not yet yielded -> stop, retry next call;
        - generator exhausted before yielding a block -> end of stream
          (normal completion); generator raises after a block -> task failure.
          Either way we move to ``DRAINED`` and postpone completion.

        For the output-budget loop we need each block's size. We read it from
        the block's local ``object_size`` via
        ``ray.experimental.get_local_object_locations`` (local-only, no RPC) —
        the driver owns the just-yielded block ref, so this is normally known.
        In the rare case it isn't, we fall back to fetching the metadata
        inline for the size (see below).

        Args:
            max_bytes_to_read: Max bytes of blocks to read. If None, all
                currently available pairs are drained.
            deferred_emits: List to which :class:`DeferredEmit` entries
                are appended.

        Returns:
            The number of bytes accounted for (for the budget loop).
        """
        bytes_read = 0

        self._track_task_output_backpressure(max_bytes_to_read)

        if self._state is not TaskGeneratorState.ACTIVE or self._pending_emit_count > 0:
            # Either the generator is already DRAINED (don't pull more), or
            # pairs pulled in a previous iteration are still awaiting their
            # background metadata fetch. Don't run ahead of them: pulling more
            # output would release the generator's backpressure and let the
            # producer get arbitrarily far ahead of unfetched metadata (the
            # pre-deferred code fetched each pair's metadata before pulling the
            # next one). Retry once the pending pairs have been emitted.
            return 0

        while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
            if self._pending_block_ref.is_nil():
                assert self._pending_meta_ref.is_nil(), (
                    "This method expects streaming generators to yield blocks then "
                    "metadata. So, if we have a reference to metadata but not the "
                    "block, it means there's an error in the implementation."
                )

                try:
                    self._pending_block_ref = self._streaming_gen._next_sync(
                        timeout_s=0
                    )
                except StopIteration:
                    # End of stream (normal completion): the generator yielded
                    # all its blocks. Move to DRAINED; completion fires once the
                    # already-pulled pairs have been emitted.
                    self._state = TaskGeneratorState.DRAINED
                    break

                if self._pending_block_ref.is_nil():
                    # The generator currently doesn't have new output.
                    # And it's not stopped yet.
                    break

                self._block_ready_callback(self._pending_block_ref)

            if self._pending_meta_ref.is_nil():
                try:
                    self._pending_meta_ref = self._streaming_gen._next_sync(
                        timeout_s=METADATA_WAIT_TIMEOUT_S
                    )
                except StopIteration:
                    # The generator should always yield 2 values (block and metadata)
                    # each time. If we get a StopIteration here, it means an error
                    # happened in the task.
                    # And in this case, the block_ref is the exception object.
                    # TODO(hchen): Ray Core should have a better interface for
                    # detecting and obtaining the exception.
                    try:
                        ray.get(self._pending_block_ref)
                        assert False, "Above ray.get should raise an exception."
                    except Exception as ex:
                        # Task failure. Like normal end-of-stream, move to
                        # DRAINED rather than firing inline: completion (now
                        # carrying ``ex``) fires only after this task's
                        # already-pulled pairs have emitted, preserving the
                        # pre-deferred emit-then-fail order. ``mark_done`` then
                        # surfaces ``ex`` for ``max_errored_blocks`` accounting.
                        self._task_error = ex
                        self._state = TaskGeneratorState.DRAINED
                        break

                if self._pending_meta_ref.is_nil():
                    # We have a reference to the block but the metadata isn't ready
                    # yet.
                    break

                self._metadata_ready_callback(self._pending_meta_ref)

            # Output-budget size from the block's local object_size (no RPC).
            # Normally known: the driver owns the just-yielded block ref, so
            # the value (which matches ``meta.size_bytes``) is in the local
            # object directory.
            info: Optional[Dict[str, Any]] = get_local_object_locations(
                [self._pending_block_ref]
            ).get(self._pending_block_ref)
            object_size: Optional[int] = (
                info.get("object_size") if info is not None else None
            )
            if object_size is None:
                # Rare: the location record for this block has no size yet
                # (``get_local_object_locations`` may omit the entry or its
                # size). Fall back to the metadata for the size, but with a
                # short timeout — if it isn't local yet either, leave the pair
                # pending and retry next round rather than blocking the loop.
                logger.warning(
                    "Local object_size unavailable for a block from operator "
                    "'%s'; falling back to its metadata for the output-budget "
                    "size.",
                    self._operator_name,
                )
                try:
                    meta_with_schema: BlockMetadataWithSchema = pickle.loads(
                        ray.get(self._pending_meta_ref, timeout=METADATA_WAIT_TIMEOUT_S)
                    )
                except ray.exceptions.GetTimeoutError:
                    break
                object_size = meta_with_schema.metadata.size_bytes

            # Defer the pair: no emit and no ``_last_block_meta`` update here.
            # The ``MetadataPrefetcher`` fetches the metadata off-thread and
            # emits the ``RefBundle`` (in append order) when it drains.
            deferred_emits.append(
                DeferredEmit(
                    task=self,
                    block_ref=self._pending_block_ref,
                    meta_ref=self._pending_meta_ref,
                )
            )
            # Charge the per-iteration output budget at pull time, not at emit
            # time: the block already exists in the object store and counts
            # toward memory backpressure regardless of whether its metadata
            # fetch later succeeds. Not refunded if the prefetcher drops the
            # pair on a fetch error — the budget is recomputed fresh each
            # scheduling iteration (see ``remaining_output_budget`` in
            # ``process_completed_tasks``), so there is no balance to restore.
            bytes_read += object_size
            self._pending_block_ref = ray.ObjectRef.nil()
            self._pending_meta_ref = ray.ObjectRef.nil()

        return bytes_read

    def _track_task_output_backpressure(self, max_bytes_to_read: Optional[int]):
        if max_bytes_to_read == 0:
            # Whenever provided `max_bytes_to_read == 0` we treat as task
            # being in output backpressure, therefore correspondingly starting
            # the timer (if necessary)
            if self._start_output_backpressure_s is None:
                self._start_output_backpressure_s = time.perf_counter()

        elif (
            max_bytes_to_read is None or max_bytes_to_read > 0
        ) and self._start_output_backpressure_s is not None:
            # Increment cumulative duration of task being in output
            # backpressure
            self._total_output_backpressure_s += (
                time.perf_counter() - self._start_output_backpressure_s
            )
            self._start_output_backpressure_s = None

    @property
    def has_finished(self) -> bool:
        """Whether the task is truly done — generator drained and all its
        pairs emitted (``task_done_callback`` fired)."""
        return self._state is TaskGeneratorState.FINISHED

    @property
    def operator_name(self) -> str:
        """Name of the physical operator that created this task."""
        return self._operator_name

    @property
    def task_error(self) -> Optional[Exception]:
        """The task's failure exception if it failed, else None. Set when the
        task becomes DRAINED; surfaced by the prefetcher for
        ``max_errored_blocks`` accounting when it transitions to FINISHED."""
        return self._task_error

    def is_drained(self) -> bool:
        """Whether the generator is exhausted/failed and all pairs pulled, but
        not all of their metadata has been emitted yet (DRAINED). The pair
        emits and the eventual ``mark_done`` happen in the prefetcher."""
        return self._state is TaskGeneratorState.DRAINED

    def has_pending_emits(self) -> bool:
        """Whether this task still has pulled pairs not yet emitted."""
        return self._pending_emit_count > 0

    def mark_pending(self) -> None:
        """Account a pulled pair as awaiting emission. Called by the
        prefetcher when a pair is queued for its background fetch."""
        self._pending_emit_count += 1

    def mark_emitted(self) -> None:
        """Account a pulled pair as emitted (or dropped). Called by the
        prefetcher once a pair's metadata is fetched and handled."""
        self._pending_emit_count -= 1

    def emit_block(self, block_ref: "ray.ObjectRef[Block]", meta_bytes: bytes) -> None:
        """Build and emit one pulled block's ``RefBundle`` from its fetched
        metadata bytes, and record it as this task's last block. Called by the
        ``MetadataPrefetcher`` once the metadata is available, on the executor
        thread."""
        meta_with_schema: BlockMetadataWithSchema = pickle.loads(meta_bytes)
        meta = meta_with_schema.metadata
        self._output_ready_callback(
            RefBundle(
                [BlockEntry(block_ref, meta)],
                owns_blocks=True,
                schema=meta_with_schema.schema,
            ),
        )
        self._last_block_meta = meta

    def mark_done(self) -> None:
        """Transition DRAINED -> FINISHED and fire ``task_done_callback``.
        For a failed task, pass the error with no stats (matching the
        pre-deferred failure path); otherwise pass the final block's stats.
        Called by the ``MetadataPrefetcher`` once all this task's pairs have
        emitted, on the executor thread."""
        if self._task_error is not None:
            self._task_done_callback(
                self._task_error,
                None,  # TaskExecStats
                None,  # TaskExecDriverStats
            )
        else:
            self._task_done_callback(
                None,  # exception
                self._last_block_meta.task_exec_stats
                if self._last_block_meta is not None
                else None,
                TaskExecDriverStats(
                    task_output_backpressure_s=self._total_output_backpressure_s,
                ),
            )
        self._state = TaskGeneratorState.FINISHED


class MetadataOpTask(OpTask):
    """Represents an OpTask that only handles metadata, instead of Block data."""

    def __init__(
        self,
        task_index: int,
        object_ref: ray.ObjectRef,
        task_done_callback: Callable[[], None],
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        """Initialize a metadata-only OpTask.

        Args:
            task_index: Index identifying this task within its operator.
            object_ref: The ObjectRef of the task.
            task_done_callback: The callback to call when the task is done.
            task_resource_bundle: Optional resource bundle reserved for this task.
        """
        super().__init__(task_index, task_resource_bundle)
        self._object_ref = object_ref
        self._task_done_callback = task_done_callback

    def get_waitable(self) -> ray.ObjectRef:
        return self._object_ref

    def on_task_finished(self):
        """Callback when the task is finished."""
        self._task_done_callback()


class PhysicalOperator(Operator):
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Physical operators are stateful and non-serializable; they live on the driver side
    of the Dataset only.

    Here's a simple example of implementing a basic "Map" operator:

        class MapOperator(PhysicalOperator):
            def __init__(self):
                self.active_tasks = []

            def add_input(self, refs, _):
                self.active_tasks.append(map_task.remote(refs))

            def has_next(self):
                ready, _ = ray.wait(self.active_tasks, timeout=0)
                return len(ready) > 0

            def get_next(self):
                ready, remaining = ray.wait(self.active_tasks, num_returns=1)
                self.active_tasks = remaining
                return ready[0]

    Note that the above operator fully supports both bulk and streaming execution,
    since `add_input` and `get_next` can be called in any order. In bulk execution
    (now deprecated), all inputs would be added up-front, but in streaming
    execution (now the default execution mode) the calls could be interleaved.
    """

    _OPERATOR_ID_LABEL_KEY = "__data_operator_id"

    def __init__(
        self,
        name: str,
        input_dependencies: List["PhysicalOperator"],
        data_context: DataContext,
        target_max_block_size_override: Optional[int] = None,
        num_output_splits: int = 1,
    ):
        self._name = name
        self._input_dependencies = input_dependencies
        self._output_dependencies: List["PhysicalOperator"] = []

        for input in input_dependencies:
            assert isinstance(
                input, PhysicalOperator
            ), "Must inherit from PhysicalOperator"

            # Assert that number of output splits produced by this operator is not
            # exceeded by its input deps
            assert num_output_splits >= input.num_output_splits(), (
                f"Number of output splits of the upstream may not exceed that one of the downstream: "
                f"{num_output_splits} for {self}, {input.num_output_splits()} for {input}"
            )

        # Number of output splits this operator partitions its output by
        self._num_output_splits = num_output_splits

        self._wire_output_deps(input_dependencies)
        self._inputs_complete = not input_dependencies
        self._output_block_size_option_override = OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size_override
        )
        self._started = False
        self._shutdown = False
        self._in_task_submission_backpressure = False
        self._task_submission_backpressure_policy: Optional[str] = None
        self._in_task_output_backpressure = False
        self._task_output_backpressure_policy: Optional[str] = None
        self._estimated_num_output_bundles = None
        self._estimated_output_num_rows = None
        self._is_execution_marked_finished = False
        # The LogicalOperator(s) which were translated to create this PhysicalOperator.
        # Set via `PhysicalOperator.set_logical_operators()`.
        self._logical_operators: List[LogicalOperator] = []
        self._data_context = data_context
        self._id = str(uuid.uuid4())
        # Initialize metrics after data_context is set
        self._metrics = OpRuntimeMetrics(self)

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

    @property
    def id(self) -> str:
        """Return a unique identifier for this operator."""
        return self._id

    @property
    def data_context(self) -> DataContext:
        return self._data_context

    # Override the following methods to correct type hints.

    @property
    def name(self) -> str:
        return self._name

    @property
    def input_dependencies(self) -> List["PhysicalOperator"]:
        return self._input_dependencies

    @property
    def dag_str(self) -> str:
        """String representation of the whole physical DAG."""
        if self.input_dependencies:
            out_str = ", ".join([x.dag_str for x in self.input_dependencies])
            out_str += " -> "
        else:
            out_str = ""
        out_str += f"{self.__class__.__name__}[{self._name}]"
        return out_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._name}]"

    def __str__(self) -> str:
        return repr(self)

    @property
    def output_dependencies(self) -> List["PhysicalOperator"]:
        return self._output_dependencies

    def post_order_iter(self) -> Iterator["PhysicalOperator"]:
        return super().post_order_iter()  # type: ignore

    def _apply_transform(
        self, transform: Callable[["PhysicalOperator"], "PhysicalOperator"]
    ) -> "PhysicalOperator":
        # 1) Recursively transform input operators first.
        transformed_input_ops = []
        has_changes = False

        for input_op in self.input_dependencies:
            transformed_input_op = input_op._apply_transform(transform)
            transformed_input_ops.append(transformed_input_op)
            if transformed_input_op is not input_op:
                has_changes = True

        # 2) If any input changed, create a shallow copy of the current node,
        # rebind its inputs, and rewire reverse dependencies from old inputs
        # to transformed inputs.
        if has_changes:
            target = self._copy_for_transform()
            for input_op in self.input_dependencies:
                assert isinstance(input_op, PhysicalOperator), input_op
                input_op._output_dependencies = [
                    dep for dep in input_op._output_dependencies if dep is not self
                ]
            target._input_dependencies = transformed_input_ops
            target._wire_output_deps(transformed_input_ops)
        else:
            target = self

        # 3) Apply transform on the current node itself. If transform replaces
        # the current node, rewire reverse dependencies from old node inputs
        # to the returned replacement node inputs.
        # Returning the same node must not mutate inputs in-place.
        original_inputs = tuple(target.input_dependencies)
        transformed_target = transform(target)
        if transformed_target is not target:
            for input_op in original_inputs:
                assert isinstance(input_op, PhysicalOperator), input_op
                input_op._output_dependencies = [
                    dep for dep in input_op._output_dependencies if dep is not target
                ]
            transformed_target._rewire_output_deps(
                target, transformed_target.input_dependencies
            )
        else:
            assert (
                tuple(transformed_target.input_dependencies) == original_inputs
            ), "In-place input mutation is not supported; return a new node instead."
        return transformed_target

    def _copy_for_transform(self) -> "PhysicalOperator":
        # copy.copy() is not safe here because PhysicalOperator.__reduce__()
        # intentionally raises. Use a side-effect-free shallow copy to avoid
        # re-running __init__ wiring/ID/metrics initialization during transform.
        target = object.__new__(type(self))
        target.__dict__ = self.__dict__.copy()
        # The transformed node should have a distinct identity and metrics owner.
        target._id = str(uuid.uuid4())
        target._metrics = OpRuntimeMetrics(target)
        # The copied node belongs to a new transformed DAG. Reverse edges are
        # rewired by parents, so avoid carrying stale downstream references.
        target._output_dependencies = []
        return target

    def _rewire_output_deps(
        self,
        source_op: "PhysicalOperator",
        input_dependencies: List["PhysicalOperator"],
    ) -> None:
        for input_op in input_dependencies:
            assert isinstance(input_op, PhysicalOperator), input_op
            input_op._output_dependencies = [
                dep for dep in input_op._output_dependencies if dep is not source_op
            ]
            if self not in input_op._output_dependencies:
                input_op._output_dependencies.append(self)

    def _wire_output_deps(self, input_dependencies: List["PhysicalOperator"]) -> None:
        for input_op in input_dependencies:
            assert isinstance(input_op, PhysicalOperator), input_op
            input_op._output_dependencies.append(self)

    def set_logical_operators(
        self,
        *logical_ops: LogicalOperator,
    ):
        self._logical_operators = list(logical_ops)

    @property
    def target_max_block_size_override(self) -> Optional[int]:
        """
        Target max block size output by this operator. If this returns None,
        then the default from DataContext should be used.
        """
        if self._output_block_size_option_override is None:
            return None
        else:
            return self._output_block_size_option_override.target_max_block_size

    def override_target_max_block_size(self, target_max_block_size: Optional[int]):
        self._output_block_size_option_override = OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size
        )

    def mark_execution_finished(self):
        """Manually mark that this operator has finished execution."""
        self._is_execution_marked_finished = True

    def has_execution_finished(self) -> bool:
        """Return True when this operator has finished execution.

        The outputs may or may not have been taken.
        """
        from ..operators.base_physical_operator import InternalQueueOperatorMixin

        internal_input_queue_num_blocks = 0
        if isinstance(self, InternalQueueOperatorMixin):
            internal_input_queue_num_blocks = self.internal_input_queue_num_blocks()

        # NOTE: Execution is considered finished if
        #   - The operator was explicitly marked finished OR
        #   - The following auto-completion conditions are met
        #       - All input blocks have been ingested
        #       - Internal queue is empty
        #       - There are no active or pending tasks
        return self._is_execution_marked_finished or (
            self._inputs_complete
            and self.num_active_tasks() == 0
            and internal_input_queue_num_blocks == 0
        )

    def has_completed(self) -> bool:
        """Returns whether this operator has been fully completed.

        An operator is completed iff:
            * The operator has finished execution (i.e., `has_execution_finished()` is True).
            * All outputs have been taken (i.e., `has_next()` is False) from it.
        """
        from ..operators.base_physical_operator import InternalQueueOperatorMixin

        internal_output_queue_num_blocks = 0
        if isinstance(self, InternalQueueOperatorMixin):
            internal_output_queue_num_blocks = self.internal_output_queue_num_blocks()

        # NOTE: We check for (internal_output_queue_size == 0) and
        # (not self.has_next()) because ReorderingBundleQueue can
        # return False for self.has_next(), but have a non-empty queue size.
        # Draining the internal output queue is important to free object refs.
        return (
            self.has_execution_finished()
            and internal_output_queue_num_blocks == 0
            # TODO following check is redundant; remove
            and not self.has_next()
        )

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatasetStats."""
        raise NotImplementedError

    @property
    def metrics(self) -> OpRuntimeMetrics:
        """Returns the runtime metrics of this operator."""
        self._metrics._extra_metrics = self._extra_metrics()
        return self._metrics

    def _extra_metrics(self) -> Dict[str, Any]:
        """Subclasses should override this method to report extra metrics
        that are specific to them."""
        return {}

    def _get_logical_args(self) -> Dict[str, Dict[str, Any]]:
        """Return the logical arguments that were translated to create this
        PhysicalOperator."""
        res = {}
        for i, logical_op in enumerate(self._logical_operators):
            logical_op_id = f"{logical_op}_{i}"
            res[logical_op_id] = logical_op._get_args()
        return res

    # TODO(@balaji): Disambiguate this with `incremental_resource_usage`.
    def per_task_resource_allocation(
        self: "PhysicalOperator",
    ) -> ExecutionResources:
        """The amount of logical resources used by each task.

        For regular tasks, these are the resources required to schedule a task. For
        actor tasks, these are the resources required to schedule an actor divided by
        the number of actor threads (i.e., `max_concurrency`).

        Returns:
            The resource requirement per task.
        """
        return ExecutionResources.zero()

    def get_max_concurrency_limit(self: "PhysicalOperator") -> Optional[int]:
        """The maximum number of tasks that can be run concurrently.

        Some operators manually configure a maximum concurrency. For example, if you
        specify `concurrency` in `map_batches`.
        """
        return None

    # TODO(@balaji): Disambiguate this with `base_resource_usage`.
    def min_scheduling_resources(
        self: "PhysicalOperator",
    ) -> ExecutionResources:
        """The minimum resource bundle required to schedule a worker.

        For regular tasks, this is the resources required to schedule a task. For actor
        tasks, this is the resources required to schedule an actor.
        """
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        """Return any extra status to be displayed in the operator progress bar.

        For example, `<N> actors` to show current number of actors in an actor pool.
        """
        return ""

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator,
        or ``None`` if unable to provide a reasonable estimate (for example,
        if no tasks have finished yet).

        The value returned may be an estimate based off the consumption so far.
        This is useful for reporting progress.

        Subclasses should either override this method, or update
        ``self._estimated_num_output_bundles`` appropriately.
        """
        return self._estimated_num_output_bundles

    def num_output_rows_total(self) -> Optional[int]:
        """Returns the total number of output rows of this operator,
        or ``None`` if unable to provide a reasonable estimate (for example,
        if no tasks have finished yet).

        The value returned may be an estimate based off the consumption so far.
        This is useful for reporting progress.

        Subclasses should either override this method, or update
        ``self._estimated_output_num_rows`` appropriately.
        """
        return self._estimated_output_num_rows

    def num_output_splits(self) -> int:
        """Returns the number of splits for this operator's output is partitioned into.

        Most operators have a single output split.
        """
        return self._num_output_splits

    def start(self, options: ExecutionOptions) -> None:
        """Called by the executor when execution starts for an operator.

        Args:
            options: The global options used for the overall execution.
        """
        self._started = True

    def can_add_input(self) -> bool:
        """Return whether it is desirable to add input to this operator right now.

        Operators can customize the implementation of this method to apply additional
        backpressure (e.g., waiting for internal actors to be created).
        """
        return True

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        """Called when an upstream result is available.

        Inputs may be added in any order, and calls to `add_input` may be interleaved
        with calls to `get_next` / `has_next` to implement streaming execution.

        Subclasses should override `_add_input_inner` instead of this method.

        Args:
            refs: The ref bundle that should be added as input.
            input_index: The index identifying the input dependency producing the
                input. For most operators, this is always `0` since there is only
                one upstream input operator.
        """
        assert 0 <= input_index < len(self._input_dependencies), (
            f"Input index out of bounds (total inputs {len(self._input_dependencies)}, "
            f"index is {input_index})"
        )

        self._metrics.on_input_received(refs)
        self._add_input_inner(refs, input_index)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Subclasses should override this method to implement `add_input`."""
        raise NotImplementedError

    def input_done(self, input_index: int) -> None:
        """Called when the upstream operator at index `input_index` has_completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for the given input index.
        """
        pass

    def all_inputs_done(self) -> None:
        """Called when all upstream operators has_completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for any input index.
        """
        self._inputs_complete = True

    def has_next(self) -> bool:
        """Returns when a downstream output is available.

        When this returns true, it is safe to call `get_next()`.
        """
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Get the next downstream output.

        It is only allowed to call this if `has_next()` has returned True.

        Subclasses should override `_get_next_inner` instead of this method.
        """
        output = self._get_next_inner()
        self._metrics.on_output_taken(output)
        return output

    def _get_next_inner(self) -> RefBundle:
        """Subclasses should override this method to implement `get_next`."""
        raise NotImplementedError

    def get_active_tasks(self) -> List[OpTask]:
        """Get a list of the active tasks of this operator.

        Subclasses should return *all* running normal/actor tasks. The
        StreamingExecutor will wait on these tasks and trigger callbacks.
        """
        return []

    def num_active_tasks(self) -> int:
        """Return the number of active tasks.

        This method is used for 2 purposes:
        * Determine if this operator is completed.
        * Displaying active task info in the progress bar.
        Thus, the return value can be less than `len(get_active_tasks())`,
        if some tasks are not needed for the above purposes. E.g., for the
        actor pool map operator, readiness checking tasks can be excluded
        from `num_active_tasks`, but they should be included in
        `get_active_tasks`.

        Subclasses can override this as a performance optimization.
        """
        return len(self.get_active_tasks())

    def throttling_disabled(self) -> bool:
        """Whether to disable resource throttling for this operator.

        This should return True for operators that only manipulate bundle metadata
        (e.g., the OutputSplitter operator). This hints to the execution engine that
        these operators should not be throttled based on resource usage.
        """
        return False

    def shutdown(self, timer: Timer, force: bool = False) -> None:
        """Abort execution and release all resources used by this operator.

        This release any Ray resources acquired by this operator such as active
        tasks, actors, and objects.
        """
        if self._shutdown:
            return
        elif not self._started:
            raise ValueError("Operator must be started before being shutdown.")

        # Mark operator as shut down
        self._shutdown = True
        # Time shutdown sequence duration
        with timer.timer():
            self._do_shutdown(force)

    def _do_shutdown(self, force: bool):
        # Default implementation simply cancels any outstanding active task
        self._cancel_active_tasks(force=force)

    def current_logical_usage(self) -> ExecutionResources:
        """Returns the current estimated CPU, GPU, and memory usage of this operator,
        excluding object store memory.

        This method is called by the executor to decide how to allocate resources
        between different operators.
        """
        return ExecutionResources.zero()

    def estimate_object_store_usage(self, state: "OpState") -> ObjectStoreUsage:
        """Returns the bytes this operator contributes to the global object
        store budget. Subclasses may override this when their object store
        footprint doesn't match the generic model.
        """
        # Operator's internal Object Store usage
        mem_op_internal = self.metrics.obj_store_mem_pending_task_outputs or 0

        # Operator's outputs' Object Store usage
        op_outputs_bytes = (
            # Internal output queue
            self.metrics.obj_store_mem_internal_outqueue
            +
            # External output queue
            state.output_queue_bytes()
        )

        # TODO fix ineligible ops: this needs to include usage of all of OS
        #      for ineligible ops
        #
        # Outputs of this operator used downstream
        used_op_outputs_bytes = sum(
            (
                downstream_op.metrics.obj_store_mem_internal_inqueue_for_input(
                    downstream_op.input_dependencies.index(self)
                )
                + downstream_op.metrics.obj_store_mem_pending_task_inputs
            )
            for downstream_op in self.output_dependencies
        )
        return ObjectStoreUsage(
            internal=int(mem_op_internal),
            outputs=int(op_outputs_bytes + used_op_outputs_bytes),
        )

    def running_logical_usage(self) -> ExecutionResources:
        """Returns the estimated running CPU, GPU, and memory usage of this operator,
        excluding object store memory.

        This method is called by the resource manager and the streaming
        executor to display the number of currently running CPUs, GPUs, and memory in
        the progress bar.

        Note, this method returns `current_logical_usage() -
        pending_logical_usage()` by default. Subclasses should only override
        `pending_logical_usage()` if needed.
        """
        usage = self.current_logical_usage()
        usage = usage.subtract(self.pending_logical_usage())
        return usage

    def pending_logical_usage(self) -> ExecutionResources:
        """Returns the estimated pending CPU, GPU, and memory usage of this operator,
        excluding object store memory.

        This method is called by the resource manager and the streaming
        executor to display the number of currently pending actors in the
        progress bar.
        """
        return ExecutionResources.zero()

    def min_max_resource_requirements(
        self,
    ) -> Tuple[ExecutionResources, ExecutionResources]:
        """Returns lower/upper boundary of resource requirements for this operator:

        - Minimal: lower bound (min) of resources required to start this operator
        (for most operators this is 0, except the ones that utilize actors)
        - Maximum: upper bound (max) of how many resources this operator could
        utilize.
        """
        return ExecutionResources.zero(), ExecutionResources.inf()

    def incremental_resource_usage(self) -> ExecutionResources:
        """Returns the incremental resources required for processing another input.

        For example, an operator that launches a task per input could return
        ExecutionResources(cpu=1) as its incremental usage.
        """
        return ExecutionResources()

    def notify_in_task_submission_backpressure(
        self, in_backpressure: bool, policy_name: Optional[str] = None
    ) -> None:
        """Called periodically from the executor to update internal in backpressure
        status for stats collection purposes.

        Args:
            in_backpressure: Value this operator's in_backpressure should be set to.
            policy_name: Name of the backpressure policy that triggered.
        """
        # only update on change to in_backpressure
        if self._in_task_submission_backpressure != in_backpressure:
            self._metrics.on_toggle_task_submission_backpressure(in_backpressure)
            self._in_task_submission_backpressure = in_backpressure
        self._task_submission_backpressure_policy = policy_name

    def notify_in_task_output_backpressure(
        self, in_backpressure: bool, policy_name: Optional[str] = None
    ) -> None:
        """Called periodically from the executor to update internal output backpressure
        status for stats collection purposes.

        Args:
            in_backpressure: Value this operator's output backpressure should be set to.
            policy_name: Name of the backpressure policy that triggered.
        """
        # only update on change to in_backpressure
        if self._in_task_output_backpressure != in_backpressure:
            self._metrics.on_toggle_task_output_backpressure(in_backpressure)
            self._in_task_output_backpressure = in_backpressure
        self._task_output_backpressure_policy = policy_name

    def get_autoscaling_actor_pools(self) -> List[AutoscalingActorPool]:
        """Return a list of `AutoscalingActorPool`s managed by this operator."""
        return []

    def supports_fusion(self) -> bool:
        """Returns ```True``` if this operator can be fused with other operators."""
        return False

    def refresh_state(self):
        """Refreshes the state of the operator at runtime.

        This method will be called at runtime in each StreamingExecutor iteration.
        Subclasses can override it to account for asynchronous updates, like restarting
        actors, retrying tasks, or lost objects which are NOT transparent to the
        StreamingExecutor.

        TODO: Currently this method is synchronous. We should consider making this async,
        or calling it in an asynchronous context.
        """
        pass

    def get_actor_info(self) -> ActorPoolInfo:
        """Returns the current status of actors being used by the operator"""
        return ActorPoolInfo(
            running=0,
            restarting=0,
            pending=0,
            active=0,
            idle=0,
            pool_utilization=0,
            tasks_in_flight=0,
        )

    def _cancel_active_tasks(self, force: bool):
        tasks: List[OpTask] = self.get_active_tasks()

        # Interrupt all (still) running tasks immediately
        for task in tasks:
            task._cancel(force=force)

        # In case of forced cancellation block until task actually return
        # to guarantee all tasks are done upon return from this method
        if force:
            # Wait for all tasks to get cancelled before returning
            for task in tasks:
                try:
                    ray.get(task.get_waitable())
                except ray.exceptions.RayError:
                    # Cancellation either succeeded, or the task might have already
                    # failed with a different error, or cancellation failed.
                    # In all cases, we swallow the exception.
                    pass

    def upstream_op_num_outputs(self):
        upstream_op_num_outputs = sum(
            op.num_outputs_total() or 0 for op in self.input_dependencies
        )
        return upstream_op_num_outputs


class ReportsExtraResourceUsage(abc.ABC):
    @abc.abstractmethod
    def extra_resource_usage(self: PhysicalOperator) -> ExecutionResources:
        """Returns resources used by this operator beyond standard accounting."""
        ...


def estimate_total_num_of_blocks(
    num_tasks_submitted: int,
    upstream_op_num_outputs: int,
    metrics: OpRuntimeMetrics,
    total_num_tasks: Optional[int] = None,
) -> Tuple[int, int, int]:
    """This method is trying to estimate total number of blocks/rows based on
    - How many outputs produced by the input deps
    - How many blocks/rows produced by tasks of this operator
    """

    if (
        upstream_op_num_outputs > 0
        and metrics.average_num_inputs_per_task
        and metrics.average_num_outputs_per_task
        and metrics.average_rows_outputs_per_task
    ):
        estimated_num_tasks = total_num_tasks
        if estimated_num_tasks is None:
            estimated_num_tasks = (
                upstream_op_num_outputs / metrics.average_num_inputs_per_task
            )

        estimated_num_output_bundles = round(
            estimated_num_tasks * metrics.average_num_outputs_per_task
        )
        estimated_output_num_rows = round(
            estimated_num_tasks * metrics.average_rows_outputs_per_task
        )

        return (
            estimated_num_tasks,
            estimated_num_output_bundles,
            estimated_output_num_rows,
        )

    return (0, 0, 0)
