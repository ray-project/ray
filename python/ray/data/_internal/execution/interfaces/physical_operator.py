import abc
import logging
import pickle
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
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
from ray.data.block import Block, BlockMetadata, TaskExecWorkerStats
from ray.data.context import DataContext
from ray.experimental.locations import get_local_object_locations

logger = logging.getLogger(__name__)

# Timeout for getting metadata from Ray object references (in seconds)
METADATA_GET_TIMEOUT_S = 1.0

# Timeout for waiting for metadata object to become available (in seconds)
METADATA_WAIT_TIMEOUT_S = 0.1

# TODO(hchen): Ray Core should have a common interface for these two types.
Waitable = Union[ray.ObjectRef, ObjectRefGenerator]


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
    deferred until after a batched ``ray.get(meta_refs)`` in the caller.

    Populated by :meth:`DataOpTask.on_data_ready`; consumed by
    :func:`ray.data._internal.execution.streaming_executor_state.process_completed_tasks`.

    Every pair is deferred (``meta_bytes`` starts ``None``) so all metadata is
    fetched in ONE batched ``ray.get``. The budget size used inside
    ``on_data_ready`` comes from the block's local ``object_size`` when known
    (``local_object_size`` set), or the operator's running average block size
    on a miss (``local_object_size is None``). ``replay_deferred_emits`` fills
    ``meta_bytes`` from the batched fetch and uses ``local_object_size`` +
    the exact ``meta.size_bytes`` for the hit-rate / closeness telemetry.
    """

    task: "DataOpTask"
    block_ref: "ray.ObjectRef[Block]"
    meta_ref: "ray.ObjectRef[BlockMetadata]"
    meta_bytes: Optional[bytes] = None
    # The block's locally-known object_size (no RPC), or None on a miss.
    local_object_size: Optional[int] = None


# Process-wide running average block size per operator (keyed by operator
# name), [sum_bytes, count]. Updated from exact ``meta.size_bytes`` in
# ``replay_deferred_emits`` and read in ``on_data_ready`` to estimate the
# budget size when a block's size isn't known locally (the rare miss),
# avoiding a per-ref ``ray.get`` so all metadata flows through one batched
# fetch.
_op_avg_block_size: Dict[str, List[int]] = {}


def _record_block_size(op_name: str, size_bytes: int) -> None:
    acc = _op_avg_block_size.get(op_name)
    if acc is None:
        _op_avg_block_size[op_name] = [size_bytes, 1]
    else:
        acc[0] += size_bytes
        acc[1] += 1


def _avg_block_size(op_name: str) -> int:
    """Average observed block size for ``op_name``; 0 if none seen yet."""
    acc = _op_avg_block_size.get(op_name)
    if acc is None or acc[1] == 0:
        return 0
    return acc[0] // acc[1]


# --- Local-size probe telemetry (see PR #63904) ----------------------------
# Recorded in ``replay_deferred_emits`` where both the block's locally-known
# ``object_size`` and the exact ``meta.size_bytes`` are available: how often
# the size is known locally (hit rate) and how close the local size is to the
# metadata size (aggregate ratio, within-1%, p50/p90/max relative diff).
# Surfaces whether the local size lookup fully replaces the metadata fetch.
_PROBE_REL_DIFF_BUCKET = 0.001  # 0.1% resolution
_PROBE_REL_DIFF_NBUCKETS = 1001  # 0..100% in 0.1% steps + overflow
_PROBE_LOG_EVERY = 5000  # log a cumulative snapshot every N probed pairs


class _LocalSizeProbe:
    total = 0
    hits = 0
    sum_object = 0
    sum_meta = 0
    within_1pct = 0
    max_rel_diff = 0.0
    rel_diff_hist: List[int] = [0] * _PROBE_REL_DIFF_NBUCKETS


def _record_local_size_probe(
    local_object_size: Optional[int], meta_size_bytes: int
) -> None:
    p = _LocalSizeProbe
    p.total += 1
    if local_object_size is not None:
        p.hits += 1
        p.sum_object += local_object_size
        p.sum_meta += meta_size_bytes
        if meta_size_bytes > 0:
            rel = abs(local_object_size - meta_size_bytes) / meta_size_bytes
            if rel <= 0.01:
                p.within_1pct += 1
            if rel > p.max_rel_diff:
                p.max_rel_diff = rel
            bucket = min(
                int(rel / _PROBE_REL_DIFF_BUCKET), _PROBE_REL_DIFF_NBUCKETS - 1
            )
            p.rel_diff_hist[bucket] += 1
    if p.total % _PROBE_LOG_EVERY == 0:
        s = local_size_probe_stats()
        logger.info(
            "[local-size-probe] %d/%d (%.1f%%) pairs had a local object_size; "
            "object_size/meta ratio=%.4f; rel diff p50=%.2f%% p90=%.2f%% max=%.2f%%.",
            s["local_size_probe_hits"],
            s["local_size_probe_total"],
            s["local_size_probe_hit_pct"],
            s["local_size_probe_size_ratio"],
            s["local_size_probe_rel_diff_p50_pct"],
            s["local_size_probe_rel_diff_p90_pct"],
            s["local_size_probe_max_rel_diff_pct"],
        )


def _probe_rel_diff_pct(frac: float) -> float:
    """Approx percentile (%) of per-hit |object-meta|/meta from the histogram."""
    hist = _LocalSizeProbe.rel_diff_hist
    total = sum(hist)
    if total == 0:
        return 0.0
    target = frac * total
    cumulative = 0
    for i, count in enumerate(hist):
        cumulative += count
        if cumulative >= target:
            return 100.0 * i * _PROBE_REL_DIFF_BUCKET
    return 100.0 * (_PROBE_REL_DIFF_NBUCKETS - 1) * _PROBE_REL_DIFF_BUCKET


def local_size_probe_stats() -> Dict[str, Any]:
    """Cumulative local-size probe stats as a flat dict for benchmark metrics."""
    p = _LocalSizeProbe
    return {
        "local_size_probe_total": p.total,
        "local_size_probe_hits": p.hits,
        "local_size_probe_hit_pct": (100.0 * p.hits / p.total) if p.total else 0.0,
        "local_size_probe_size_ratio": (
            p.sum_object / p.sum_meta if p.sum_meta else 0.0
        ),
        "local_size_probe_within_1pct_pct": (
            100.0 * p.within_1pct / p.hits if p.hits else 0.0
        ),
        "local_size_probe_rel_diff_p50_pct": _probe_rel_diff_pct(0.50),
        "local_size_probe_rel_diff_p90_pct": _probe_rel_diff_pct(0.90),
        "local_size_probe_max_rel_diff_pct": 100.0 * p.max_rel_diff,
    }


def reset_local_size_probe() -> None:
    """Reset the process-wide local-size probe counters (call before a run)."""
    p = _LocalSizeProbe
    p.total = 0
    p.hits = 0
    p.sum_object = 0
    p.sum_meta = 0
    p.within_1pct = 0
    p.max_rel_diff = 0.0
    p.rel_diff_hist = [0] * _PROBE_REL_DIFF_NBUCKETS


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
        self._has_finished = False

        # When ``on_data_ready`` is invoked in deferred-emit mode and the
        # streaming gen raises ``StopIteration``, we postpone firing
        # ``task_done_callback`` until after the caller's deferred-emit
        # replay updates ``_last_block_meta`` to the actual final meta.
        # The caller (``process_completed_tasks``) walks tasks with this
        # flag set and fires the callback once the replay is complete.
        self._task_done_pending: bool = False

        # Count of this task's deferred pairs not yet emitted by the async
        # ``MetadataPrefetcher``. The postponed ``task_done_callback`` fires
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
        """Pull ready pairs from the streaming generator and append them
        to ``deferred_emits`` for the caller to batch-fetch and replay.

        Per pulled ``(block_ref, meta_ref)`` pair, the block's
        ``object_size`` is looked up via
        ``ray.experimental.get_local_object_locations`` — local-only,
        no RPC.

        Every pair is appended with ``meta_bytes=None`` so all metadata is
        fetched in ONE batched ``ray.get(meta_refs)`` by the caller — a single
        metadata-fetch path, no per-ref ``ray.get`` here.

        - **Known size**: the budget loop uses the local ``object_size``
          directly (recorded on the entry for telemetry).
        - **Unknown size** (rare — ``object_size`` is locally known for ~all
          pairs): the budget loop uses the operator's running average block
          size (``_avg_block_size``); the exact metadata still arrives via the
          batched fetch.

        No ``RefBundle`` is emitted and ``_last_block_meta`` is not
        updated inside this call — both happen in
        :func:`replay_deferred_emits`, which iterates the deferred list
        in append order to preserve gen-yield emission order. The
        terminal ``task_done_callback`` for end-of-stream is postponed
        (``_task_done_pending`` set to True) so it fires after
        ``_last_block_meta`` reflects the replayed state.

        Args:
            max_bytes_to_read: Max bytes of blocks to read. If None, all
                currently available pairs are drained.
            deferred_emits: List to which :class:`DeferredEmit` entries
                are appended.

        Returns: The number of bytes accounted for (for the budget loop).
        """
        bytes_read = 0

        self._track_task_output_backpressure(max_bytes_to_read)

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
                    # Defer firing ``task_done_callback`` until after the
                    # caller's deferred-replay updates
                    # ``_last_block_meta`` with the final processed meta.
                    self._task_done_pending = True
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
                        self._task_done_callback(
                            ex,
                            None,  # TaskExecStats
                            None,  # TaskExecDriverStats
                        )

                        self._has_finished = True
                        raise ex from None

                if self._pending_meta_ref.is_nil():
                    # We have a reference to the block but the metadata isn't ready
                    # yet.
                    break

                self._metadata_ready_callback(self._pending_meta_ref)

            # Local size lookup (no RPC). The block's object_size — when
            # known — is the value that matches ``meta.size_bytes`` used
            # by the budget loop. A known size also implies the
            # streaming gen has yielded both the block AND its metadata
            # (yields happen in order), so the batched ``ray.get`` the
            # caller will do later won't block.
            size_known_locally: Optional[int] = None
            info = get_local_object_locations([self._pending_block_ref]).get(
                self._pending_block_ref
            )
            if info is not None:
                size_known_locally = info.get("object_size")

            if size_known_locally is not None:
                # Defer everything: no ray.get inside the loop, no emit,
                # no _last_block_meta update. Caller will batch ray.get
                # and replay emits in deferred-list order.
                deferred_emits.append(
                    DeferredEmit(
                        task=self,
                        block_ref=self._pending_block_ref,
                        meta_ref=self._pending_meta_ref,
                        meta_bytes=None,
                        local_object_size=size_known_locally,
                    )
                )
                bytes_read += size_known_locally
                self._pending_block_ref = ray.ObjectRef.nil()
                self._pending_meta_ref = ray.ObjectRef.nil()
                continue

            # Size not known locally — estimate it from the operator's
            # running average block size (recorded in `replay_deferred_emits`
            # from exact `meta.size_bytes`) and defer the metadata to the
            # same batched `ray.get`. This keeps a SINGLE metadata-fetch
            # path: no per-ref `ray.get` inside the loop. The estimate only
            # feeds the output-budget arithmetic; the exact metadata still
            # arrives via the batched fetch in replay. Misses are rare
            # (object_size is locally known for ~all pairs), so the estimate
            # is a small, transient budget approximation.
            est_size = _avg_block_size(self._operator_name)
            deferred_emits.append(
                DeferredEmit(
                    task=self,
                    block_ref=self._pending_block_ref,
                    meta_ref=self._pending_meta_ref,
                    meta_bytes=None,
                )
            )
            bytes_read += est_size
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
        return self._has_finished

    def drain_and_emit(self, max_bytes_to_read: Optional[int]) -> int:
        """Convenience: run ``on_data_ready`` + synchronous replay in one
        call. For tests and one-off callers that aren't participating in
        :func:`process_completed_tasks`'s cross-task batching. Production
        code calls :meth:`on_data_ready` directly with a shared deferred
        list, then runs :func:`replay_deferred_emits` once after all
        tasks have been drained — that's where the batched-fetch win
        comes from."""
        deferred: List[DeferredEmit] = []
        bytes_read = self.on_data_ready(max_bytes_to_read, deferred)
        replay_deferred_emits(deferred, [self])
        return bytes_read


def replay_deferred_emits(
    deferred: List[DeferredEmit],
    tasks_for_done_check: List[DataOpTask],
) -> None:
    """Run the batched-fetch + emit + done-callback phase that pairs
    with :meth:`DataOpTask.on_data_ready` in deferred mode.

    1. Issues ONE batched ``ray.get`` covering the deferred entries
       whose ``meta_bytes`` is still ``None`` (the known-size pairs
       that ``on_data_ready`` deliberately didn't fetch). On batched
       failure: per-ref ``ray.get`` retry fallback, then skip any refs
       that still fail (logged).
    2. Replays ``deferred`` in append order — calling each task's
       ``_output_ready_callback`` and updating ``_last_block_meta``.
       Append order matches today's per-op, per-task, per-pair
       traversal because every ``on_data_ready`` invocation appends
       sequentially.
    3. Fires postponed ``task_done_callback`` for any task in
       ``tasks_for_done_check`` whose ``_task_done_pending`` flag is
       set, using its now-final ``_last_block_meta``.
    """
    if deferred:
        # Batched fetch for entries whose bytes weren't already stashed.
        to_fetch = [d.meta_ref for d in deferred if d.meta_bytes is None]
        if to_fetch:
            try:
                fetched = ray.get(to_fetch)
                bytes_by_ref = dict(zip(to_fetch, fetched))
            except Exception:
                logger.warning(
                    "Batched ray.get of %d deferred meta_refs failed; "
                    "falling back to per-ref ray.get for each.",
                    len(to_fetch),
                )
                bytes_by_ref = {}
                for ref in to_fetch:
                    try:
                        bytes_by_ref[ref] = ray.get(ref)
                    except Exception:
                        logger.warning(
                            "Per-ref ray.get retry also failed for meta_ref %s; "
                            "this RefBundle will be skipped.",
                            ref.hex(),
                        )
            for d in deferred:
                if d.meta_bytes is None:
                    d.meta_bytes = bytes_by_ref.get(d.meta_ref)

        # Replay in append order — preserves per-op, per-task,
        # per-pair emission order from on_data_ready.
        for d in deferred:
            if d.meta_bytes is None:
                # The fetch failed for this ref; skip emit.
                continue
            _emit_deferred_entry(d, d.meta_bytes)

    # Fire any postponed ``task_done_callback`` now that
    # ``_last_block_meta`` reflects the deferred-replay state.
    for task in tasks_for_done_check:
        if task._task_done_pending:
            _fire_task_done(task)


def _emit_deferred_entry(d: DeferredEmit, meta_bytes: bytes) -> None:
    """Emit one deferred pair's ``RefBundle`` from its fetched ``meta_bytes``,
    update ``_last_block_meta``, and record the per-op average + probe
    telemetry. Shared by ``replay_deferred_emits`` (synchronous) and the
    background ``MetadataPrefetcher`` (async)."""
    # Lazy import to avoid a physical_operator <-> block.py cycle at load.
    from ray.data.block import BlockMetadataWithSchema

    meta_with_schema: BlockMetadataWithSchema = pickle.loads(meta_bytes)
    meta = meta_with_schema.metadata
    # Feed the per-op running average with the exact size, so future
    # local-size misses can estimate the budget without a ray.get.
    _record_block_size(d.task._operator_name, meta.size_bytes)
    # Telemetry: local object_size hit rate + closeness to exact size.
    _record_local_size_probe(d.local_object_size, meta.size_bytes)
    d.task._output_ready_callback(
        RefBundle(
            [BlockEntry(d.block_ref, meta)],
            owns_blocks=True,
            schema=meta_with_schema.schema,
        ),
    )
    d.task._last_block_meta = meta


def _fire_task_done(task: "DataOpTask") -> None:
    """Fire a task's postponed end-of-stream ``task_done_callback`` using its
    now-final ``_last_block_meta``."""
    task._task_done_callback(
        None,  # exception
        task._last_block_meta.task_exec_stats
        if task._last_block_meta is not None
        else None,
        TaskExecDriverStats(
            task_output_backpressure_s=task._total_output_backpressure_s,
        ),
    )
    task._has_finished = True
    task._task_done_pending = False


class MetadataOpTask(OpTask):
    """Represents an OpTask that only handles metadata, instead of Block data."""

    def __init__(
        self,
        task_index: int,
        object_ref: ray.ObjectRef,
        task_done_callback: Callable[[], None],
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        """
        Args:
            object_ref: The ObjectRef of the task.
            task_done_callback: The callback to call when the task is done.
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
