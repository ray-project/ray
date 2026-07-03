"""ShuffleMapOpV3 — map phase of the v3 file-transport hash shuffle.

Drives the per-mapper ``v3_map_task`` and, on ``all_inputs_done``, emits
N ``RefBundle`` wrappers to its output queue — one per ``partition_id`` —
mirroring the v2 ``ShuffleMapOp`` map->reduce contract (one bundle per
partition, ``__partition__<pid>`` sentinel encoded in metadata).

Two things distinguish v3 from v2 at the map layer:

  * v3 does NOT transpose per-mapper shards into per-partition shard
    tuples. Each mapper writes a single ``.shf`` file containing all N
    partitions' bytes; the ``ShuffleHandle`` returned by the mapper task
    is a small dict (manager ActorHandle + per-partition byte index).
  * The N partition wrappers built here all point at the SAME shared
    plasma object (``ray.put(handle_refs)``), differing only in their
    ``partition_id`` sentinel and ``size_bytes`` hint. Reducer dispatch
    therefore costs 1 nested-ref serialization per task (not M),
    preserving the optimization the reduce op used to perform itself.

The upstream barrier (`_completed_handle_refs` fills as mappers finish;
wrappers emitted at `all_inputs_done`) mirrors v2's
`_maybe_emit_partition_bundles` gating — reducers can't start before map
finishes in either implementation.

Lifecycle: ``start()`` is the natural place to spawn one ``ShuffleManager``
actor per cluster node that map tasks may run on (managers serve their own
node's shuffle files over a per-node socket endpoint). ``_do_shutdown()``
kills those actors and removes the on-disk shuffle directory.
"""

import functools
import logging
import secrets
import tempfile
import typing
from collections import defaultdict
from typing import Any, Dict, List, Optional

import ray
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    ObjectStoreUsage,
    OpTask,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.hash_shuffle_v3 import (
    PartitionFn,
    ShuffleCompression,
    v3_map_task,
)
from ray.data._internal.execution.operators.sub_progress import (
    SubProgressBarMixin,
)
from ray.data.block import BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


# Per-mapper sentinel stamped on the emitted handle bundle's metadata.
# Lets the reducer recover "which mapper produced this handle" if it ever
# needs to (current reducer doesn't, since it just collects every handle
# it sees — but keeping it parallels the v2 ``__partition__N`` convention
# and helps debugging).
_MAPPER_ID_SENTINEL = "__v3_mapper__"


def _make_mapper_sentinel(mapper_id: int) -> List[str]:
    return [f"{_MAPPER_ID_SENTINEL}{mapper_id}"]


def _estimate_handle_plasma_bytes(handle_ref) -> int:
    """Rough plasma footprint of a ShuffleHandle ref.

    The handle is a small dict (manager ActorHandle + token + path +
    per-partition index of (offset, length) tuples); the GB of partition
    data lives on local disk via file-transport, not in plasma. We don't
    need an exact number -- the streaming executor only uses size_bytes
    to gate backpressure, and being within an order of magnitude of
    reality is sufficient. Try Ray's object-locations API for an accurate
    serialized size; fall back to a conservative constant when it's
    unavailable (driver not joined, ref garbage-collected, etc.).
    """
    try:
        from ray.experimental import get_object_locations

        info = get_object_locations([handle_ref]).get(handle_ref)
        if info is not None:
            size = info.get("object_size")
            if isinstance(size, int) and size > 0:
                return size
    except Exception:
        pass
    # Fallback: handles are typically a few KB.
    return 4 * 1024


class ShuffleMapOpV3(InternalQueueOperatorMixin, PhysicalOperator, SubProgressBarMixin):
    """V3 map operator. See module docstring."""

    _DEFAULT_MAP_NUM_CPUS = 1.0
    # Floor for the per-task partition pool budget. Tiny inputs (or inputs
    # with missing metadata, ``estimated_bytes == 0``) still get a usable
    # working set; for normal sizes the dynamic ``_POOL_GROWTH × input``
    # formula always exceeds this.
    _MIN_POOL_BYTES = 4 * 1024 * 1024  # 4 MiB
    # Default = UNBOUNDED pool: accumulate every partition fully and encode it
    # ONCE at end-of-task (exactly like the v2 path in shuffle_tasks.py, which
    # has no pool and doesn't OOM — it bounds memory via the per-task `memory`
    # resource request instead). A small pool spills each partition in tiny
    # increments -> tens of thousands of ~17KB zstd encodes -> the map becomes
    # encode-bound. The pool is now opt-IN (pass `pool_budget_bytes=`) only when
    # a hard per-task memory clamp is actually needed.
    _UNBOUNDED_POOL_BYTES = 1 << 62
    # Multiplier on the per-task input size when sizing the partition pool.
    # 2× gives headroom over the naive "output ≈ input" identity (chunked
    # output, compression overhead pre-encode, partition skew). Higher
    # numbers don't help — once pool > sum(output_bytes), the extra capacity
    # is never filled. Lower numbers force mid-task spills proportional to
    # ``input / pool``, increasing IPC fragmentation on the reducer side.
    _POOL_GROWTH = 2

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        partition_fn: PartitionFn,
        compression: ShuffleCompression = None,
        pool_budget_bytes: Optional[int] = None,
        fsync_on_close: bool = True,
        map_cpus: float = _DEFAULT_MAP_NUM_CPUS,
        base_dir: Optional[str] = None,
        name: str = "ShuffleMapV3",
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._partition_fn: PartitionFn = partition_fn
        self._compression: ShuffleCompression = compression
        # Fixed override of the per-task partition pool budget. ``None`` ⇒
        # dynamic ``max(_MIN_POOL_BYTES, _POOL_GROWTH × estimated_bytes)``
        # per task; an explicit int ⇒ use that value for every task
        # regardless of input size (useful in tests and for capping under
        # memory pressure).
        self._pool_budget_override: Optional[int] = pool_budget_bytes
        self._fsync_on_close: bool = fsync_on_close

        # -- Map task config --
        self._map_num_cpus: float = map_cpus

        # -- On-disk staging --
        # ``base_dir`` is just a directory-name template — each node mkdirs
        # the same path on its OWN local FS. Driver doesn't own anything on
        # remote disks. Cleanup is performed by the ``ShuffleManager`` actor
        # itself: an ``atexit`` hook in the actor process ``rmtree``s
        # ``base_dir`` on graceful actor termination (ref-count → 0).
        # SIGKILL paths (OOM, ``ray.kill``, crash) skip atexit, leaving the
        # files on disk for a ``max_restarts`` respawn — that's the right
        # property for fault tolerance.
        #
        # Caller-supplied ``base_dir`` is treated as scratch space: it WILL
        # be removed when the shuffle's actors are released. Callers that
        # want their directory preserved should not pass a path they expect
        # to keep.
        self._base_dir: str = base_dir or tempfile.mkdtemp(prefix="ray_shuffle_v3_")
        # Per-shuffle auth token; ShuffleManager rejects requests with any
        # other token. Cheap defense against accidental cross-shuffle reads
        # by misrouted reducers in a shared cluster.
        self._token: str = secrets.token_hex(16)
        # Stable per-op id used as the named-actor suffix for the
        # ShuffleManager on each node. Mappers do ``get_if_exists=True`` so
        # the FIRST mapper on a node spawns; the rest share the same actor.
        self._shuffle_id: str = secrets.token_hex(8)

        # -- Map task tracking --
        self._next_map_idx: int = 0
        self._shuffle_map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Output queue --
        # Populated at ``all_inputs_done`` time with N partition-wrapper
        # bundles (one per partition_id), each carrying the SAME
        # ``_shared_handles_ref`` + a distinct partition_id sentinel in
        # metadata. This mirrors v2's map->reduce contract (one bundle per
        # partition), giving the executor N gating points for backpressure
        # — instead of the reduce op firing all N reducers at once.
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()

        # -- Post-map accumulation (used to build the N wrappers) --
        # Handles returned by completed mappers (one per mapper task).
        # Held on the driver until ``_maybe_emit_partition_wrappers`` runs
        # (fires only after both ``all_inputs_done`` has been called AND
        # every mapper task has completed).
        self._completed_handle_refs: List[ObjectRef] = []
        # Original mapper-input RefBundles, pinned so the handle ObjectRefs
        # inside them stay alive until the op shuts down.
        self._completed_input_bundles: List[RefBundle] = []
        # Single plasma object holding the full handle-ref list, shared by
        # every partition wrapper's block-ref slot. Ray dispatch cost per
        # reducer .remote() = 1 nested ref (not M), preserving the
        # optimization the reduce op used to make. Held here so its
        # lifetime spans the whole shuffle; released in ``_do_shutdown``.
        self._shared_handles_ref: Optional[ObjectRef] = None
        # One-shot guard so wrappers are emitted exactly once even though
        # ``_maybe_emit_partition_wrappers`` is called from both the
        # ``all_inputs_done`` override and each ``_handle_map_done`` (either
        # can be the last-to-fire event).
        self._wrappers_emitted: bool = False

        # -- Stats --
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []
        # Per-partition decoded (pa.Table.nbytes, pre-compression) byte total,
        # summed across all completed mappers. Sized for the reducer's
        # per-task memory ask, mirroring v2's _partition_bytes path
        # (shuffle_map_operator.py:134/303 + get_partition_bytes()).
        self._partition_decoded_bytes: Dict[int, int] = defaultdict(int)

        # -- Sub-progress bar --
        self._map_bar: Optional["BaseProgressBar"] = None

    # Queue plumbing
    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return []

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_queue]

    def _pick_target_node(self, refs: RefBundle) -> Optional[str]:
        """Choose a target node hint for the map task, preferring input-block
        locality. Returns None when there's no locality info — caller then
        skips NodeAffinity and lets Ray schedule on any available node
        (which then spawns its own local ShuffleManager via get_if_exists).
        """
        prefer_locs = refs.get_preferred_object_locations()
        if prefer_locs:
            return max(prefer_locs, key=lambda n: prefer_locs[n])
        return None

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return
        node_id = self._pick_target_node(refs)
        self._submit_shuffle_map_task(refs, target_node_id=node_id)

    def _submit_shuffle_map_task(
        self,
        input_bundle: RefBundle,
        *,
        target_node_id: Optional[str],
    ) -> None:
        """Submit one v3_map_task. Endpoint resolution happens INSIDE the
        task on whatever node Ray ends up running it on — driver no longer
        pre-resolves. ``target_node_id`` is just a locality hint.
        """
        map_id = self._next_map_idx
        self._next_map_idx += 1

        estimated_bytes = sum((m.size_bytes or 0) for m in input_bundle.metadata)

        # Per-task pool budget: explicit override wins; otherwise UNBOUNDED
        # (accumulate every partition fully, encode once at end-of-task — the
        # v2 behavior). The old dynamic ``max(4MB, 2×estimated_bytes)`` formula
        # collapsed to the 4MB floor for fused-read maps (estimated_bytes≈0,
        # since the input bundle is ListFiles metadata, not the data read
        # inside the task) → ~17KB shards → ~46k tiny zstd encodes → map became
        # encode-bound (80s/task). Default to no mid-task flushing.
        if self._pool_budget_override is not None:
            pool_budget_bytes = self._pool_budget_override
        else:
            pool_budget_bytes = self._UNBOUNDED_POOL_BYTES

        # Memory ask: peak working set ≈ input resident + full partition output
        # ≈ 2× input (the v2 SHUFFLE_PEAK_MEMORY_MULTIPLIER). Sized from the
        # input estimate, NOT the pool budget (the default pool is unbounded;
        # adding it would request 2^62 bytes and never schedule).
        resources: Dict[str, Any] = {"num_cpus": self._map_num_cpus}
        if estimated_bytes > 0:
            resources["memory"] = estimated_bytes * 2

        ray_options: Dict[str, Any] = dict(resources)
        if target_node_id is not None:
            # soft=True lets Ray reschedule on retry (worker death) without
            # the original node being available — the retried task spawns a
            # fresh local manager on its new node and a fresh handle (with
            # the new manager) replaces the old ObjectRef value transparently.
            ray_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                target_node_id, soft=True
            )

        handle_ref = v3_map_task.options(**ray_options).remote(
            *input_bundle.block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            out_dir=self._base_dir,
            map_id=map_id,
            shuffle_id=self._shuffle_id,
            token=self._token,
            map_op_name=self.name,
            pool_budget_bytes=pool_budget_bytes,
            compression=self._compression,
            fsync_on_close=self._fsync_on_close,
        )

        task = MetadataOpTask(
            task_index=map_id,
            object_ref=handle_ref,
            task_done_callback=functools.partial(
                self._handle_map_done, map_id, handle_ref, input_bundle
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(resources),
        )
        self._shuffle_map_tasks[map_id] = task
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.add(requested)

        all_blocks_meta = tuple(
            BlockEntry(ref=ref, metadata=meta)
            for ref, meta in zip(input_bundle.block_refs, input_bundle.metadata)
        )
        self._metrics.on_task_submitted(
            map_id,
            RefBundle(all_blocks_meta, schema=None, owns_blocks=False),
            task_id=task.get_task_id(),
        )

        if self._map_bar is not None:
            _, _, num_rows = estimate_total_num_of_blocks(
                map_id + 1,
                self.upstream_op_num_outputs(),
                self._metrics,
                total_num_tasks=None,
            )
            self._map_bar.update(total=num_rows)

    def _handle_map_done(
        self,
        map_id: int,
        handle_ref: "ObjectRef",
        input_bundle: RefBundle,
    ) -> None:
        """``MetadataOpTask`` callback: handle is materialized.

        We don't need to actually deserialize the handle on the driver (the
        reducer can do that), but we do want to record stats from the
        on-disk file size and free input bundles. We emit a 1-block bundle
        whose ref IS the handle ref, schema=None (handles aren't tables).
        """
        task = self._shuffle_map_tasks.pop(map_id)
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.subtract(requested)

        # Fold this mapper's per-partition decoded bytes into the op-level
        # accumulator that ShuffleReduceOpV3 reads via ``get_partition_bytes``.
        # The handle is local plasma here (the task just completed), so
        # ``ray.get`` is a μs-level local read; best-effort, never break the
        # pipeline if the field is absent (older mappers won't carry it).
        try:
            handle = ray.get(handle_ref)
            for pid, nbytes in (handle.get("decoded_bytes") or {}).items():
                self._partition_decoded_bytes[pid] += nbytes
        except Exception:
            # Best-effort (metrics only), but silence would hide a category
            # of failures that empty this accumulator and collapse every
            # reducer's memory ask to zero -- previously observed as 30×6GB
            # reducers OOMing a node because an UnboundLocalError in the
            # ``ray.get(handle_ref)`` call above was swallowed by a bare
            # ``pass``. Log with full traceback so future breakage is loud.
            logger.exception(
                "ShuffleMapOpV3: failed to fold decoded_bytes for "
                "map_id=%s; reducer memory ask will be under-counted",
                map_id,
            )

        # OpRuntimeMetrics.on_task_output_generated asserts every output block
        # carries exec_stats with wall_time_s AND block_ser_time_s set. The
        # handle isn't a real computed block, so we attach a minimal,
        # already-populated stats object (builder sets wall_time_s; we set the
        # serialization time to 0.0 so the assertion holds).
        #
        # NOTE: do NOT ``import ray`` locally in this function -- module-level
        # ``import ray`` (top of file) already provides it. A function-local
        # ``import ray`` (bare, not ``from ray.X import Y``) would make ``ray``
        # a local for this whole method under Python scoping rules, turning
        # the ``ray.get(handle_ref)`` above into UnboundLocalError before the
        # local binding is assigned. The A/B on a 50GB/50-partition run: with
        # the local import, n_nonzero(decoded_bytes) = 0 and reducer ask = 0;
        # without it, n_nonzero = 50, total = 53.9 GB, reducer ask ≈ 2.15 GB.
        # ``from ray.data.block import BlockExecStats`` below is fine -- it
        # only binds ``BlockExecStats`` locally, not ``ray``.
        from ray.data.block import BlockExecStats

        # BlockExecStats is a frozen dataclass — pass block_ser_time_s through
        # build(**kwargs) (builder() populates wall_time_s itself).
        exec_stats = BlockExecStats.builder().build(block_ser_time_s=0.0)

        # size_bytes must reflect the bundle's *plasma footprint*, not the
        # on-disk shuffle output. The plasma object is the ShuffleHandle dict
        # (manager handle + token + path + per-partition index), typically a
        # few KB. The GB of partition data lives on local disk on the map
        # node via file-transport -- never in the object store. Reporting
        # handle["total_bytes"] here would over-account plasma usage by
        # orders of magnitude and trigger spurious backpressure / spill
        # decisions in ResourcePoolManager.
        size_bytes = _estimate_handle_plasma_bytes(handle_ref)

        out_meta = BlockMetadata(
            # The handle isn't itself a Block; it contributes 0 rows to the
            # row-count metric (the real rows surface at the reduce output).
            # Must be an int, not None: OpRuntimeMetrics.on_task_output_generated
            # does ``rows += output.num_rows()`` and would TypeError on None.
            num_rows=0,
            size_bytes=size_bytes,
            exec_stats=exec_stats,
            input_files=_make_mapper_sentinel(map_id),
        )
        # Synthetic per-mapper output bundle — used ONLY for per-task
        # metric bookkeeping (submitted -> output_generated -> finished).
        # It is NOT pushed to the output queue; downstream sees the N
        # partition wrappers built in ``all_inputs_done`` instead.
        out_bundle = RefBundle(
            (BlockEntry(ref=handle_ref, metadata=out_meta),),
            schema=None,
            owns_blocks=False,  # shared_handles_ref pins the underlying ref
        )

        # Accumulate for the partition wrappers built in ``all_inputs_done``.
        # We pin ``input_bundle`` (rather than destroy it here) so the
        # handle ObjectRef inside stays alive — ``ray.put(handle_refs)``
        # later takes a snapshot, but the underlying refs still need an
        # explicit owner. Released in ``_do_shutdown``.
        self._completed_handle_refs.append(handle_ref)
        self._completed_input_bundles.append(input_bundle)

        # Roll up stats from input metadata.
        input_rows = sum((m.num_rows or 0) for m in input_bundle.metadata)
        input_bytes = sum((m.size_bytes or 0) for m in input_bundle.metadata)
        self._total_input_rows += input_rows
        self._total_input_bytes += input_bytes
        # No exec_stats here (we'd need v3_map_task to surface them); MVP
        # leaves block stats minimal.

        # Order matters: on_task_output_generated looks up the task in
        # _running_tasks, but on_task_finished POPS it from that dict. So emit
        # the output-generated event FIRST, then mark the task finished
        # (submitted -> output_generated -> finished).
        self._metrics.on_task_output_generated(task_index=map_id, output=out_bundle)
        self._metrics.on_task_finished(
            map_id, None, task_exec_stats=None, task_exec_driver_stats=None
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_rows)

        # This map task may be the last-to-finish event completing the
        # (all_inputs_done + no map tasks in flight) gate — try to emit.
        self._maybe_emit_partition_wrappers()

    def all_inputs_done(self) -> None:
        """Upstream (feeder of mapper input bundles) has closed. Wrappers
        can only be emitted once *both* this has fired AND every in-flight
        map task has completed, so we just try; the actual emission is
        gated in ``_maybe_emit_partition_wrappers``.
        """
        super().all_inputs_done()
        self._maybe_emit_partition_wrappers()

    def _maybe_emit_partition_wrappers(self) -> None:
        """Emit the N partition-wrapper bundles into ``_output_queue`` —
        one per partition_id, each carrying the SAME shared handle-list
        plasma ref plus a distinct ``__partition__<pid>`` sentinel in
        metadata.

        Called from two places (either can be the last-to-fire event):
          * ``all_inputs_done``: upstream closes with all map tasks
            already finished.
          * ``_handle_map_done``: a map task finishes after
            ``all_inputs_done`` already fired.

        Gated on: ``_inputs_complete`` AND no map tasks in flight AND
        wrappers not already emitted.

        This gives the executor N distinct bundle-arrivals to backpressure
        the downstream ``ShuffleReduceOpV3`` against — the reducer sees
        one bundle per partition and dispatches one reducer per bundle,
        exactly like the v2 ``ShuffleReduceOp`` does. The N wrappers do
        not own the shared ref (``owns_blocks=False``); the ref's lifetime
        is bound to ``self._shared_handles_ref`` for the entire op.
        """
        if self._wrappers_emitted:
            return
        if not self._inputs_complete:
            return
        if self._shuffle_map_tasks:
            return
        self._wrappers_emitted = True

        if not self._completed_handle_refs:
            # No mapper produced a handle (empty upstream); nothing to emit.
            return

        # Local import: v2 map operator's sentinel encoding. Sharing it
        # keeps the reduce side reusable across V2 and V3 (both call
        # ``extract_partition_id`` on the same sentinel format).
        from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
            make_partition_sentinel,
        )
        from ray.data.block import BlockExecStats

        # ONE plasma object holds the list of M handle refs; every wrapper's
        # single block slot points at it. Ray sees 1 borrowed ref per
        # reducer .remote() dispatch (not M), preserving the dispatch-cost
        # optimization that the reduce op used to make internally.
        self._shared_handles_ref = ray.put(self._completed_handle_refs)

        partition_bytes = self.get_partition_bytes()
        for partition_id in range(self._num_partitions):
            # size_bytes hint drives the reducer's memory ask via the
            # wrapper's metadata (reduce reads it in _add_input_inner). If
            # the accumulator hasn't recorded this partition (retry / empty
            # partition / older mapper), the hint is 0 → Ray defaults.
            size_bytes = partition_bytes.get(partition_id, 0)
            # exec_stats must be present (framework asserts wall_time_s +
            # block_ser_time_s are set for every emitted block metadata);
            # the wrapper isn't a "computed" block, so we attach a minimal
            # already-populated stats object.
            exec_stats = BlockExecStats.builder().build(block_ser_time_s=0.0)
            wrapper_meta = BlockMetadata(
                # Wrapper doesn't itself carry rows (real rows surface at
                # reduce output). Must be int-typed for metric arithmetic.
                num_rows=0,
                size_bytes=size_bytes,
                exec_stats=exec_stats,
                input_files=list(make_partition_sentinel(partition_id)),
            )
            wrapper = RefBundle(
                (
                    BlockEntry(
                        ref=self._shared_handles_ref, metadata=wrapper_meta
                    ),
                ),
                schema=None,
                owns_blocks=False,
            )
            self._output_queue.add(wrapper)
            self._metrics.on_output_queued(wrapper)

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_map_tasks.values())

    def get_partition_bytes(self) -> Dict[int, int]:
        """Per-partition decoded byte totals summed across completed mappers.

        Consumed by ``ShuffleReduceOpV3`` to size each reducer's memory ask.
        Mirrors ``ShuffleMapOp.get_partition_bytes`` (v2). Returns a snapshot
        copy; the underlying counter keeps growing as more mappers finish.
        """
        return dict(self._partition_decoded_bytes)

    def has_execution_finished(self) -> bool:
        if self._shuffle_map_tasks or self._output_queue.has_next():
            return False
        # Between "last mapper task finished" and "_maybe_emit_partition_wrappers
        # fires" both queues can be transiently empty even though we haven't
        # produced any output yet. Refuse to declare finished until the
        # emission gate has actually fired (or upstream never opened).
        if self._inputs_complete and not self._wrappers_emitted:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._output_queue.has_next()
            and (not self._inputs_complete or self._wrappers_emitted)
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._output_queue.clear()
        # Release pinned mapper-input bundles — their sole purpose was to
        # keep the handle ObjectRefs alive across dispatch. Once reducers
        # have finished (this method runs at op shutdown), Ray-core
        # ref-counting is safe to reclaim.
        for bundle in self._completed_input_bundles:
            bundle.destroy_if_owned()
        self._completed_input_bundles.clear()
        self._completed_handle_refs.clear()
        # Drop the shared handle-list plasma object. Reducer tasks that
        # captured a borrowed ref have already completed by shutdown time.
        self._shared_handles_ref = None
        # ShuffleManager actors are kept alive by the ActorHandle inside
        # each emitted ShuffleHandle; Ray ref-counting tears them down once
        # all reducer bundles release. No cleanup needed here.

    # Stats / progress
    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._map_resource_usage.cpu,
            memory=self._map_resource_usage.memory,
        )

    def estimate_object_store_usage(self, state) -> ObjectStoreUsage:
        # Bulk shuffle data lives on disk, not in Plasma. Handles are
        # KB-sized dicts, so their object-store footprint is negligible.
        return ObjectStoreUsage(internal=0, outputs=0)

    def incremental_resource_usage(self) -> ExecutionResources:
        # Mirror v2's heuristic: 2× per-task input-bytes memory hint.
        avg_input = self._metrics.average_bytes_inputs_per_task
        memory = int(avg_input * 2) if avg_input else 0
        return ExecutionResources(cpu=self._map_num_cpus, memory=memory)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        maps_done = self._next_map_idx - len(self._shuffle_map_tasks)
        return f"map: {maps_done}/{self._next_map_idx}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Map"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Map":
            self._map_bar = pg

    # V3-specific accessors for data stored on disk
    @property
    def num_partitions(self) -> int:
        return self._num_partitions

    @property
    def base_dir(self) -> str:
        return self._base_dir

    @property
    def token(self) -> str:
        return self._token

    @property
    def shuffle_id(self) -> str:
        return self._shuffle_id
