"""ShuffleMapOpV3 — map phase of the v3 file-transport hash shuffle.

Drives the per-mapper ``v3_map_task`` and emits ONE output ``RefBundle`` per
completed mapper, each carrying the returned ``ShuffleHandle`` ref. Unlike
the v2 ``ShuffleMapOp``, this op does NOT transpose per-mapper output into
per-partition bundles: the v3 ``ShuffleHandle`` already contains the
per-partition byte index, and the v3 reducer reads its partition's bytes
directly from the source node's ``ShuffleManager`` via the file-transport
side-channel. Net effect:

  * No ``_partition_staging[pid]`` FIFO queues on the driver.
  * Each completed map task contributes ONE handle bundle.
  * The downstream ``ShuffleReduceOpV3`` collects all handle bundles and,
    once the map phase finishes, dispatches ``num_partitions`` reduce
    tasks each given the full handle list + a ``partition_id``.

Lifecycle: ``start()`` is the natural place to spawn one ``ShuffleManager``
actor per cluster node that map tasks may run on (managers serve their own
node's shuffle files over a per-node socket endpoint). ``_do_shutdown()``
kills those actors and removes the on-disk shuffle directory.

This is the operator-layer skeleton for the MVP "ds.repartition() goes
through v3" path; planner glue and join/multi-seq are follow-up work.
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
    from ray.data._internal.execution.operators.map_transformer import (
        MapTransformer,
    )
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
        upstream_map_transformer: Optional["MapTransformer"] = None,
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

        # When set, OperatorFusionRule has absorbed an upstream
        # TaskPoolMapOperator (typically the V2 parquet read, optionally
        # with map/filter chained in front) into this op. v3_map_task
        # applies it to incoming blocks via apply_transform before
        # partitioning, so the read/map chain runs inline in the shuffle
        # map ray task with no plasma round-trip for the intermediate
        # blocks. None means standard non-fused dispatch.
        self._upstream_map_transformer: Optional[
            "MapTransformer"
        ] = upstream_map_transformer

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
        # Each item is a 1-block RefBundle whose ref points at the
        # ShuffleHandle dict returned by ``v3_map_task``.
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()

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

    def supports_fusion(self) -> bool:
        return True

    def absorbs_upstream_map_transformer(self) -> bool:
        return True

    def fuse_with_upstream_map_transformer(
        self, upstream_map_transformer
    ) -> "ShuffleMapOpV3":
        """Return a new ShuffleMapOpV3 that runs upstream_map_transformer
        inline before partitioning, taking the absorbed upstream's input
        as its own input. Composes with any previously-absorbed upstream
        transformer (so chained Read -> Map -> ... -> ShuffleMapV3
        collapses in fusion-rule waves into one op carrying the full
        transform chain).
        """
        up_op = self.input_dependencies[0]
        existing = self._upstream_map_transformer
        if existing is not None:
            combined = upstream_map_transformer.fuse(existing)
        else:
            combined = upstream_map_transformer
        return ShuffleMapOpV3(
            input_op=up_op.input_dependencies[0],
            data_context=self.data_context,
            num_partitions=self._num_partitions,
            partition_fn=self._partition_fn,
            compression=self._compression,
            pool_budget_bytes=self._pool_budget_override,
            fsync_on_close=self._fsync_on_close,
            map_cpus=self._map_num_cpus,
            base_dir=self._base_dir,
            name=f"{up_op.name}->{self.name.split('->')[-1]}",
            upstream_map_transformer=combined,
        )

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
        #
        # KNOWN HAZARD: with upstream fusion (this op absorbs a
        # ReadFilesParquetV2 etc.), the input bundle is the ListFiles output
        # -- a manifest of file paths, a few KB -- not the GBs the fused
        # read will actually pull inside the task. ``estimated_bytes`` then
        # ≈ 0 and no memory ask is emitted, so the ResourceManager packs
        # concurrent map tasks by CPU alone and can OOM the node (observed:
        # 8 fused maps × ~2.4 GB actual working set on a 30 GB node). v2
        # does NOT have this bug because its ShuffleMapOp does not absorb
        # upstream map transformers -- the read stays a separate op whose
        # output bundle carries the real ~1 GB estimate. TODO: derive the
        # working-set estimate from the manifest's file-size column (or
        # apply a target_max_block_size-derived floor) when
        # ``_upstream_map_transformer is not None``.
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
            upstream_map_transformer=self._upstream_map_transformer,
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
        out_bundle = RefBundle(
            (BlockEntry(ref=handle_ref, metadata=out_meta),),
            schema=None,
            owns_blocks=True,
        )
        self._output_queue.add(out_bundle)
        self._metrics.on_output_queued(out_bundle)

        # Free input bundles (they've been consumed by the map task).
        input_bundle.destroy_if_owned()

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
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._output_queue.has_next()
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._output_queue.clear()
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
