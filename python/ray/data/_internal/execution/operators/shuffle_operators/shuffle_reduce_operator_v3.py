"""ShuffleReduceOpV3 — reduce phase of the v3 file-transport hash shuffle.

Consumes the per-mapper ``ShuffleHandle`` bundles emitted by
``ShuffleMapOpV3`` and, once the map phase is closed, dispatches one
``v3_reduce_task`` per partition. Each reduce task is a Ray streaming
generator that yields ``(block, pickled metadata)`` pairs matching the v2
``_shuffle_reduce_task`` protocol; we wrap each generator in a
``DataOpTask`` so the executor sees ordinary streaming output bundles.

Key state-machine difference vs. v2:

  * v2 ``ShuffleReduceOp`` receives one bundle PER PARTITION (each
    containing M shard refs) and launches a reduce task immediately on
    arrival.
  * v3 ``ShuffleReduceOpV3`` receives one bundle PER MAPPER (each
    containing a single ``ShuffleHandle`` ref). It must wait until ALL
    mapper handles are in (``all_inputs_done``) before launching the N
    reducers, because every reducer needs the full handle list to pull
    its partition's bytes from every source node's ``ShuffleManager``.

This mirrors v2's effective behavior — v2's ShuffleMapOp also gates
``_maybe_emit_partition_bundles`` on ``_inputs_complete``, so reduce can't
start before map finishes there either; we just push the gathering
responsibility one operator downstream.
"""

import functools
import logging
import typing
from collections import deque
from typing import Any, Dict, List, Optional

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    OpTask,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.hash_shuffle_v3 import (
    ReduceFn,
    v3_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_v3 import (  # noqa: E501
    ShuffleMapOpV3,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.sub_progress import (
    SubProgressBarMixin,
)
from ray.data.block import BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext
from ray.types import ObjectRef

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import (
        MapTransformer,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ShuffleReduceOpV3(PhysicalOperator, SubProgressBarMixin):

    _DEFAULT_REDUCE_NUM_CPUS = 1.0
    _DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB

    def __init__(
        self,
        input_op: ShuffleMapOpV3,
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        streaming_reduce: bool = True,
        coalesce_output: bool = False,
        max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
        reduce_prefetch_dir: Optional[str] = None,
        reduce_cpus: Optional[float] = None,
        name: str = "ShuffleReduceV3",
        downstream_map_transformer: Optional["MapTransformer"] = None,
        downstream_map_task_kwargs: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        # When set, OperatorFusionRule has absorbed a downstream MapOperator
        # (typically Write) into this reduce op. v3_reduce_task applies it
        # to each emitted block before yielding, so the downstream task
        # never has to run -- output of this op IS the downstream's output
        # (e.g., the write-stats blocks Write would have produced).
        self._downstream_map_transformer: Optional[
            "MapTransformer"
        ] = downstream_map_transformer
        # map_task_kwargs the absorbed downstream MapOperator would have
        # received via its scheduler (e.g. Write's ``{"write_uuid": ...}``).
        # v3_reduce_task threads this into the TaskContext it builds around
        # the transformer so datasinks that read ``ctx.kwargs[...]`` see
        # the same values they would in the un-fused path.
        self._downstream_map_task_kwargs: Dict[str, Any] = (
            downstream_map_task_kwargs or {}
        )
        self._coalesce_output: bool = coalesce_output
        self._streaming_reduce: bool = streaming_reduce
        self._max_bytes_per_fetch: int = max_bytes_per_fetch
        self._reduce_prefetch_dir: Optional[str] = reduce_prefetch_dir
        self._reduce_num_cpus: float = (
            reduce_cpus if reduce_cpus is not None else self._DEFAULT_REDUCE_NUM_CPUS
        )

        # -- Handle accumulation --
        # Refs we've received from upstream (the ShuffleMapOpV3). One ref
        # per mapper. We must wait for the upstream to close before
        # dispatching reducers, because each reducer needs ALL of them.
        self._handle_refs: List[ObjectRef] = []
        # Keep the input bundles alive so the handle refs aren't dropped
        # mid-flight; destroyed on shutdown / completion.
        self._handle_input_bundles: List[RefBundle] = []
        # Single plasma object holding the full handle-ref list, shared by all
        # reducers (see _dispatch_all_reducers). Kept alive until shutdown.
        self._shared_handles_ref: Optional[ObjectRef] = None

        # -- Reduce task tracking --
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0
        self._reducers_dispatched: bool = False

        # -- Output queue --
        # Streaming generator yields (block, pickled metadata) pairs; the
        # DataOpTask harness assembles each into a RefBundle and our
        # callback pushes it here.
        self._output_queue: deque = deque()

        # -- Stats --
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bar --
        self._reduce_bar: Optional["BaseProgressBar"] = None

    def supports_fusion(self) -> bool:
        return True

    # NOTE: ``absorbs_downstream_map_transformer`` /
    # ``fuse_with_downstream_map_transformer`` were removed when the generic
    # emitter pass was retired in favor of upstream's dedicated pass
    # ``_fuse_map_into_shuffle_reduce_in_dag`` (operator_fusion.py). That
    # pass now type-branches on ``(ShuffleReduceOp | ShuffleReduceOpV3)``
    # and constructs the fused replacement itself, so V3 no longer needs
    # op-side capability methods. ``_downstream_map_transformer`` and
    # ``_downstream_map_task_kwargs`` ctor params + fields stay because
    # v3_reduce_task consumes them and the fusion pass populates them.

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Each upstream bundle is one mapper's ShuffleHandle ref. Just
        accumulate; the reducer dispatch happens once map closes."""
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return
        # Expect exactly one block per mapper bundle (the handle dict).
        for ref in refs.block_refs:
            self._handle_refs.append(ref)
        self._handle_input_bundles.append(refs)

    def all_inputs_done(self) -> None:
        """Upstream map has finished — dispatch one reduce task per
        partition. Each task gets the FULL handle list and a specific
        ``partition_id``; the v3 reduce code uses the handles' index dict
        to find which (offset, length) ranges to fetch for that partition.
        """
        super().all_inputs_done()
        self._dispatch_all_reducers()

    def _dispatch_all_reducers(self) -> None:
        if self._reducers_dispatched:
            return
        self._reducers_dispatched = True

        if not self._handle_refs:
            # No mapper produced any handle, thus nothing to reduce.
            return

        target_max_block_size = self.data_context.target_max_block_size

        # Bundle the full handle-ref list into ONE plasma object and pass that
        # single ref to every reducer, instead of passing all M handle ObjectRefs
        # inline to each of the P reduce tasks. Passing M nested refs per task
        # made Ray register/serialize M borrowed refs on every ``.remote()``
        # (~17ms/task in-situ, ~99% of dispatch -> the P=500 reduce ramp). With a
        # single top-level ref, Ray auto-dereferences it on the worker back to the
        # same handle list (the existing ``for h in handles: ray.get(h)`` loop in
        # ``v3_reduce_task`` is unchanged). Held as a member so the plasma object
        # outlives dispatch; released in ``_do_shutdown``. The inner map-output
        # refs stay pinned by ``_handle_input_bundles`` as before.
        self._shared_handles_ref = ray.put(self._handle_refs)

        for partition_id in range(self._num_partitions):
            self._dispatch_one_reducer(partition_id, target_max_block_size)

    def _dispatch_one_reducer(
        self, partition_id: int, target_max_block_size: Optional[int]
    ) -> None:
        # Per-partition memory ask: 2× the decoded byte total this reducer
        # will see (bounds peak heap from accum + reshape carry). Same source
        # as v2's path -- see ShuffleMapOpV3.get_partition_bytes / the
        # mapper task's ``decoded_bytes`` field. If the upstream hasn't
        # populated bytes yet (e.g. retry on a partition with no bytes
        # recorded), fall back to leaving the hint unset so Ray's defaults
        # apply.
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOpV3)
        estimated_bytes = upstream.get_partition_bytes().get(partition_id, 0)
        reduce_resources: Dict[str, Any] = {"num_cpus": self._reduce_num_cpus}
        if estimated_bytes > 0:
            reduce_resources["memory"] = int(
                estimated_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER
            )
        reduce_options: Dict[str, Any] = {
            **reduce_resources,
            "scheduling_strategy": "SPREAD",
            "num_returns": "streaming",
        }

        block_gen = v3_reduce_task.options(**reduce_options).remote(
            self._shared_handles_ref,
            partition_id,
            self._reduce_fn,
            self._reduce_prefetch_dir,
            self._max_bytes_per_fetch,
            target_max_block_size,
            self._streaming_reduce,
            self._downstream_map_transformer,
            self.name,
            self._downstream_map_task_kwargs,
            self._coalesce_output,
        )

        data_task = DataOpTask(
            task_index=partition_id,
            streaming_gen=block_gen,
            output_ready_callback=functools.partial(
                self._handle_reduce_output_ready, partition_id
            ),
            task_done_callback=functools.partial(
                self._handle_reduce_done, partition_id
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                reduce_resources
            ),
            operator_name=self.name,
        )

        # ShuffleMapOpV3 emits one bundle per mapper, NOT per partition, so
        # we always start with no prior task for this partition_id.
        assert partition_id not in self._shuffle_reduce_tasks, (
            f"partition_id {partition_id} already has an in-flight reducer "
            f"— ShuffleReduceOpV3 should dispatch each partition exactly once"
        )
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        # For metrics, treat the handle list as this reducer's "input."
        # We don't have a partition-specific RefBundle (handles cover every
        # partition), so we just pass a synthetic empty bundle.
        self._metrics.on_task_submitted(
            partition_id,
            RefBundle((), schema=None, owns_blocks=False),
            task_id=data_task.get_task_id(),
        )

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
        """Callback for each yielded (block, metadata) pair from a reducer."""
        self._output_queue.append(bundle)
        self._metrics.on_output_queued(bundle)
        self._metrics.on_task_output_generated(task_index=partition_id, output=bundle)
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            self._num_reduce_tasks_submitted,
            self.upstream_op_num_outputs(),
            self._metrics,
            total_num_tasks=self._num_partitions,
        )
        self._estimated_num_output_bundles = num_outputs
        self._estimated_output_num_rows = num_rows
        if self._reduce_bar is not None:
            self._reduce_bar.update(
                increment=bundle.num_rows() or 0,
                total=self.num_output_rows_total(),
            )

    def _handle_reduce_done(
        self,
        partition_id: int,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        """Callback when a reduce streaming generator finishes."""
        self._shuffle_reduce_tasks.pop(partition_id, None)
        self._metrics.on_task_finished(
            task_index=partition_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if exc:
            logger.error(
                "Reduce of partition %d failed: %s",
                partition_id,
                exc,
                exc_info=exc,
            )

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._metrics.on_output_dequeued(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_reduce_tasks.values())

    def throttling_disabled(self) -> bool:
        # Opt out of the ResourceManager reservation UNCONDITIONALLY. Two effects:
        #   (1) while the map phase runs, reduce (not yet active) stops reserving
        #       a budget slice, so map reaches full CPU concurrency sooner;
        #   (2) once reduce is running it isn't admission-throttled either, so all
        #       partitions run at full concurrency.
        # Reduce is the terminal op (Write is fused in), so there's nothing
        # downstream to reserve for. This intentionally supersedes the earlier
        # ``return not self._reducers_dispatched`` (which only opted out while
        # waiting for map) to also un-throttle reduce execution itself.
        #
        # TODO(shuffle_v3): this removes the memory safety valve during reduce --
        # reduce tasks request only num_cpus (no memory estimate), so throttling
        # was the only backpressure. Unbounded reduce has OOM'd at 512GB before.
        # Validate against OOM and gate this properly (e.g. revert to opting out
        # only while waiting for map, and/or attach a real per-task memory
        # estimate so the ResourceManager can back-pressure instead).
        return True

    def has_execution_finished(self) -> bool:
        if self._shuffle_reduce_tasks or self._output_queue:
            return False
        # We also need the reducers to have been DISPATCHED — otherwise we
        # might "finish" before all_inputs_done has fired (e.g. an empty
        # upstream).
        if not self._reducers_dispatched and not self._inputs_complete:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_reduce_tasks
            and not self._output_queue
            and self._reducers_dispatched
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()
        # Destroying the input bundles drops our hold on each ShuffleHandle
        # ObjectRef. Each handle dict carries the source node's
        # ShuffleManager ActorHandle, so as the ObjectRefs are freed Ray
        # ref-counting transitively releases the manager actors — index
        # lifetime drives file lifetime, no explicit release RPC needed.
        for bundle in self._handle_input_bundles:
            bundle.destroy_if_owned()
        self._handle_input_bundles.clear()
        self._handle_refs.clear()
        # Drop our hold on the shared handle-list plasma object.
        self._shared_handles_ref = None

    # Stats / progress
    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOpV3)
        return upstream.num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        usage = ExecutionResources.zero()
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            if bundle is None:
                continue
            usage = usage.add(ExecutionResources(cpu=bundle.cpu, memory=bundle.memory))
        return usage

    def incremental_resource_usage(self) -> ExecutionResources:
        """Per-task resource ask for the framework's budget allocator.

        Uses the upstream mapper op's per-partition decoded byte totals
        (same source v2 uses, see shuffle_reduce_operator.py:311-324).
        The avg-over-partitions estimate matches v2's policy: it's a
        typical-case admission hint; per-task ``.options(memory=...)``
        in ``_dispatch_one_reducer`` uses the *exact* per-partition bytes
        so skewed partitions are sized correctly at Ray-core level.
        """
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOpV3)
        partition_bytes = upstream.get_partition_bytes()
        memory = 0
        sizes = [b for b in partition_bytes.values() if b > 0]
        if sizes:
            avg_bytes = sum(sizes) / len(sizes)
            memory = int(avg_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER)
        return ExecutionResources(cpu=self._reduce_num_cpus, memory=memory)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        submitted = self._num_reduce_tasks_submitted
        done = submitted - len(self._shuffle_reduce_tasks)
        return f"reduce: {done}/{submitted}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Reduce":
            self._reduce_bar = pg
