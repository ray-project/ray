"""ExternalHashShuffleReduceOp — reduce phase of the external-shuffle variant.

Consumes the N ``partition wrapper`` bundles emitted by
``ExternalHashShuffleMapOp`` (one per partition_id, each carrying a shared
handle-list ref + a ``__partition__<pid>`` sentinel in metadata) and
dispatches one ``external_hash_shuffle_reduce_task`` per bundle. Each reduce task is a Ray
streaming generator that yields ``(block, pickled metadata)`` pairs
matching the v2 ``_shuffle_reduce_task`` protocol; we wrap each
generator in a ``DataOpTask`` so the executor sees ordinary streaming
output bundles.

Structurally identical to v2 ``ShuffleReduceOp``: one input bundle in,
one reduce task out (via ``_add_input_inner``). The executor's normal
backpressure + resource-manager machinery therefore gates reducer
dispatch — N gating points (one per partition wrapper), same as v2.

The only external-shuffle-specific twist is what the input bundle carries:
instead of M shard ObjectRefs (plasma variant), an external-shuffle
wrapper carries ONE ref pointing at a
shared plasma object holding the full mapper-handle list. That single
ref, plus the sentinel-encoded partition_id, is everything
``external_hash_shuffle_reduce_task`` needs to pull its partition's bytes from every source
node's ``ShuffleManager``.

Barrier semantics: ``ExternalHashShuffleMapOp.all_inputs_done`` only emits the N
wrappers after every mapper has finished, so ``ExternalHashShuffleReduceOp``
still can't dispatch a reducer before map is fully closed — same
effective ordering as v2 (v2's ``ShuffleMapOp._maybe_emit_partition_bundles``
also gates on ``_inputs_complete``).
"""

import functools
import logging
import typing
from collections import deque
from typing import Any, Dict, List, Optional

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
from ray.data._internal.execution.operators.hash_shuffle_external import (
    _DEFAULT_MAX_BYTES_PER_FETCH,
    ReduceFn,
    external_hash_shuffle_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    extract_partition_id,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_external import (  # noqa: E501
    ExternalHashShuffleMapOp,
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


class ExternalHashShuffleReduceOp(PhysicalOperator, SubProgressBarMixin):
    """External-shuffle reduce operator.

    Structurally mirrors ``ShuffleReduceOp``: one wrapper bundle in via
    ``_add_input_inner``, one reduce task out. The wrapper carries
    ``shared_handles_ref`` + partition_id sentinel; ``external_hash_shuffle_reduce_task``
    uses those to fetch its partition's bytes over TCP from each
    mapper's ``ShuffleManager``.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: ExternalHashShuffleMapOp,
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        disallow_block_splitting: bool = False,
        reduce_cpus: Optional[float] = None,
        name: str = "ExternalHashShuffleReduce",
        fused_output_map_transformer: Optional["MapTransformer"] = None,
        fused_output_map_task_kwargs: Optional[Dict[str, Any]] = None,
        fused_output_map_target_max_block_size_override: Optional[int] = None,
        # -- External-shuffle-specific below --
        coalesce_output: bool = False,
        max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
        reduce_prefetch_dir: Optional[str] = None,
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting

        # -- Reduce task config & tracking -----------------------------------
        self._shuffle_reduce_task_num_cpus: float = (
            reduce_cpus
            if reduce_cpus is not None
            else self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0

        # -- Fused downstream map --------------------------------------------
        self._fused_output_map_transformer: Optional[
            "MapTransformer"
        ] = fused_output_map_transformer
        self._fused_output_map_task_kwargs: Dict[str, Any] = (
            fused_output_map_task_kwargs or {}
        )
        self._fused_output_map_target_max_block_size_override: Optional[int] = (
            fused_output_map_target_max_block_size_override
        )

        # -- Output queue ----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Stats -----------------------------------------------------------
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._reduce_bar: Optional["BaseProgressBar"] = None

        # =====================================================================
        # External-shuffle-specific state below.
        # =====================================================================

        # external_hash_shuffle_reduce_task behavior knobs (no v2 counterpart):
        # - ``coalesce_output``: concat all reduce_fn output into one block
        # - ``max_bytes_per_fetch``: cap per-FETCH byte volume
        # - ``reduce_prefetch_dir``: staging dir for prefetch.bin
        self._coalesce_output: bool = coalesce_output
        self._max_bytes_per_fetch: int = max_bytes_per_fetch
        self._reduce_prefetch_dir: Optional[str] = reduce_prefetch_dir

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Executor calls this with one partition wrapper at a time (bundle
        contains 1 block-ref = ``shared_handles_ref`` + partition_id
        sentinel in metadata). We extract the two pieces and dispatch one
        reduce task. Backpressure is fully executor-driven — no
        accumulation, no dispatch loop.
        """
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return

        partition_id = extract_partition_id(refs)
        # The wrapper's sole block-ref IS the shared handle-list plasma
        # object built by ExternalHashShuffleMapOp.all_inputs_done. Ray dispatch sees
        # 1 nested borrowed ref per reduce task (not M).
        handles_ref = refs.block_refs[0]
        # size_bytes comes from the wrapper's metadata (populated by
        # ExternalHashShuffleMapOp from its per-partition decoded-byte accumulator).
        estimated_bytes = sum((m.size_bytes or 0) for m in refs.metadata)

        # Match v2's disallow_block_splitting semantics
        # (shuffle_reduce_operator.py:161-165): when True, drop the
        # BlockOutputBuffer reshape target so a partition emits as a
        # single block regardless of size.
        target_max_block_size = (
            None
            if self._disallow_block_splitting
            else self.data_context.target_max_block_size
        )

        self._dispatch_one_reducer(
            partition_id,
            handles_ref,
            estimated_bytes,
            target_max_block_size,
        )
        # Wrapper was built with owns_blocks=False (shared_handles_ref is
        # owned by the upstream map op), so this is a no-op for the ref —
        # it just releases the Python-side RefBundle.
        refs.destroy_if_owned()

    def _dispatch_one_reducer(
        self,
        partition_id: int,
        handles_ref: ObjectRef,
        estimated_bytes: int,
        target_max_block_size: Optional[int],
    ) -> None:
        # Per-partition memory ask: 2× the decoded byte total this reducer
        # will see (bounds peak heap from accum + reshape carry). Estimate
        # comes from the wrapper's metadata (populated by the map op from
        # its per-partition decoded_bytes accumulator).
        reduce_resources: Dict[str, Any] = {
            "num_cpus": self._shuffle_reduce_task_num_cpus,
        }
        if estimated_bytes > 0:
            reduce_resources["memory"] = int(
                estimated_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER
            )
        reduce_options: Dict[str, Any] = {
            **reduce_resources,
            "scheduling_strategy": "SPREAD",
            "num_returns": "streaming",
        }

        block_gen = external_hash_shuffle_reduce_task.options(**reduce_options).remote(
            handles_ref,
            partition_id,
            self._reduce_fn,
            self._reduce_prefetch_dir,
            self._max_bytes_per_fetch,
            target_max_block_size,
            self._fused_output_map_transformer,
            self.name,
            self._fused_output_map_task_kwargs,
            self._coalesce_output,
            self._fused_output_map_target_max_block_size_override,
        )

        data_task = DataOpTask(
            task_index=partition_id,
            streaming_gen=block_gen,
            block_ref_counter=self._block_ref_counter,
            producer_id=self.id,
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

        # Map emits each partition exactly once, so no collision possible.
        assert partition_id not in self._shuffle_reduce_tasks, (
            f"partition_id {partition_id} already has an in-flight reducer "
            f"— ExternalHashShuffleMapOp should emit each partition wrapper exactly once"
        )
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        # For metrics, we don't have a partition-specific data bundle (the
        # wrapper's only ref is the shared handle list), so pass a synthetic
        # empty bundle — same pattern as before.
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

    def has_execution_finished(self) -> bool:
        if self._shuffle_reduce_tasks or self._output_queue:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_reduce_tasks
            and not self._output_queue
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()
        # ShuffleManager actors + handle refs are owned by ExternalHashShuffleMapOp
        # (via its ``_shared_handles_ref`` + pinned input bundles). Its
        # own ``_do_shutdown`` releases them; nothing to clean here.

    # Stats / progress
    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ExternalHashShuffleMapOp)
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
        """Per-task resource ask for the framework's budget allocator."""
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ExternalHashShuffleMapOp)
        partition_bytes = upstream.get_partition_bytes()
        memory = 0
        sizes = [b for b in partition_bytes.values() if b > 0]
        if sizes:
            avg_bytes = sum(sizes) / len(sizes)
            memory = int(avg_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER)
        return ExecutionResources(
            cpu=self._shuffle_reduce_task_num_cpus,
            memory=memory,
        )

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
