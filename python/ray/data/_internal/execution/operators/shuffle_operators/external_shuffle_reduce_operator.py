import functools
import logging
import typing
from collections import deque
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    OpTask,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_map_operator import (  # noqa: E501
    ExternalHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_tasks import (  # noqa: E501
    _DEFAULT_FETCH_THREADS,
    _DEFAULT_MAX_BYTES_PER_FETCH,
    ReduceFn,
    _external_shuffle_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    extract_partition_id,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    _SHUFFLE_REDUCE_RUNTIME_ENV,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data.block import BlockAccessor, BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import (
        MapTransformer,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ExternalHashShuffleReduceOp(PhysicalOperator, SubProgressBarMixin):
    """External-shuffle reduce operator.

    Structurally mirrors ``ShuffleReduceOp``: one wrapper bundle per partition
    per input in via ``_add_input_inner``, one reduce task out. Each wrapper
    carries ``shared_handles_ref`` + partition_id sentinel;
    ``_external_shuffle_reduce_task`` uses those to fetch its partition's bytes
    over Arrow Flight from each mapper's ``ShuffleFileServer``.

    Supports one or more co-partitioned upstream ``ExternalHashShuffleMapOp``s.
    With multiple inputs (e.g. join) every input must be partitioned into the
    same ``num_partitions``; this op pairs up the per-partition wrappers across
    all inputs and the reducer receives one table list per input.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: Union[ExternalHashShuffleMapOp, List[ExternalHashShuffleMapOp]],
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        disallow_block_splitting: bool = False,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        peak_memory_multiplier: float = SHUFFLE_PEAK_MEMORY_MULTIPLIER,
        name: str = "ExternalHashShuffleReduce",
        should_emit_empty_partitions: bool = True,
        fused_output_map_transformer: Optional["MapTransformer"] = None,
        fused_output_map_task_kwargs: Optional[Dict[str, Any]] = None,
        fused_output_map_target_max_block_size_override: Optional[int] = None,
    ):
        input_ops: List[PhysicalOperator] = (
            [input_op]
            if isinstance(input_op, ExternalHashShuffleMapOp)
            else list(input_op)
        )
        assert input_ops, (
            "ExternalHashShuffleReduceOp requires at least one upstream "
            "ExternalHashShuffleMapOp"
        )
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
        )

        self._num_inputs: int = len(input_ops)
        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting
        self._emit_empty_partitions: bool = should_emit_empty_partitions
        self._peak_memory_multiplier: float = peak_memory_multiplier

        # -- Reduce task config & tracking -----------------------------------
        self._reduce_ray_remote_args: Dict[str, Any] = dict(
            reduce_ray_remote_args or {}
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0

        # -- Per-partition pairing across inputs -----------------------------
        # partition_id -> input_index -> that input's wrapper bundle. A reduce
        # task is submitted once all inputs delivered their wrapper.
        self._pending_inputs: Dict[int, Dict[int, RefBundle]] = {}

        # -- Fused downstream map --------------------------------------------
        self._fused_output_map_transformer = fused_output_map_transformer
        self._fused_output_map_task_kwargs = fused_output_map_task_kwargs or {}
        self._fused_output_map_target_max_block_size_override = (
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

        # _external_shuffle_reduce_task tuning (internal defaults, not exposed):
        self._max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH
        self._fetch_threads: int = _DEFAULT_FETCH_THREADS

    def _reduce_task_remote_args(self, memory_estimate: int) -> Dict[str, Any]:
        remote_args: Dict[str, Any] = {
            "num_cpus": self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS,
            "scheduling_strategy": "SPREAD",
            "runtime_env": _SHUFFLE_REDUCE_RUNTIME_ENV,
        }
        if memory_estimate > 0:
            remote_args["memory"] = memory_estimate
        remote_args.update(self._reduce_ray_remote_args)
        remote_args["num_returns"] = "streaming"
        return remote_args

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert 0 <= input_index < self._num_inputs
        if not refs.block_refs:
            refs.destroy_if_owned()
            return

        partition_id = extract_partition_id(refs)
        estimated_rows = sum((m.num_rows or 0) for m in refs.metadata)

        # Single-input empty-partition fast path. Do not gate on
        # ``size_bytes``: a ``null``-typed table can have rows with
        # ``tbl.nbytes == 0``. Skipped for multi-input reduces (an outer
        # join's empty side can still produce rows) and when a downstream map
        # is fused in (the map must run even on empty partitions, e.g. Write).
        schema = refs.schema
        if (
            self._num_inputs == 1
            and self._fused_output_map_transformer is None
            and isinstance(schema, pa.Schema)
            and estimated_rows == 0
        ):
            if self._emit_empty_partitions:
                self._emit_empty_partition(refs, schema)
            else:
                refs.destroy_if_owned()
            return

        pending = self._pending_inputs.setdefault(partition_id, {})
        assert input_index not in pending, (
            f"input {input_index} already delivered a wrapper for partition "
            f"{partition_id}; each ExternalHashShuffleMapOp must emit at most "
            f"one wrapper per partition"
        )
        pending[input_index] = refs
        if len(pending) == self._num_inputs:
            del self._pending_inputs[partition_id]
            self._submit_reduce_task(
                partition_id, [pending[i] for i in range(self._num_inputs)]
            )

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        for partition_id in list(self._pending_inputs.keys()):
            pending = self._pending_inputs.pop(partition_id)
            bundles = [pending.get(i) for i in range(self._num_inputs)]
            self._submit_reduce_task(partition_id, bundles)

    def _submit_reduce_task(
        self,
        partition_id: int,
        bundles: List[Optional[RefBundle]],
    ) -> None:
        handles_refs: List[Any] = []
        estimated_bytes = 0
        for bundle in bundles:
            if bundle is not None and bundle.block_refs:
                handles_refs.append(bundle.block_refs[0])
                estimated_bytes += sum((m.size_bytes or 0) for m in bundle.metadata)
            else:
                handles_refs.append([])

        reduce_options = self._reduce_task_remote_args(
            int(estimated_bytes * self._peak_memory_multiplier)
            if estimated_bytes > 0
            else 0
        )

        target_max_block_size = (
            None
            if self._disallow_block_splitting
            else self.data_context.target_max_block_size
        )

        map_task_context = None
        if self._fused_output_map_transformer is not None:
            map_task_context = TaskContext(
                task_idx=partition_id,
                op_name=self.name,
                target_max_block_size_override=(
                    self._fused_output_map_target_max_block_size_override
                ),
            )
            map_task_context.kwargs.update(self._fused_output_map_task_kwargs)

        block_gen = _external_shuffle_reduce_task.options(**reduce_options).remote(
            *handles_refs,  # pyrefly: ignore[bad-argument-type]
            partition_id=partition_id,
            reduce_fn=self._reduce_fn,
            max_bytes_per_fetch=self._max_bytes_per_fetch,
            fetch_threads=self._fetch_threads,
            target_max_block_size=target_max_block_size,
            map_transformer=self._fused_output_map_transformer,
            map_task_context=map_task_context,
            data_context=self.data_context,
            emit_empty_partition=self._emit_empty_partitions,
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
                self._handle_reduce_done, partition_id, bundles
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(reduce_options),
            operator_name=self.name,
        )

        assert (
            partition_id not in self._shuffle_reduce_tasks
        ), f"partition_id {partition_id} already has an in-flight reducer"
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        # Synthetic empty bundle for metrics: the wrapper's only ref is
        # the shared handle list, not partition-specific data.
        self._metrics.on_task_submitted(
            partition_id,
            RefBundle((), schema=None, owns_blocks=False),
            task_id=data_task.get_task_id(),
        )

    def _emit_empty_partition(self, refs: RefBundle, schema: pa.Schema) -> None:
        """Emit one empty output block for an empty partition.

        The partition contributed no rows, so there is nothing to reduce; we
        build the empty block from the schema the map stage propagated onto
        the wrapper and queue it as this partition's single output block.
        """
        empty_block = schema.empty_table()
        block_meta = BlockAccessor.for_block(empty_block).get_metadata()
        out_bundle = RefBundle(
            (
                BlockEntry(
                    ref=ray.put(empty_block),  # pyrefly: ignore[bad-argument-type]
                    metadata=block_meta,
                ),
            ),
            schema=schema,
            owns_blocks=True,
        )
        refs.destroy_if_owned()

        # Empty partition creates a new block; register it for memory tracking.
        self._block_ref_counter.on_block_produced(
            out_bundle.blocks[0].ref,  # pyrefly: ignore[bad-argument-type]
            block_meta.size_bytes or 0,
            self.id,
        )
        self._num_reduce_tasks_submitted += 1
        self._output_queue.append(out_bundle)
        self._metrics.on_output_queued(out_bundle)
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            self._num_reduce_tasks_submitted,
            self.upstream_op_num_outputs(),
            self._metrics,
            total_num_tasks=self._num_partitions,
        )
        self._estimated_num_output_bundles = num_outputs
        self._estimated_output_num_rows = num_rows
        if self._reduce_bar is not None:
            self._reduce_bar.update(increment=0, total=self.num_output_rows_total())

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._metrics.on_output_dequeued(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_reduce_tasks.values())

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
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
        input_bundles: List[Optional[RefBundle]],
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        for input_bundle in input_bundles:
            if input_bundle is not None:
                input_bundle.destroy_if_owned()
        if partition_id not in self._shuffle_reduce_tasks:
            return
        self._shuffle_reduce_tasks.pop(partition_id)
        self._metrics.on_task_finished(
            task_index=partition_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if exc:
            logger.error(
                f"Reduce of partition {partition_id} failed: {exc}", exc_info=exc
            )

    def has_execution_finished(self) -> bool:
        if self._shuffle_reduce_tasks or self._output_queue or self._pending_inputs:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_reduce_tasks
            and not self._output_queue
            and not self._pending_inputs
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()
        for pending in self._pending_inputs.values():
            for bundle in pending.values():
                bundle.destroy_if_owned()
        self._pending_inputs.clear()

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        # Multi-input reduces (e.g. join) can grow or shrink the row count, so
        # it is unknown until the reducers run; a single-input reduce preserves
        # it.
        if self._num_inputs > 1:
            return None
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ExternalHashShuffleMapOp)
        return upstream.num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        cpu: float = 0
        memory: float = 0
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            if bundle is None:
                continue
            cpu += bundle.cpu
            memory += bundle.memory
        return ExecutionResources(cpu=cpu, memory=memory)

    def incremental_resource_usage(self) -> ExecutionResources:
        memory = 0
        for upstream in self.input_dependencies:
            assert isinstance(upstream, ExternalHashShuffleMapOp)
            sizes = [b for b in upstream.get_partition_bytes().values() if b > 0]
            if sizes:
                avg_bytes = sum(sizes) / len(sizes)
                memory += int(avg_bytes * self._peak_memory_multiplier)
        return ExecutionResources.from_resource_dict(
            self._reduce_task_remote_args(memory)
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
