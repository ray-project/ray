import functools
import logging
import typing
from collections import deque
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union

import numpy as np
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
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    handle_from_block,
    teardown_disk_shuffle,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    _DEFAULT_FETCH_THREADS,
    _DEFAULT_MAX_BYTES_PER_FETCH,
    ReduceFn,
    _disk_shuffle_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    _SHUFFLE_REDUCE_RUNTIME_ENV,
    _merged_reduce_runtime_env,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.util import merge_label_selector
from ray.data.block import BlockAccessor, BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext
from ray.types import ObjectRef

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer

logger = logging.getLogger(__name__)


class DiskHashShuffleReduceOp(PhysicalOperator):
    """Disk-shuffle reduce operator.

    Consumes the disk-shuffle map operator(s) built by
    ``plan_disk_hash_shuffle_map`` directly: one handle block per completed
    mapper in via ``_add_input_inner``, one reduce task per partition out.
    Each handle names the mapper's on-disk file and its per-partition byte
    ranges, rows and bytes; ``_disk_shuffle_reduce_task`` uses the handles to
    fetch its partition over Arrow Flight from each mapper's
    ``ShuffleFileServer``.

    Supports one or more co-partitioned upstream map operators. With multiple
    inputs (e.g. join) every input must be partitioned into the same
    ``num_partitions``; the reducer receives one table list per input. The op
    also owns end-of-shuffle cleanup (file servers and on-disk directories)
    since it is the last reader of the mapper files.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: Union[PhysicalOperator, List[PhysicalOperator]],
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        disallow_block_splitting: bool = False,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        peak_memory_multiplier: float = SHUFFLE_PEAK_MEMORY_MULTIPLIER,
        name: str = "DiskHashShuffleReduce",
        should_emit_empty_partitions: bool = True,
        preserves_row_count: bool = True,
        fused_output_map_transformer: Optional["MapTransformer"] = None,
        fused_output_map_task_kwargs: Optional[Dict[str, Any]] = None,
        fused_output_map_target_max_block_size_override: Optional[int] = None,
        blocking_materializing: bool = False,
    ):
        input_ops: List[PhysicalOperator] = (
            [input_op] if isinstance(input_op, PhysicalOperator) else list(input_op)
        )
        assert input_ops, (
            "DiskHashShuffleReduceOp requires at least one upstream "
            "disk-shuffle map operator"
        )
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
            blocking_materializing=blocking_materializing,
        )

        self._num_inputs: int = len(input_ops)
        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting
        self._emit_empty_partitions: bool = should_emit_empty_partitions
        # False when reduce_fn (aggregation) or a fused map can change the row
        # count, so num_output_rows_total() can't borrow the map op's total.
        self._preserves_row_count: bool = preserves_row_count
        self._peak_memory_multiplier: float = peak_memory_multiplier

        self._reduce_ray_remote_args: Dict[str, Any] = dict(
            reduce_ray_remote_args or {}
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0
        self._handle_bundles: List[RefBundle] = []
        self._handle_refs_by_input: List[List[ObjectRef]] = [[] for _ in input_ops]
        self._shared_handles_by_input: List[Optional["ray.ObjectRef"]] = [None] * len(
            input_ops
        )
        self._partition_rows_by_input: List[np.ndarray] = [
            np.zeros(num_partitions, dtype=np.int64) for _ in input_ops
        ]
        self._partition_bytes_by_input: List[np.ndarray] = [
            np.zeros(num_partitions, dtype=np.int64) for _ in input_ops
        ]
        self._schema: Optional[pa.Schema] = None
        # (shuffle_id, node_id) of every file server the mappers created.
        self._file_servers: Set[Tuple[str, str]] = set()
        self._fused_output_map_transformer = fused_output_map_transformer
        self._fused_output_map_task_kwargs = fused_output_map_task_kwargs or {}
        self._fused_output_map_target_max_block_size_override = (
            fused_output_map_target_max_block_size_override
        )

        self._output_queue: deque = deque()
        self._output_blocks_stats: List[BlockStats] = []

        # _disk_shuffle_reduce_task tuning (internal defaults, not exposed):
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
        user_args = dict(self._reduce_ray_remote_args)
        user_runtime_env = user_args.pop("runtime_env", None)
        remote_args.update(user_args)
        if user_runtime_env is not None:
            remote_args["runtime_env"] = _merged_reduce_runtime_env(user_runtime_env)
        remote_args["name"] = self.name
        remote_args["num_returns"] = "streaming"
        remote_args = merge_label_selector(
            remote_args, self.data_context.execution_options.label_selector
        )
        return remote_args

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert 0 <= input_index < self._num_inputs
        for entry in refs.blocks:
            handle_block = ray.get(entry.ref)  # pyrefly: ignore[no-matching-overload]
            self._record_handle(handle_from_block(handle_block), input_index)
            self._handle_refs_by_input[input_index].append(entry.ref)
        self._handle_bundles.append(refs)

    def _record_handle(self, handle: Mapping[str, Any], input_index: int) -> None:
        for key, tallies in (
            ("num_rows", self._partition_rows_by_input),
            ("decoded_bytes", self._partition_bytes_by_input),
        ):
            counts = handle.get(key)
            if counts is not None:
                tallies[input_index] += np.asarray(counts, dtype=np.int64)
        if self._schema is None:
            self._schema = handle.get("schema")
        shuffle_id = handle.get("shuffle_id")
        node_id = handle.get("node_id")
        if shuffle_id and node_id:
            self._file_servers.add((shuffle_id, node_id))

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._shared_handles_by_input = [
            ray.put(refs) if refs else None for refs in self._handle_refs_by_input
        ]
        if not any(self._handle_refs_by_input):
            return
        for partition_id in range(self._num_partitions):
            if not self._try_emit_empty_partition(partition_id):
                self._submit_reduce_task(partition_id)

    def _try_emit_empty_partition(self, partition_id: int) -> bool:
        """Return True if ``partition_id`` was handled without a reduce task.

        Only a single-input partition with no rows and a known schema, and no
        fused map (which must still run, e.g. a Write). Emits one typed empty
        block, or nothing when ``should_emit_empty_partitions`` is False
        (aggregations: the mappers' pre-finalize schema would conflict with
        finalized partitions).
        """
        if self._num_inputs != 1 or self._fused_output_map_transformer is not None:
            return False
        if not isinstance(self._schema, pa.Schema):
            return False
        if self._partition_rows_by_input[0][partition_id] > 0:
            return False
        if self._emit_empty_partitions:
            self._emit_empty_partition(self._schema)
        return True

    def _submit_reduce_task(self, partition_id: int) -> None:
        handles_refs: List[Any] = [
            shared if shared is not None else []
            for shared in self._shared_handles_by_input
        ]
        estimated_bytes = self._partition_bytes(partition_id)
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

        block_gen = _disk_shuffle_reduce_task.options(**reduce_options).remote(
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
                self._handle_reduce_done, partition_id
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(reduce_options),
            operator_name=self.name,
        )

        assert (
            partition_id not in self._shuffle_reduce_tasks
        ), f"partition_id {partition_id} already has an in-flight reducer"
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        self._metrics.on_task_submitted(
            partition_id,
            RefBundle((), schema=None, owns_blocks=False),
            task_id=data_task.get_task_id(),
        )

    def _partition_bytes(self, partition_id: int) -> int:
        return int(
            sum(
                partition_bytes[partition_id]
                for partition_bytes in self._partition_bytes_by_input
            )
        )

    def _emit_empty_partition(self, schema: pa.Schema) -> None:
        """Emit one empty output block for an empty partition.

        The partition contributed no rows, so there is nothing to reduce; we
        build the empty block from the schema the mappers reported and queue it
        as this partition's single output block.
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

        # Empty partition creates a new block; register it for memory tracking.
        self._block_ref_counter.on_block_produced(
            out_bundle.blocks[0].ref,  # pyrefly: ignore[bad-argument-type]
            block_meta.size_bytes or 0,
            self.id,
        )
        self._num_reduce_tasks_submitted += 1
        self._output_queue.append(out_bundle)
        self._metrics.on_output_queued(out_bundle)
        self._update_output_estimates()

    def _update_output_estimates(self) -> None:
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            self._num_reduce_tasks_submitted,
            self.upstream_op_num_outputs(),
            self._metrics,
            total_num_tasks=self._num_partitions,
        )
        self._estimated_num_output_bundles = num_outputs
        self._estimated_output_num_rows = num_rows

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
        self._update_output_estimates()

    def _handle_reduce_done(
        self,
        partition_id: int,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
        aborted: bool,
    ) -> None:
        if partition_id not in self._shuffle_reduce_tasks:
            return
        self._shuffle_reduce_tasks.pop(partition_id)
        if exc:
            logger.error(
                f"Reduce of partition {partition_id} failed: {exc}", exc_info=exc
            )
        self._metrics.on_task_finished(
            task_index=partition_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )

    def has_execution_finished(self) -> bool:
        if self._shuffle_reduce_tasks or self._output_queue:
            return False
        return super().has_execution_finished()

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()
        teardown_disk_shuffle(self._file_servers)
        for bundle in self._handle_bundles:
            bundle.destroy_if_owned()
        self._handle_bundles.clear()
        self._handle_refs_by_input = [[] for _ in range(self._num_inputs)]
        self._shared_handles_by_input = [None] * self._num_inputs

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        # Multi-input reduces (e.g. join) and non-row-preserving reduces
        # (aggregation, fused map) can grow or shrink the row count, so it is
        # unknown until the reducers run.
        if self._num_inputs > 1 or not self._preserves_row_count:
            return None
        if self._inputs_complete:
            return int(self._partition_rows_by_input[0].sum())
        writer = self.input_dependencies[0]
        if (
            isinstance(writer, TaskPoolMapOperator)
            and len(writer._logical_operators) <= 1
            and writer.input_dependencies
        ):
            return writer.input_dependencies[0].num_output_rows_total()
        return None

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
        """Per-task resource ask for the framework's budget allocator: the
        average non-empty partition's request, as for ShuffleReduceOp."""
        total_bytes = np.sum(self._partition_bytes_by_input, axis=0)
        non_empty = total_bytes[total_bytes > 0]
        memory = (
            int(non_empty.mean() * self._peak_memory_multiplier)
            if non_empty.size
            else 0
        )
        return ExecutionResources.from_resource_dict(
            self._reduce_task_remote_args(memory)
        )

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        submitted = self._num_reduce_tasks_submitted
        done = submitted - len(self._shuffle_reduce_tasks)
        return f"reduce: {done}/{submitted}"
