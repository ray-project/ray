import functools
import logging
import typing
from collections import defaultdict, deque
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np
import pyarrow as pa

import ray
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
    ReorderingBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
    task_resource_dict,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    OpTask,
    TaskExecDriverStats,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    _SHUFFLE_FILE_SERVER_NAMESPACE,
    ShuffleFileServer,
    _file_server_name,
    disk_shuffle_dirs,
    handle_from_block,
    teardown_disk_shuffle,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    _DEFAULT_FETCH_THREADS,
    _DEFAULT_MAX_BYTES_PER_FETCH,
    ReduceFn,
    _disk_shuffle_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    extract_partition_id,
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    _SHUFFLE_REDUCE_RUNTIME_ENV,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.shuffle_phase_metrics import (
    PhaseSpec,
    ShuffleOperatorMetrics,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.util import merge_label_selector, merge_runtime_envs
from ray.data._internal.logical.interfaces.partitioning import HashPartitioning
from ray.data._internal.stats import StatsDict
from ray.data._internal.utils.transform_pyarrow import _create_empty_table
from ray.data.block import BlockAccessor, BlockMetadata, BlockStats, TaskExecWorkerStats
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer

logger = logging.getLogger(__name__)


class DiskHashShuffleRegroupOp(PhysicalOperator):
    """Regroups the disk-shuffle writers' handles into one bundle per partition.

    Each writer task (``plan_disk_hash_shuffle_map``) emits one handle block,
    and every reduce task needs all of them. This driver-only op collects the
    handles and, once every writer is done, emits one bundle per partition, as
    ``ShuffleMapOp`` does for the object-store reduce. The reduce then submits
    one task per ``add_input``, which the executor's backpressure paces.

    A partition's bundle holds one block entry per input: a ref to that
    input's list of handle refs (shared by all partitions), with the
    partition's rows and decoded bytes from that input as its metadata.

    The op also owns the per-node ``ShuffleFileServer`` actors: it creates one
    per (shuffle_id, node_id) the first time a mapper handle from that node
    arrives, and kills them, along with the on-disk directories, in
    ``teardown``, which the reduce calls once no reducer can still need the
    files. Owning them here (not as detached actors created by map tasks) ties
    their lifetime to the driver, so they cannot outlive the job, and no server
    is created for a node whose mappers never reported a handle.
    """

    def __init__(
        self,
        input_ops: List[PhysicalOperator],
        data_context: DataContext,
        *,
        num_partitions: int,
        name: str = "DiskHashShuffleRegroup",
    ):
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
        )
        self._num_partitions: int = num_partitions
        self._handle_bundles: List[RefBundle] = []
        self._handle_refs_by_input: List[List[ObjectRef]] = [[] for _ in input_ops]
        # Map task index of each handle, parallel to ``_handle_refs_by_input``.
        self._handle_map_ids_by_input: List[List[int]] = [[] for _ in input_ops]
        self._partition_rows_by_input: List[np.ndarray] = [
            np.zeros(num_partitions, dtype=np.int64) for _ in input_ops
        ]
        self._partition_bytes_by_input: List[np.ndarray] = [
            np.zeros(num_partitions, dtype=np.int64) for _ in input_ops
        ]
        self._schema_by_input: List[Optional[pa.Schema]] = [None] * len(input_ops)
        # (shuffle_id, node_id) -> owned ShuffleFileServer handle, created on
        # the first handle from that node and killed in teardown.
        self._file_servers: Dict[Tuple[str, str], "ray.actor.ActorHandle"] = {}
        self._output_queue: deque = deque()
        self._estimated_num_output_bundles = num_partitions

    @property
    def num_inputs(self) -> int:
        return len(self.input_dependencies)

    @property
    def partition_bytes_by_input(self) -> List[np.ndarray]:
        return self._partition_bytes_by_input

    @property
    def is_synchronization_barrier(self) -> bool:
        # Every partition's bundle needs every writer's handle.
        return True

    def throttling_disabled(self) -> bool:
        # Launches no tasks, so it needs no share of the resource budget.
        return True

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        handle_refs = [entry.ref for entry in refs.blocks]
        handle_blocks = ray.get(handle_refs)  # pyrefly: ignore[no-matching-overload]
        for ref, handle_block in zip(handle_refs, handle_blocks):
            handle = handle_from_block(handle_block)
            self._record_handle(handle, input_index)
            self._handle_refs_by_input[input_index].append(ref)
            self._handle_map_ids_by_input[input_index].append(
                handle.get("map_id", len(self._handle_map_ids_by_input[input_index]))
            )
        self._handle_bundles.append(refs)

    def _record_handle(self, handle: Mapping[str, Any], input_index: int) -> None:
        for key, tallies in (
            ("num_rows", self._partition_rows_by_input),
            ("decoded_bytes", self._partition_bytes_by_input),
        ):
            counts = handle.get(key)
            if counts is not None:
                tallies[input_index] += np.asarray(counts, dtype=np.int64)
        if self._schema_by_input[input_index] is None:
            self._schema_by_input[input_index] = handle.get("schema")
        shuffle_id = handle.get("shuffle_id")
        node_id = handle.get("node_id")
        if shuffle_id and node_id and (shuffle_id, node_id) not in self._file_servers:
            self._file_servers[(shuffle_id, node_id)] = self._create_file_server(
                shuffle_id, node_id
            )

    @staticmethod
    def _create_file_server(shuffle_id: str, node_id: str) -> "ray.actor.ActorHandle":
        """Start the owned, node-pinned ``ShuffleFileServer`` for one mapper node.

        Named-actor creation registers the name with GCS synchronously, so a
        reducer's ``ray.get_actor`` can never run ahead of it; the actor's
        startup overlaps the rest of the map phase. The served directory is
        the one the planner handed the mappers for this shuffle.
        """
        map_dir, _ = disk_shuffle_dirs(shuffle_id)
        return ShuffleFileServer.options(  # pyrefly: ignore[missing-attribute]
            name=_file_server_name(shuffle_id, node_id),
            namespace=_SHUFFLE_FILE_SERVER_NAMESPACE,
            max_restarts=-1,
            max_task_retries=-1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
            num_cpus=0,
        ).remote(map_dir)

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        if not any(self._handle_refs_by_input):
            return
        # Writers finish in any order; publish handles in map task order so a
        # reducer's input, and so its output, is the same on every run.
        shared_handles = [
            ray.put([ref for _, ref in sorted(zip(map_ids, refs), key=lambda t: t[0])])
            for map_ids, refs in zip(
                self._handle_map_ids_by_input, self._handle_refs_by_input
            )
        ]
        for partition_id in range(self._num_partitions):
            entries = tuple(
                BlockEntry(
                    ref=shared,  # pyrefly: ignore[bad-argument-type]
                    metadata=BlockMetadata(
                        num_rows=int(rows[partition_id]),
                        size_bytes=int(nbytes[partition_id]),
                        exec_stats=None,
                        input_files=make_partition_sentinel(partition_id),
                    ),
                )
                for shared, rows, nbytes in zip(
                    shared_handles,
                    self._partition_rows_by_input,
                    self._partition_bytes_by_input,
                )
            )
            bundle = RefBundle(
                entries, schema=self._schema_by_input[0], owns_blocks=False
            )
            self._output_queue.append(bundle)
            self._metrics.on_output_queued(bundle)

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return []

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: []}

    def num_output_rows_total(self) -> Optional[int]:
        if self.num_inputs > 1:
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

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._output_queue.clear()

    def teardown(self) -> None:
        """Kill the owned file servers and remove the mappers' shuffle files."""
        # ``no_restart=True`` goes through GCS DestroyActor, overriding
        # ``max_restarts=-1``.
        for server in self._file_servers.values():
            ray.kill(server, no_restart=True)
        teardown_disk_shuffle(self._file_servers.keys())
        self._file_servers.clear()
        for bundle in self._handle_bundles:
            bundle.destroy_if_owned()
        self._handle_bundles.clear()
        self._handle_refs_by_input = [[] for _ in range(self.num_inputs)]
        self._handle_map_ids_by_input = [[] for _ in range(self.num_inputs)]


class DiskHashShuffleReduceOp(PhysicalOperator):
    """Disk-shuffle reduce operator.

    Consumes a ``DiskHashShuffleRegroupOp``: one bundle per partition in via
    ``_add_input_inner``, one reduce task per partition out. The bundle refs
    each input's mapper handles, which name the mapper's on-disk file and its
    per-partition byte ranges; ``_disk_shuffle_reduce_task`` uses them to
    fetch its partition over Arrow Flight from each mapper's
    ``ShuffleFileServer``.

    Supports one or more co-partitioned inputs. With multiple inputs (e.g.
    join) every input must be partitioned into the same ``num_partitions``;
    the reducer receives one table list per input.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: DiskHashShuffleRegroupOp,
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        disallow_block_splitting: bool = False,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        preserve_partition_order: bool = False,
        peak_memory_multiplier: float = SHUFFLE_PEAK_MEMORY_MULTIPLIER,
        name: str = "DiskHashShuffleReduce",
        should_emit_empty_partitions: bool = True,
        preserves_row_count: bool = True,
        fused_output_map_transformer: Optional["MapTransformer"] = None,
        fused_output_map_task_kwargs: Optional[Dict[str, Any]] = None,
        fused_output_map_target_max_block_size_override: Optional[int] = None,
        output_partitioning: Optional[HashPartitioning] = None,
    ):
        assert isinstance(input_op, DiskHashShuffleRegroupOp), input_op
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_inputs: int = input_op.num_inputs
        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting
        self._preserve_partition_order: bool = preserve_partition_order
        self._emit_empty_partitions: bool = should_emit_empty_partitions
        self._output_partitioning: Optional[HashPartitioning] = output_partitioning
        # False when reduce_fn (aggregation) or a fused map can change the row
        # count, so num_output_rows_total() can't borrow the map op's total.
        self._preserves_row_count: bool = preserves_row_count
        self._peak_memory_multiplier: float = peak_memory_multiplier

        self._reduce_ray_remote_args: Dict[str, Any] = dict(
            reduce_ray_remote_args or {}
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0
        self._fused_output_map_transformer = fused_output_map_transformer
        self._fused_output_map_task_kwargs = fused_output_map_task_kwargs or {}
        self._fused_output_map_target_max_block_size_override = (
            fused_output_map_target_max_block_size_override
        )

        self._output_queue: BaseBundleQueue = (
            ReorderingBundleQueue()
            if self._preserve_partition_order
            else FIFOBundleQueue()
        )
        # See set_emit_whole_partitions.
        self._emit_whole_partitions: bool = False
        # Each partition's outputs, kept until its reduce task finishes.
        self._held_partition_outputs: Dict[int, List[RefBundle]] = defaultdict(list)
        # Partitions that skipped the reduce task (single input, no rows) and
        # the bundles emitted for them; the task estimate covers the rest.
        self._num_empty_partitions: int = 0
        self._num_empty_partition_bundles: int = 0

        self._phase_metrics = ShuffleOperatorMetrics(
            self,
            self._metrics,
            [
                PhaseSpec(
                    "reduce",
                    "Reduce",
                    is_output_phase=True,
                    total_num_tasks=num_partitions,
                )
            ],
        )
        self._reduce_metrics = self._phase_metrics.phase_metrics["reduce"]

        # _disk_shuffle_reduce_task tuning (internal defaults, not exposed):
        self._max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH
        self._fetch_threads: int = _DEFAULT_FETCH_THREADS

    def set_emit_whole_partitions(self) -> None:
        """Same as ``ShuffleReduceOp.set_emit_whole_partitions``."""
        self._emit_whole_partitions = True

    def _reduce_task_remote_args(self, memory_estimate: int) -> Dict[str, Any]:
        remote_args: Dict[str, Any] = {
            "num_cpus": self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS,
            "scheduling_strategy": "SPREAD",
            "runtime_env": _SHUFFLE_REDUCE_RUNTIME_ENV,
        }
        if memory_estimate > 0:
            remote_args["memory"] = memory_estimate
        # None means "unset" to ray.remote, so it must not override the defaults.
        user_args = {
            k: v for k, v in self._reduce_ray_remote_args.items() if v is not None
        }
        user_runtime_env = user_args.pop("runtime_env", None)
        remote_args.update(user_args)
        if user_runtime_env is not None:
            remote_args["runtime_env"] = merge_runtime_envs(
                _SHUFFLE_REDUCE_RUNTIME_ENV, user_runtime_env
            )
        remote_args["name"] = self.name
        remote_args["num_returns"] = "streaming"
        remote_args = merge_label_selector(
            remote_args, self.data_context.execution_options.label_selector
        )
        return remote_args

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        partition_id = extract_partition_id(refs)
        if not self._try_emit_empty_partition(partition_id, refs):
            self._submit_reduce_task(partition_id, refs)

    def _try_emit_empty_partition(self, partition_id: int, refs: RefBundle) -> bool:
        """Return True if the partition was handled without a reduce task.

        Only a single-input partition with no rows and a known schema, and no
        fused map (which must still run, e.g. a Write). Emits one typed empty
        block, or nothing when ``should_emit_empty_partitions`` is False
        (aggregations: the mappers' pre-finalize schema would conflict with
        finalized partitions).
        """
        if self._num_inputs != 1 or self._fused_output_map_transformer is not None:
            return False
        if not isinstance(refs.schema, pa.Schema):
            return False
        if refs.blocks[0].metadata.num_rows:
            return False
        self._on_empty_partition()
        if self._emit_empty_partitions:
            self._emit_empty_partition(partition_id, refs.schema)
        elif self._preserve_partition_order:
            # The key must still be finalized or the ordered queue would
            # never advance past this partition.
            self._output_queue.finalize(key=partition_id)
        return True

    def _submit_reduce_task(self, partition_id: int, refs: RefBundle) -> None:
        # One entry per input, each a ref to that input's handle list.
        handles_refs = [entry.ref for entry in refs.blocks]
        estimated_bytes = sum((m.size_bytes or 0) for m in refs.metadata)
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
            task_resource_request=task_resource_dict(reduce_options),
            operator_name=self.name,
        )

        assert (
            partition_id not in self._shuffle_reduce_tasks
        ), f"partition_id {partition_id} already has an in-flight reducer"
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        self._reduce_metrics.on_task_submitted(
            partition_id,
            RefBundle((), schema=None, owns_blocks=False),
            task_id=data_task.get_task_id(),
        )

    def _on_empty_partition(self) -> None:
        self._num_empty_partitions += 1
        # The per-task average only describes partitions that run a task.
        self._reduce_metrics.total_num_tasks = (
            self._num_partitions - self._num_empty_partitions
        )
        if self._phase_metrics.estimated_num_output_bundles is not None:
            self._phase_metrics.update_output_estimates(
                self._reduce_metrics, self._num_reduce_tasks_submitted
            )

    def _emit_empty_partition(self, partition_id: int, schema: pa.Schema) -> None:
        """Emit one empty output block for an empty partition.

        The partition contributed no rows, so there is nothing to reduce; we
        build the empty block from the schema the mappers reported and queue it
        as this partition's single output block.
        """
        empty_block = _create_empty_table(schema)
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
        self._num_empty_partition_bundles += 1
        self._record_partition_output(partition_id, out_bundle, partition_complete=True)

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.get_next()
        self._reduce_metrics.on_output_dequeued(bundle)
        self._reduce_metrics.record_block_stats(*bundle.metadata)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_reduce_tasks.values())

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
        if self._emit_whole_partitions:
            # Hold it until the task ends, but count it as queued for memory.
            self._held_partition_outputs[partition_id].append(bundle)
            self._reduce_metrics.on_output_queued(bundle)
        else:
            self._record_partition_output(partition_id, bundle)
        self._reduce_metrics.on_task_output(partition_id, bundle)

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
        if self._emit_whole_partitions:
            # Queue the finished partition as one bundle, or drop it on failure.
            held = self._held_partition_outputs.pop(partition_id, [])
            for bundle in held:
                self._reduce_metrics.on_output_dequeued(bundle)
            if held and exc is None:
                self._record_partition_output(
                    partition_id, RefBundle.merge_ref_bundles(held)
                )
        if self._preserve_partition_order:
            self._output_queue.finalize(key=partition_id)
        self._reduce_metrics.on_task_finished(
            partition_id, exc, task_exec_stats, task_exec_driver_stats
        )
        # The per-task output average only exists once a task has finished.
        self._phase_metrics.update_output_estimates(
            self._reduce_metrics, self._num_reduce_tasks_submitted
        )

    def _record_partition_output(
        self,
        partition_id: int,
        bundle: RefBundle,
        *,
        partition_complete: bool = False,
    ) -> None:
        self._output_queue.add(bundle, key=partition_id)
        self._reduce_metrics.on_output_queued(bundle)
        if self._preserve_partition_order and partition_complete:
            self._output_queue.finalize(key=partition_id)

    def has_execution_finished(self) -> bool:
        if self._shuffle_reduce_tasks or len(self._output_queue) > 0:
            return False
        return super().has_execution_finished()

    def _do_shutdown(self, force: bool = False) -> None:
        # Cancel in-flight reducers before their mapper files go away. The
        # executor shuts the regroup down first, so the reduce owns teardown.
        try:
            super()._do_shutdown(force)
        finally:
            self._shuffle_reduce_tasks.clear()
            self._output_queue.clear()
            self._held_partition_outputs.clear()
            regroup_op = self.input_dependencies[0]
            assert isinstance(regroup_op, DiskHashShuffleRegroupOp)
            regroup_op.teardown()

    def _extra_metrics(self) -> Dict[str, Any]:
        return self._phase_metrics.extra_metrics()

    def get_stats(self) -> StatsDict:
        return self._phase_metrics.stats()

    def num_outputs_total(self) -> Optional[int]:
        estimate = self._phase_metrics.estimated_num_output_bundles
        if estimate is None:
            # No reducer has finished; the total is only known once no
            # partition is left that needs one.
            if self._num_empty_partitions < self._num_partitions:
                return None
            return self._num_empty_partition_bundles
        return estimate + self._num_empty_partition_bundles

    def num_output_rows_total(self) -> Optional[int]:
        # Multi-input reduces (e.g. join) and non-row-preserving reduces
        # (aggregation, fused map) can grow or shrink the row count, so it is
        # estimated from finished reducers instead of borrowed from upstream.
        if self._num_inputs > 1 or not self._preserves_row_count:
            estimate = self._phase_metrics.estimated_output_num_rows
            if estimate is None and self._num_empty_partitions == self._num_partitions:
                return 0
            return estimate
        return self.input_dependencies[0].num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        cpu: float = 0
        gpu: float = 0
        memory: float = 0
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            if bundle is None:
                continue
            cpu += bundle.cpu
            gpu += bundle.gpu
            memory += bundle.memory
        return ExecutionResources(cpu=cpu, gpu=gpu, memory=memory)

    def incremental_resource_usage(self) -> ExecutionResources:
        """Per-task resource ask for the framework's budget allocator: the
        average non-empty partition's request, as for ShuffleReduceOp."""
        regroup_op = self.input_dependencies[0]
        assert isinstance(regroup_op, DiskHashShuffleRegroupOp)
        total_bytes = np.asarray(sum(regroup_op.partition_bytes_by_input))
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
