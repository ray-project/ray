import functools
import logging
import typing
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
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
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    extract_partition_id,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    ReduceFn,
    _shuffle_reduce_task,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data.block import BlockAccessor, BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ShuffleReduceOp(PhysicalOperator, SubProgressBarMixin):
    """Reduce phase of a shuffle.

    Supports one or more co-partitioned upstream `ShuffleMapOp`s.  With a single
    input this is the unary reduce used by repartition/sort.  With multiple
    inputs (e.g. join) every input must be partitioned into the same
    `num_partitions`; this op pairs up the per-partition bundles across all
    inputs and hands the reducer one shard list per input.

    Args:
        input_op: Upstream `ShuffleMapOp`, or a list of them (one per input).
            For an N-input reduce the reducer receives shards in this order.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.  Must match the
            value used by every paired `ShuffleMapOp`.
        reduce_fn: Function called once per partition (in blocking mode)
            or incrementally (in streaming mode) to combine input shards
            into output blocks.  Receives `(partition_id, tables_by_input)`
            where `tables_by_input` is aligned with `input_op`.
        streaming_reduce: If True, `reduce_fn` is called whenever a
            partition's accumulator reaches `target_max_block_size`.  If
            False, wait for all shards before calling once (required for
            sort, stateful aggregation, or any multi-input reduce).  Forced
            to False when `disallow_block_splitting=True` or there is more
            than one input.
        disallow_block_splitting: If True, output blocks are emitted as-is
            without being reshaped to `target_max_block_size` — required
            for hash-shuffle's "partition = block" contract.
        reduce_cpus: CPU request per reduce task.  Defaults to 1.
        name: Display name shown in progress bars and logs.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: Union[ShuffleMapOp, List[ShuffleMapOp]],
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        streaming_reduce: bool = True,
        disallow_block_splitting: bool = False,
        reduce_cpus: Optional[float] = None,
        name: str = "ShuffleReduce",
    ):
        input_ops: List[ShuffleMapOp] = (
            [input_op] if isinstance(input_op, ShuffleMapOp) else list(input_op)
        )
        assert input_ops, "ShuffleReduceOp requires at least one upstream ShuffleMapOp"
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
        )

        self._num_inputs: int = len(input_ops)
        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting
        # Streaming requires partial-input reductions and a single input.  It is
        # forced off when block-splitting is disallowed (reducer must see the
        # whole partition before emitting) or when there are multiple inputs
        # (the reducer must see every input's shards together).
        self._streaming_reduce: bool = (
            streaming_reduce and not disallow_block_splitting and self._num_inputs == 1
        )

        # -- Reduce task config & tracking -----------------------------------
        self._shuffle_reduce_task_num_cpus: float = (
            reduce_cpus
            if reduce_cpus is not None
            else self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0

        # -- Per-partition pairing across inputs -----------------------------
        # partition_id -> input_index -> the single bundle that input emitted
        # for that partition.  A reduce task is submitted once all inputs have
        # delivered their bundle for a partition.
        self._pending_inputs: Dict[int, Dict[int, RefBundle]] = {}

        # -- Output queue ----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Stats -----------------------------------------------------------
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._reduce_bar: Optional["BaseProgressBar"] = None

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Buffer this input's partition-bundle; submit when all inputs paired.

        Each upstream bundle is a single partition's shards (M blocks from M
        mappers) from one input.  The partition_id is encoded in the first
        block's `input_files`.  A reduce task runs only once every input has
        delivered its bundle for that partition, so the reducer sees all inputs'
        shards together.  This is the framework-gated entry point — the executor
        only calls it when all configured backpressure policies say the op can
        accept another input.
        """
        assert 0 <= input_index < self._num_inputs

        if not refs.block_refs:
            refs.destroy_if_owned()
            return

        partition_id = extract_partition_id(refs)

        # Single-input fast path: an empty partition needs no reduce task; emit
        # one empty block built from the schema the map stage propagated.  For
        # multi-input reduces (e.g. outer joins) one empty side can still
        # produce rows, so we always run the reducer there.
        if self._num_inputs == 1:
            schema = refs.schema
            if isinstance(schema, pa.Schema) and not any(
                (m.num_rows or 0) for m in refs.metadata
            ):
                self._emit_empty_partition(refs, schema)
                return

        pending = self._pending_inputs.setdefault(partition_id, {})
        assert input_index not in pending, (
            f"input {input_index} already delivered a bundle for partition "
            f"{partition_id}; each ShuffleMapOp must emit at most one bundle "
            f"per partition"
        )
        pending[input_index] = refs
        if len(pending) == self._num_inputs:
            self._pending_inputs.pop(partition_id)
            self._submit_reduce_task(partition_id, pending)

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        # Every upstream input is now exhausted.  A partition still missing an
        # input's bundle will never receive it -- that input ran no map tasks
        # for this partition's key space (e.g. a block-less input).  Flush such
        # partitions with an empty placeholder for each missing input so the op
        # can complete instead of hanging on a never-paired partition.
        for partition_id in list(self._pending_inputs.keys()):
            pending = self._pending_inputs.pop(partition_id)
            for input_index in range(self._num_inputs):
                pending.setdefault(
                    input_index, RefBundle((), schema=None, owns_blocks=True)
                )
            self._submit_reduce_task(partition_id, pending)

    def _submit_reduce_task(
        self, partition_id: int, bundles_by_input: Dict[int, RefBundle]
    ) -> None:
        """Submit one reduce task for a fully-paired partition."""
        # Order bundles by input index so the reducer's `tables_by_input`
        # lines up with the upstream input order.
        ordered: List[RefBundle] = [
            bundles_by_input[i] for i in range(self._num_inputs)
        ]
        shard_refs_by_input = [list(b.block_refs) for b in ordered]
        estimated_bytes = sum((m.size_bytes or 0) for b in ordered for m in b.metadata)

        reduce_resources: Dict[str, Any] = {
            "num_cpus": self._shuffle_reduce_task_num_cpus,
        }
        if estimated_bytes > 0:
            reduce_resources["memory"] = int(estimated_bytes * 2)
        reduce_options = {
            **reduce_resources,
            "scheduling_strategy": "SPREAD",
            "num_returns": "streaming",
        }

        target_max_block_size = (
            None
            if self._disallow_block_splitting
            else self.data_context.target_max_block_size
        )
        block_gen = _shuffle_reduce_task.options(**reduce_options).remote(
            shard_refs_by_input,  # pyrefly: ignore[bad-argument-type]
            partition_id,
            self._reduce_fn,
            target_max_block_size,
            self._streaming_reduce,
            self.data_context.hash_shuffle_reduce_batch_size,
        )

        # The inputs may carry different schemas (e.g. left vs right of a join),
        # so we don't merge them into one bundle; we track the list for cleanup
        # and build a schema-less bundle purely for metrics accounting.
        metrics_bundle = RefBundle(
            tuple(
                BlockEntry(ref=ref, metadata=meta)
                for b in ordered
                for ref, meta in zip(b.block_refs, b.metadata)
            ),
            schema=None,
            owns_blocks=False,
        )

        data_task = DataOpTask(
            task_index=partition_id,
            streaming_gen=block_gen,
            output_ready_callback=functools.partial(
                self._handle_reduce_output_ready, partition_id
            ),
            task_done_callback=functools.partial(
                self._handle_reduce_done, partition_id, ordered
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                reduce_resources
            ),
            operator_name=self.name,
        )

        assert partition_id not in self._shuffle_reduce_tasks, (
            f"partition_id {partition_id} already has an in-flight reducer "
            f"task; ShuffleMapOp must emit at most one bundle per partition"
        )
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1
        self._metrics.on_task_submitted(
            partition_id, metrics_bundle, task_id=data_task.get_task_id()
        )

    def _emit_empty_partition(self, refs: RefBundle, schema: pa.Schema) -> None:
        """Emit one empty output block for an empty partition.

        The partition contributed no rows, so there is nothing to reduce; we
        build the empty block from the schema the map stage propagated onto
        the bundle and queue it as this partition's single output block.
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
        input_bundles: List[RefBundle],
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        """Callback when a reduce task finishes (with or without exception)."""
        for input_bundle in input_bundles:
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
        # For a single-input reduce (repartition/sort) the row count is
        # preserved, so we can forecast it from the map stage.  Multi-input
        # reduces (join) can grow or shrink the row count arbitrarily, so the
        # output size is unknown until the reducers run.
        if self._num_inputs != 1:
            return None
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOp)
        return upstream.num_output_rows_total()

    def _aggregate_partition_bytes(self) -> Dict[int, int]:
        """Sum each partition's bytes across all upstream map ops."""
        totals: Dict[int, int] = defaultdict(int)
        for upstream in self.input_dependencies:
            assert isinstance(upstream, ShuffleMapOp)
            for partition_id, nbytes in upstream.get_partition_bytes().items():
                totals[partition_id] += nbytes
        return totals

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
        partition_bytes = self._aggregate_partition_bytes()
        memory = 0
        sizes = [b for b in partition_bytes.values() if b > 0]
        if sizes:
            avg_bytes = sum(sizes) / len(sizes)
            memory = int(avg_bytes * 2)
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
