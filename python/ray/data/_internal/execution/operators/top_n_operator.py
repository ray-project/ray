import time
from dataclasses import dataclass, field
from time import monotonic
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Union,
    cast,
)

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.bundle_queue import BaseBundleQueue, FIFOBundleQueue
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
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.util import (
    merge_label_selector,
    yield_block_with_stats,
)
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    Schema,
    TaskExecWorkerStats,
    to_stats,
)
from ray.data.context import DataContext
from ray.exceptions import TaskCancelledError
from ray.types import ObjectRef

# Candidates the final merge fetches at once. Peak merge memory is one batch
# plus the running top-N, independent of the total number of candidates.
_MERGE_BATCH_BYTES = 128 * 1024 * 1024
_MERGE_BATCH_BLOCKS = 32
_MERGE_BATCH_WAIT_S = 0.1
_MERGE_PEAK_MEMORY_MULTIPLIER = 3


@dataclass
class _MergeState:
    """Track the active merge task and its owned bundles.

    Only one merge task can run at a time. The output is committed
    only after the task succeeds.
    """

    task: Optional[DataOpTask] = None
    inputs: List[RefBundle] = field(default_factory=list)
    output: Optional[RefBundle] = None


class TopNMergeOperator(InternalQueueOperatorMixin, PhysicalOperator):
    """Incrementally merge candidate batches into a single global Top-N."""

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        sort_key: SortKey,
        limit: int,
        ray_remote_args: Dict[str, Any],
        output_schema: Optional[Schema],
        name: str,
    ) -> None:
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )
        self._sort_key = sort_key
        self._limit = limit
        self._ray_remote_args = dict(ray_remote_args)
        self._output_schema = output_schema
        self._input_buffer = FIFOBundleQueue()
        self._output_buffer = FIFOBundleQueue()
        self._pending_since: Optional[float] = None
        self._merge = _MergeState()
        self._next_task_index = 0
        self._stats: StatsDict = {name: []}
        self._estimated_num_output_bundles = 1

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return [self._input_buffer]

    def internal_input_queue_num_blocks(self) -> int:
        state = self._merge.output if self._merge.task is None else None
        return self._input_buffer.num_blocks() + (
            len(state.blocks) if state is not None else 0
        )

    def internal_input_queue_num_bytes(self) -> int:
        state = self._merge.output if self._merge.task is None else None
        return self._input_buffer.estimate_size_bytes() + (
            state.size_bytes() if state is not None else 0
        )

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_buffer]

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        assert not self._inputs_complete

        if not refs.blocks:
            refs.destroy_if_owned()
            return

        if not self._input_buffer.has_next():
            self._pending_since = monotonic()

        self._input_buffer.add(refs)
        self._metrics.on_input_queued(refs, input_index=0)
        self._maybe_submit_merge()

    def can_add_input(self) -> bool:
        # Admission can overshoot by one bundle, which is merged on its own.
        return (
            not self._inputs_complete
            and not self._is_execution_marked_finished
            and not self._shutdown
            and self._input_buffer.num_blocks() < _MERGE_BATCH_BLOCKS
            and self._input_buffer.estimate_size_bytes() < _MERGE_BATCH_BYTES
        )

    def all_inputs_done(self) -> None:
        assert not self._inputs_complete
        super().all_inputs_done()
        self._maybe_submit_merge()

    def _batch_ready(self) -> bool:
        if self._pending_since is None:
            return False
        return (
            self._inputs_complete
            or self._input_buffer.num_blocks() >= _MERGE_BATCH_BLOCKS
            or self._input_buffer.estimate_size_bytes() >= _MERGE_BATCH_BYTES
            or monotonic() - self._pending_since >= _MERGE_BATCH_WAIT_S
        )

    def _get_candidate_batch(self) -> List[RefBundle]:
        batch: List[RefBundle] = []
        num_blocks = size_bytes = 0

        for bundle in self._input_buffer:
            next_blocks = num_blocks + len(bundle.blocks)
            next_bytes = size_bytes + bundle.size_bytes()
            if batch and (
                next_blocks > _MERGE_BATCH_BLOCKS or next_bytes > _MERGE_BATCH_BYTES
            ):
                break

            batch.append(bundle)
            num_blocks, size_bytes = next_blocks, next_bytes

        return batch

    def _maybe_submit_merge(self) -> None:
        if (
            self._merge.task is not None
            or self._is_execution_marked_finished
            or self._shutdown
        ):
            return
        if not self._input_buffer.has_next():
            self._maybe_finish()
        elif self._batch_ready():
            self._submit_merge_task(self._get_candidate_batch())

    def _submit_merge_task(self, batch: List[RefBundle]) -> None:
        assert self._merge.task is None
        assert not self._merge.inputs
        assert batch

        if self._merge.output is not None:
            state = self._merge.output
            self._merge.output = None
            self._metrics.on_input_dequeued(state, input_index=0)
            self._merge.inputs.append(state)

        for _ in batch:
            bundle = self._input_buffer.get_next()
            self._metrics.on_input_dequeued(bundle, input_index=0)
            self._merge.inputs.append(bundle)

        if not self._input_buffer.has_next():
            self._pending_since = None

        try:
            entries = [
                entry for bundle in self._merge.inputs for entry in bundle.blocks
            ]
            batches = list(_batch_candidates(entries))
            remote_args = self._merge_task_remote_args(batches)
            task_index = self._next_task_index

            block_gen = (
                cached_remote_fn(_final_top_n_task)
                .options(**remote_args)
                .remote(
                    [[entry.ref for entry in batch] for batch in batches],
                    self._sort_key,
                    self._limit,
                    self._output_schema,
                    self.data_context,
                    task_index,
                )
            )

            self._merge.task = DataOpTask(
                task_index=task_index,
                streaming_gen=block_gen,
                block_ref_counter=self._block_ref_counter,
                producer_id=self.id,
                output_ready_callback=self._handle_output_ready,
                task_done_callback=self._handle_merge_done,
                task_resource_bundle=ExecutionResources.from_resource_dict(remote_args),
                operator_name=self.name,
            )
            self._metrics.on_task_submitted(
                task_index,
                RefBundle(tuple(entries), owns_blocks=False, schema=None),
                task_id=self._merge.task.get_task_id(),
            )
            self._next_task_index += 1

        except Exception:
            self.mark_execution_finished()
            self._clear_merge_buffers()
            raise

    def _maybe_finish(self) -> None:
        if self._is_execution_marked_finished or not self._inputs_complete:
            return

        if self._merge.output is not None:
            state = self._merge.output
            self._merge.output = None
            self._metrics.on_input_dequeued(state, input_index=0)
            self._queue_output(state)
        elif self._next_task_index == 0:
            self._estimated_output_num_rows = 0
            if self._output_schema is None:
                self._estimated_num_output_bundles = 0
            else:
                result = _empty_block_from_schema(self._output_schema)
                metadata = BlockMetadataWithSchema.from_block(result)
                result_ref = ray.put(result)
                output = RefBundle(
                    (
                        BlockEntry(
                            cast(ObjectRef[Block], result_ref), metadata.metadata
                        ),
                    ),
                    owns_blocks=True,
                    schema=metadata.schema,
                )
                self._block_ref_counter.on_block_produced(
                    result_ref, output.size_bytes(), self.id
                )
                self._queue_output(output)

        # Bypass the mixin's queue clearing so the final output can be drained.
        PhysicalOperator.mark_execution_finished(self)

    def _merge_task_remote_args(
        self, batches: Iterable[List[BlockEntry]] = ()
    ) -> Dict[str, Any]:
        remote_args = merge_label_selector(
            {"num_cpus": 1, **self._ray_remote_args},
            self.data_context.execution_options.label_selector,
        )
        peak_bytes = _estimate_merge_peak_bytes(batches, self._limit)
        if peak_bytes:
            # Reserve extra memory for concatenation and Top-N selection.
            remote_args.setdefault("memory", _MERGE_PEAK_MEMORY_MULTIPLIER * peak_bytes)

        remote_args["name"] = self.name
        remote_args["num_returns"] = "streaming"
        return remote_args

    def _queue_output(self, bundle: RefBundle) -> None:
        self._output_buffer.add(bundle)
        self._metrics.on_output_queued(bundle)
        self._estimated_output_num_rows = bundle.num_rows()

    def _handle_output_ready(self, bundle: RefBundle) -> None:
        if self._shutdown or self._is_execution_marked_finished:
            bundle.destroy_if_owned()
            return
        assert self._merge.task is not None
        assert self._merge.output is None
        self._merge.output = bundle
        self._stats[self.name].extend(to_stats(bundle.metadata))
        self._metrics.on_task_output_generated(self._merge.task.task_index(), bundle)

    def _handle_merge_done(
        self,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        assert self._merge.task is not None
        self._metrics.on_task_finished(
            self._merge.task.task_index(), exc, task_exec_stats, task_exec_driver_stats
        )
        for bundle in self._merge.inputs:
            bundle.destroy_if_owned()
        self._merge.inputs.clear()
        if exc is not None or self._shutdown or self._is_execution_marked_finished:
            self.mark_execution_finished()
            self._clear_merge_buffers()
            self._merge.task = None
            return
        assert self._merge.output is not None
        self._merge.task = None
        self._metrics.on_input_queued(self._merge.output, input_index=0)
        self._maybe_submit_merge()

    def refresh_state(self) -> None:
        # Flush partial batches even if upstream is waiting for them to be consumed.
        self._maybe_submit_merge()

    def has_next(self) -> bool:
        return self._output_buffer.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return [self._merge.task] if self._merge.task is not None else []

    def get_stats(self) -> StatsDict:
        return self._stats

    def current_logical_usage(self) -> ExecutionResources:
        if self._merge.task is None:
            return ExecutionResources.zero()

        resources = self._merge.task.get_requested_resource_bundle()
        assert resources is not None
        return resources

    def incremental_resource_usage(self) -> ExecutionResources:
        if self._merge.task is not None or not self._batch_ready():
            return ExecutionResources.zero()
        return self.per_task_resource_allocation()

    def per_task_resource_allocation(self) -> ExecutionResources:
        if self._merge.task is not None:
            return self.current_logical_usage()

        inputs = self._get_candidate_batch()
        if self._merge.output is not None:
            inputs = [self._merge.output, *inputs]

        return ExecutionResources.from_resource_dict(
            self._merge_task_remote_args(
                _batch_candidates(entry for bundle in inputs for entry in bundle.blocks)
            )
        )

    def clear_internal_input_queue(self) -> None:
        while self._input_buffer.has_next():
            bundle = self._input_buffer.get_next()
            self._metrics.on_input_dequeued(bundle, input_index=0)
            bundle.destroy_if_owned()

        self._pending_since = None
        if self._merge.task is None and self._merge.output is not None:
            self._metrics.on_input_dequeued(self._merge.output, input_index=0)
            self._merge.output.destroy_if_owned()
            self._merge.output = None

    def clear_internal_output_queue(self) -> None:
        while self._output_buffer.has_next():
            bundle = self._output_buffer.get_next()
            self._metrics.on_output_dequeued(bundle)
            bundle.destroy_if_owned()

    def _clear_merge_buffers(self) -> None:
        for bundle in self._merge.inputs:
            bundle.destroy_if_owned()
        self._merge.inputs.clear()

        if self._merge.output is not None:
            if self._merge.task is None:
                self._metrics.on_input_dequeued(self._merge.output, input_index=0)

            self._merge.output.destroy_if_owned()
            self._merge.output = None

    def _do_shutdown(self, force: bool = False) -> None:
        task = self._merge.task
        try:
            super()._do_shutdown(force)
        finally:
            try:
                if task is not None:
                    task.mark_aborted(TaskCancelledError(task.get_task_id()))
            finally:
                self._clear_merge_buffers()
                self.clear_internal_input_queue()
                self.clear_internal_output_queue()


def _empty_block_from_schema(schema: Schema) -> Block:
    """Create an empty Pandas or Arrow block that preserves ``schema``."""
    import pandas as pd
    import pyarrow as pa

    if isinstance(schema, PandasBlockSchema):
        return pd.DataFrame(
            {
                name: pd.Series(dtype=dtype)
                for name, dtype in zip(schema.names, schema.types)
            }
        )
    assert isinstance(schema, pa.Schema), type(schema)
    return schema.empty_table()


def _final_top_n_task(
    candidate_ref_batches: List[List[ray.ObjectRef]],
    sort_key: SortKey,
    limit: int,
    output_schema: Optional[Schema],
    data_context: DataContext,
    task_index: int = 0,
) -> Generator[Union[Block, bytes], None, None]:
    """Merge candidate batches into a running Top-N and yield the global result."""
    start_time_s = time.perf_counter()
    stats = BlockExecStats.builder()
    result: Optional[Block] = None

    with DataContext.current(data_context):
        for refs in candidate_ref_batches:
            # Batch temporaries must be released before fetching the next batch.
            result = _merge_top_n_blocks(ray.get(refs), result, sort_key, limit)

        if result is None:
            raise ValueError("TopN final merge received no candidate blocks.")
        if (
            output_schema is not None
            and BlockAccessor.for_block(result).num_rows() == 0
        ):
            result = _empty_block_from_schema(output_schema)

    stats.finish()

    def build_metadata(block_ser_time_s: Optional[float]) -> BlockMetadataWithSchema:
        return BlockMetadataWithSchema.from_block(
            result,
            block_exec_stats=stats.build(
                block_ser_time_s=block_ser_time_s,
                task_idx=task_index,
            ),
            task_exec_stats=TaskExecWorkerStats(
                task_wall_time_s=time.perf_counter() - start_time_s,
            ),
        )

    yield from yield_block_with_stats(result, build_metadata)


def _merge_top_n_blocks(
    blocks: List[Block],
    previous_result: Optional[Block],
    sort_key: SortKey,
    limit: int,
) -> Block:
    if previous_result is not None:
        blocks.append(previous_result)

    nonempty = [block for block in blocks if BlockAccessor.for_block(block).num_rows()]

    # Keep empty blocks when all inputs are empty to preserve the schema.
    if nonempty:
        blocks = nonempty
    if len(blocks) == 1:
        candidates = blocks[0]
    else:
        blocks = TableBlockAccessor.normalize_block_types(blocks)
        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        candidates = builder.build()

    return BlockAccessor.for_block(candidates).top_n(sort_key, limit)


def _batch_candidates(entries: Iterable[BlockEntry]) -> Iterator[List[BlockEntry]]:
    """Partition worker fetches; an oversized block forms its own batch."""
    batch: List[BlockEntry] = []
    batch_bytes = 0
    for entry in entries:
        size_bytes = entry.metadata.size_bytes or 0
        if batch and (
            len(batch) + 1 > _MERGE_BATCH_BLOCKS
            or batch_bytes + size_bytes > _MERGE_BATCH_BYTES
        ):
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(entry)
        batch_bytes += size_bytes

    if batch:
        yield batch


def _estimate_merge_peak_bytes(batches: Iterable[List[BlockEntry]], limit: int) -> int:
    """Estimate peak merge memory from fetched batches and the running Top-N."""
    peak_bytes = running_bytes = processed_bytes = max_row_bytes = 0
    unknown_rows = False

    for batch in batches:
        batch_bytes = sum(entry.metadata.size_bytes or 0 for entry in batch)
        peak_bytes = max(peak_bytes, batch_bytes + running_bytes)
        processed_bytes += batch_bytes

        for entry in batch:
            size_bytes = entry.metadata.size_bytes or 0
            num_rows = entry.metadata.num_rows
            if num_rows is None:
                unknown_rows = True
            elif num_rows:
                max_row_bytes = max(
                    max_row_bytes, (size_bytes + num_rows - 1) // num_rows
                )

        running_bytes = (
            processed_bytes
            if unknown_rows
            else min(processed_bytes, limit * max_row_bytes)
        )

    return peak_bytes
