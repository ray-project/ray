import time
from typing import Any, Dict, Generator, List, Optional, Tuple, Union, cast

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


class TopNMergeOperator(InternalQueueOperatorMixin, PhysicalOperator):
    """Collect bounded candidates and asynchronously produce the global Top-N."""

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
        self._candidate_summary = {"blocks": 0, "rows": 0, "bytes": 0}
        self._merge_memory_bytes = 0
        self._input_buffer = FIFOBundleQueue()
        self._output_buffer = FIFOBundleQueue()
        self._merge_inputs: List[RefBundle] = []
        self._merge_task: Optional[DataOpTask] = None
        self._stats: StatsDict = {name: []}
        self._estimated_num_output_bundles = 1

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return [self._input_buffer]

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_buffer]

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        assert not self._inputs_complete
        self._input_buffer.add(refs)
        self._metrics.on_input_queued(refs, input_index=0)

    def all_inputs_done(self) -> None:
        """Seal candidate input and submit the bounded merge without waiting for it."""
        assert not self._inputs_complete
        super().all_inputs_done()
        try:
            entries = _collect_candidates(
                self._input_buffer.to_list(), self._candidate_summary
            )

            if not entries:
                # Preserve a known schema without launching a task for absent blocks.
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
                                cast(ObjectRef[Block], result_ref),
                                metadata.metadata,
                            ),
                        ),
                        owns_blocks=True,
                        schema=metadata.schema,
                    )
                    self._block_ref_counter.on_block_produced(
                        result_ref, output.size_bytes(), self.id
                    )
                    self._queue_output(output)
                return

            ref_batches, self._merge_memory_bytes = _batch_candidates(entries)
            remote_args = self._merge_task_remote_args()
            block_gen = (
                cached_remote_fn(_final_top_n_task)
                .options(**remote_args)
                .remote(
                    ref_batches,
                    self._sort_key,
                    self._limit,
                    self._output_schema,
                    self.data_context,
                )
            )
            # The executor consumes the generator and invokes these callbacks.
            self._merge_task = DataOpTask(
                task_index=0,
                streaming_gen=block_gen,
                block_ref_counter=self._block_ref_counter,
                producer_id=self.id,
                output_ready_callback=self._handle_output_ready,
                task_done_callback=self._handle_merge_done,
                task_resource_bundle=ExecutionResources.from_resource_dict(remote_args),
                operator_name=self.name,
            )
            # Retain original bundle ownership until the task finishes; the
            # combined bundle below is only for metrics and does not own blocks.
            while self._input_buffer.has_next():
                bundle = self._input_buffer.get_next()
                self._metrics.on_input_dequeued(bundle, input_index=0)
                self._merge_inputs.append(bundle)
            self._metrics.on_task_submitted(
                0,
                RefBundle(tuple(entries), owns_blocks=False, schema=None),
                task_id=self._merge_task.get_task_id(),
            )
        finally:
            if self._merge_task is None:
                # Empty input or submission failure leaves cleanup to this method.
                self.clear_internal_input_queue()

    def _merge_task_remote_args(self) -> Dict[str, Any]:
        remote_args = merge_label_selector(
            {"num_cpus": 1, **self._ray_remote_args},
            self.data_context.execution_options.label_selector,
        )
        if self._merge_memory_bytes:
            # Concatenation and selection can hold both a batch and its copy.
            remote_args.setdefault("memory", 2 * self._merge_memory_bytes)
        remote_args["name"] = self.name
        remote_args["num_returns"] = "streaming"
        return remote_args

    def _queue_output(self, bundle: RefBundle) -> None:
        self._output_buffer.add(bundle)
        self._metrics.on_output_queued(bundle)
        self._stats[self.name].extend(to_stats(bundle.metadata))
        self._estimated_output_num_rows = bundle.num_rows()

    def _handle_output_ready(self, bundle: RefBundle) -> None:
        self._queue_output(bundle)
        self._metrics.on_task_output_generated(0, bundle)

    def _handle_merge_done(
        self,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        self._merge_task = None
        self._metrics.on_task_finished(0, exc, task_exec_stats, task_exec_driver_stats)
        for bundle in self._merge_inputs:
            bundle.destroy_if_owned()
        self._merge_inputs.clear()

    def has_next(self) -> bool:
        return self._output_buffer.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return [self._merge_task] if self._merge_task is not None else []

    def get_stats(self) -> StatsDict:
        return self._stats

    def throttling_disabled(self) -> bool:
        # A barrier must drain upstream before it can release any candidate memory.
        return not self._inputs_complete

    def current_logical_usage(self) -> ExecutionResources:
        if self._merge_task is None:
            return ExecutionResources.zero()
        resources = self._merge_task.get_requested_resource_bundle()
        assert resources is not None
        return resources

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources.from_resource_dict(self._merge_task_remote_args())

    def per_task_resource_allocation(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def clear_internal_input_queue(self) -> None:
        while self._input_buffer.has_next():
            bundle = self._input_buffer.get_next()
            self._metrics.on_input_dequeued(bundle, input_index=0)
            bundle.destroy_if_owned()

    def clear_internal_output_queue(self) -> None:
        while self._output_buffer.has_next():
            bundle = self._output_buffer.get_next()
            self._metrics.on_output_dequeued(bundle)
            bundle.destroy_if_owned()

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        if self._merge_task is not None:
            self._merge_task.mark_aborted(
                TaskCancelledError(self._merge_task.get_task_id())
            )
        self.clear_internal_input_queue()
        self.clear_internal_output_queue()

    def _extra_metrics(self) -> Dict[str, Any]:
        return {
            "top_n_candidate_blocks": self._candidate_summary["blocks"],
            "top_n_candidate_rows": self._candidate_summary["rows"],
            "top_n_candidate_bytes": self._candidate_summary["bytes"],
        }


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
) -> Generator[Union[Block, bytes], None, None]:
    """Merge candidate batches into a running Top-N and yield the global result."""
    start_time_s = time.perf_counter()
    stats = BlockExecStats.builder()
    result: Optional[Block] = None
    with DataContext.current(data_context):
        for refs in candidate_ref_batches:
            blocks: List[Block] = ray.get(refs)
            if result is not None:
                blocks.append(result)
            blocks = TableBlockAccessor.normalize_block_types(blocks)
            builder = DelegatingBlockBuilder()
            for block in blocks:
                builder.add_block(block)
            result = BlockAccessor.for_block(builder.build()).top_n(sort_key, limit)

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
                task_idx=0,
            ),
            task_exec_stats=TaskExecWorkerStats(
                task_wall_time_s=time.perf_counter() - start_time_s,
            ),
        )

    yield from yield_block_with_stats(result, build_metadata)


def _collect_candidates(
    input_bundles: List[RefBundle],
    candidate_summary: Dict[str, int],
) -> List[BlockEntry]:
    """Return the candidate blocks and record their totals for metrics."""
    entries = [entry for bundle in input_bundles for entry in bundle.blocks]
    candidate_summary.update(
        blocks=len(entries),
        rows=sum(entry.metadata.num_rows or 0 for entry in entries),
        bytes=sum(entry.metadata.size_bytes or 0 for entry in entries),
    )
    return entries


def _batch_candidates(
    entries: List[BlockEntry],
) -> Tuple[List[List[ObjectRef[Block]]], int]:
    """Split candidates into batches of about `_MERGE_BATCH_BYTES`."""
    batches: List[List[ObjectRef[Block]]] = []
    batch_bytes = 0
    max_batch_bytes = 0
    max_block_bytes = 0
    for entry in entries:
        size_bytes = entry.metadata.size_bytes or 0
        if not batches or batch_bytes + size_bytes > _MERGE_BATCH_BYTES:
            batches.append([])
            batch_bytes = 0
        batches[-1].append(entry.ref)
        batch_bytes += size_bytes
        max_batch_bytes = max(max_batch_bytes, batch_bytes)
        max_block_bytes = max(max_block_bytes, size_bytes)
    return batches, max_batch_bytes + max_block_bytes
