import time
from typing import Any, Dict, Generator, List, Optional, Union, cast

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

TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY = "top_n_max_candidate_blocks"
TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY = "top_n_max_candidate_rows"
TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY = "top_n_max_candidate_bytes"

DEFAULT_TOP_N_MAX_CANDIDATE_BLOCKS = 4_096
DEFAULT_TOP_N_MAX_CANDIDATE_ROWS = 2_000_000
DEFAULT_TOP_N_MAX_CANDIDATE_BYTES = 256 * 1024 * 1024


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
            entries = _check_candidate_budget(
                self._input_buffer.to_list(),
                self.data_context,
                self._candidate_summary,
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

            remote_args = self._merge_task_remote_args()
            block_gen = (
                cached_remote_fn(_final_top_n_task)
                .options(**remote_args)
                .remote(
                    [entry.ref for entry in entries],
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
        candidate_bytes = self._candidate_summary["bytes"]
        if candidate_bytes:
            # Concatenation and selection can hold both candidates and their copy.
            remote_args.setdefault("memory", 2 * candidate_bytes)
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
    candidate_refs: List[ray.ObjectRef],
    sort_key: SortKey,
    limit: int,
    output_schema: Optional[Schema],
    data_context: DataContext,
) -> Generator[Union[Block, bytes], None, None]:
    """Merge candidate blocks and yield the global Top-N with execution metadata."""
    start_time_s = time.perf_counter()
    stats = BlockExecStats.builder()
    blocks: List[Block] = ray.get(candidate_refs)
    if not blocks:
        raise ValueError("TopN final merge received no candidate blocks.")
    with DataContext.current(data_context):
        blocks = TableBlockAccessor.normalize_block_types(blocks)

        if output_schema is not None and all(
            BlockAccessor.for_block(block).num_rows() == 0 for block in blocks
        ):
            result = _empty_block_from_schema(output_schema)
        else:
            builder = DelegatingBlockBuilder()
            for block in blocks:
                builder.add_block(block)
            candidates = builder.build()
            result = BlockAccessor.for_block(candidates).top_n(sort_key, limit)

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


def _get_positive_config(context: DataContext, key: str, default: int) -> int:
    """Read a strictly positive integer configuration value from the context."""
    value = context.get_config(key, default)
    if type(value) is not int or value <= 0:
        raise ValueError(
            f"TopN config {key!r} must be a positive integer, got {value!r}."
        )
    return value


def _check_candidate_budget(
    input_bundles: List[RefBundle],
    data_context: DataContext,
    candidate_summary: Optional[Dict[str, int]] = None,
) -> List[BlockEntry]:
    """Validate that candidates fit within the final single-task merge budget."""
    entries = [entry for bundle in input_bundles for entry in bundle.blocks]
    max_blocks = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_BLOCKS,
    )
    max_rows = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_ROWS,
    )
    max_bytes = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_BYTES,
    )

    candidate_rows = 0
    candidate_bytes = 0
    for entry in entries:
        metadata = entry.metadata
        if metadata.num_rows is None or metadata.size_bytes is None:
            raise ValueError(
                "TopN candidate metadata must include num_rows and size_bytes "
                "before the final merge."
            )
        candidate_rows += metadata.num_rows
        candidate_bytes += metadata.size_bytes

    actual = {
        "blocks": len(entries),
        "rows": candidate_rows,
        "bytes": candidate_bytes,
    }
    if candidate_summary is not None:
        candidate_summary.update(actual)
    limits = {
        "blocks": max_blocks,
        "rows": max_rows,
        "bytes": max_bytes,
    }
    exceeded = [name for name in actual if actual[name] > limits[name]]
    if exceeded:
        # TODO(vicwen): Replace this fail-closed single-task budget with a
        # bounded hierarchical or distributed overflow path.
        raise ValueError(
            "TopN candidates exceed the experimental single-task merge budget: "
            f"actual={actual}, limits={limits}, exceeded={exceeded}."
        )

    return entries
