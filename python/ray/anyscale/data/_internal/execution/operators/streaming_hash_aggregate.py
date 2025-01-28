import logging
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.anyscale.data.api.streaming_aggregate import StreamingAggFn
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    PhysicalOperator,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


@ray.remote
class Aggregator:
    """Actor that performs the aggregation operations."""

    def __init__(
        self,
        idx: int,
        key: str,
        agg_fn: StreamingAggFn,
        output_block_size: int,
    ):
        """
        Args:
            idx: The index of this aggregator.
            key: The key to aggregate on.
            agg_fn: The aggregation function.
            output_block_size: The target block size for the output blocks.
        """
        self._idx = idx
        self._key = key
        self._agg_fn = agg_fn
        # Intermediate state for each key.
        self._states: Dict[str, Any] = {}
        self._output_buffer = BlockOutputBuffer(output_block_size)
        self._finalized = False

    def aggregate(self, should_finalize, should_checkpoint, *input_blocks: Block):
        """Perform aggregation on input blocks.

        Args:
            should_finalize: Whether to finalize this aggregator. This is set to true
                when there is no more input to process.
            should_checkpoint: Whether to checkpoint this aggregator.
            input_blocks: The input blocks to aggregate.
        """
        for block in input_blocks:
            accessor = BlockAccessor.for_block(block)
            for row in accessor.iter_rows(True):
                key = row[self._key]
                state = self._states.get(key)
                if state is None:
                    # Initialize the key state if the key is seen for the first time.
                    state = self._agg_fn.init_state(key)
                    self._states[key] = state
                state, finished = self._agg_fn.aggregate_row(key, state, row)
                if not finished:
                    self._states[key] = state
                else:
                    del self._states[key]
                    self._output_buffer.add(state)
                    # Yield output blocks from the output buffer.
                    while self._output_buffer.has_next():
                        b_out = self._output_buffer.next()
                        m_out = BlockAccessor.for_block(b_out).get_metadata()
                        yield b_out
                        yield m_out
        if should_finalize:
            assert not self._finalized
            self._finalized = True
            # Output all remaining data in the buffer when finalizing.
            self._output_buffer.finalize()
            if self._output_buffer.has_next():
                b_out = self._output_buffer.next()
                m_out = BlockAccessor.for_block(b_out).get_metadata()
                yield b_out
                yield m_out
        if should_checkpoint:
            # TODO(hchen): checkpoint states and self._output_buffer.
            pass


class AggregatorPool:
    """A pool of aggregator actors."""

    def __init__(
        self,
        key: str,
        agg_fn: StreamingAggFn,
        num_aggregators: int,
        actor_creation_ray_remote_args: Dict[str, Any],
    ):
        self._key = key
        self._agg_fn = agg_fn
        self._num_aggregators = num_aggregators
        self._actor_creation_ray_remote_args = actor_creation_ray_remote_args
        # TODO(hchen): probably should allow users to override the output block size,
        # because they may want to output something earlier, when the per-row
        # aggregation data is small.
        self._target_block_size = DataContext.get_current().target_max_block_size
        self._aggregators: List[ActorHandle] = []

    def start(self):
        self._aggregators = [self._start_actor(i) for i in range(self._num_aggregators)]

    def restart_actor(self, idx: int):
        self._aggregators[idx] = self._start_actor(idx)

    def _start_actor(self, idx: int):
        return Aggregator.options(**self._actor_creation_ray_remote_args).remote(
            idx,
            self._key,
            self._agg_fn,
            self._target_block_size,
        )

    def shutdown(self):
        for aggregator in self._aggregators:
            ray.kill(aggregator)
        self._aggregators = []

    def get_aggregator_actors(self) -> List[ActorHandle]:
        return self._aggregators

    def get_aggregator(self, idx: int) -> ActorHandle:
        return self._aggregators[idx]


@ray.remote
def shuffle_block(
    key: str,
    num_partitions: int,
    *input_blocks: Block,
) -> List[Union[Block, List[BlockMetadata]]]:
    """
    Partition input blocks by the key hash of each row.

    Args:
        key: The key based on which to split the input blocks.
        num_partitions: The number of partitions.
        input_blocks: The input blocks to shuffle.

    Returns:
        `num_partitions` blocks, followed by an additional list of all the
         block metadata.
    """
    block_builders = [DelegatingBlockBuilder() for _ in range(num_partitions)]
    for block in input_blocks:
        accessor = BlockAccessor.for_block(block)
        # TODO(hchen): Use a native function from Arrow or Pandas to split the block,
        # instead of iterating over rows.
        for row in accessor.iter_rows(True):
            # TODO(hchen): maybe use a better hash function, such as Murmur3.
            idx = hash(row[key]) % num_partitions
            block_builders[idx].add(row)
    out_blocks = [builder.build() for builder in block_builders]
    out_metadata = [BlockAccessor.for_block(b).get_metadata() for b in out_blocks]
    res = out_blocks
    res.append(out_metadata)
    return res


class StreamingHashAggregate(PhysicalOperator):
    """Physical operator that implements hash-based streaming aggregation."""

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        key: str,
        agg_fn: StreamingAggFn,
        num_aggregators: int,
    ):
        name = f"StreamingHashAgg(key={key}, num_aggregators={num_aggregators})"
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
            target_max_block_size=None,
        )
        self._key = key
        self._agg_fn = agg_fn
        self._num_aggregators = num_aggregators

        # TODO(hchen): should we expose these configurations?
        # Args for shuffle_block.
        self._shuffle_task_ray_remote_args = {
            "num_returns": 1 + num_aggregators,
            "num_cpus": 1.0,
            "max_retries": -1,
        }
        # Args for Aggregator actor creation.
        self._aggregator_creation_ray_remote_args = {
            "num_cpus": 1.0,
        }

        self._aggregator_pool = AggregatorPool(
            key,
            agg_fn,
            num_aggregators,
            self._aggregator_creation_ray_remote_args,
        )
        self._output_queue: Deque[RefBundle] = deque()

        # In-flight shuffle tasks, keyed by their task indexes.
        self._shuffle_tasks: Dict[int, MetadataOpTask] = {}
        self._shuffle_task_next_idx = 0

        # Pending inputs for each aggregator.
        self._pending_aggregate_inputs: List[Deque[RefBundle]] = [
            deque() for _ in range(num_aggregators)
        ]
        # In-flight aggregate tasks, keyed by their aggregator indexes.
        self._aggregate_tasks: Dict[int, DataOpTask] = {}
        self._aggregator_finalized = [False] * num_aggregators

    def start(self, options: ExecutionOptions) -> None:
        self._aggregator_pool.start()
        return super().start(options)

    def shutdown(self) -> None:
        self._aggregator_pool.shutdown()
        return super().shutdown()

    def current_processor_usage(self) -> ExecutionResources:
        # Count both shuffle tasks and aggregator actors.
        num_cpus = (
            self._num_aggregators
            * self._aggregator_creation_ray_remote_args["num_cpus"]
        )
        num_cpus += (
            len(self._shuffle_tasks) * self._shuffle_task_ray_remote_args["num_cpus"]
        )
        return ExecutionResources(
            cpu=num_cpus,
            gpu=0,
        )

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._num_aggregators
            * self._aggregator_creation_ray_remote_args["num_cpus"],
            gpu=0,
            object_store_memory=0,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._shuffle_task_ray_remote_args.get("num_cpus", 0),
            gpu=0,
            object_store_memory=0,
        )

    def _add_input_inner(self, input_refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, input_index
        self._metrics.on_input_queued(input_refs)

        obj_refs = shuffle_block.options(**self._shuffle_task_ray_remote_args,).remote(
            self._key,
            self._num_aggregators,
            *input_refs.block_refs,
        )
        block_refs: List[ObjectRef[Block]] = obj_refs[:-1]
        metadata_list_ref: ObjectRef[List[BlockMetadata]] = obj_refs[-1]

        task_idx = self._shuffle_task_next_idx
        self._shuffle_task_next_idx += 1

        def _on_task_complete():
            nonlocal input_refs, block_refs, metadata_list_ref
            self._shuffle_tasks.pop(task_idx)

            # Construct RefBundles.
            metadata_list = ray.get(metadata_list_ref)
            assert len(metadata_list) == self._num_aggregators, metadata_list
            ref_bundles = [
                RefBundle([(block_ref, metadata)], owns_blocks=True)
                for block_ref, metadata in zip(block_refs, metadata_list)
            ]

            # Dispatch blocks.
            for i, ref_bundle in enumerate(ref_bundles):
                if ref_bundle.num_rows() == 0:
                    continue
                self._metrics.on_input_queued(ref_bundle)
                self._pending_aggregate_inputs[i].append(ref_bundle)
                self._dispatch_aggregate_tasks(i)

            # Update metrics.
            input_refs.destroy_if_owned()
            self._metrics.on_input_dequeued(input_refs)

            self._maybe_finalize_all_aggregators()

        # TODO(hchen): we use `MetadataOpTask` here because `DataOpTask` requires
        # the task to return a streaming generator. We should rename them
        # to avoid confusion.
        self._shuffle_tasks[task_idx] = MetadataOpTask(
            task_idx,
            metadata_list_ref,
            _on_task_complete,
        )

    def _all_shuffle_tasks_done(self):
        return self._inputs_complete and len(self._shuffle_tasks) == 0

    def _maybe_finalize_all_aggregators(self):
        """Finalize all aggregators if all shuffle tasks are done."""
        if self._all_shuffle_tasks_done():
            for i in range(self._num_aggregators):
                self._dispatch_aggregate_tasks(i)

    def all_inputs_done(self):
        super().all_inputs_done()
        self._maybe_finalize_all_aggregators()

    def _dispatch_aggregate_tasks(self, idx):
        if self._aggregator_finalized[idx]:
            return
        if idx in self._aggregate_tasks:
            # TODO(hchen): allow multiple in-flight tasks for the same aggregator.
            return
        # If this is the last input, finalize the aggregation.
        should_finalize = (
            self._all_shuffle_tasks_done()
            and len(self._pending_aggregate_inputs[idx]) == 0
        )
        if should_finalize:
            logger.debug(f"Finalizing aggregator {idx}")
            self._aggregator_finalized[idx] = True

        empty_ref_bundle = RefBundle(blocks=tuple(), owns_blocks=True)
        try:
            # TODO(hchen): dispatch multiple inputs at once.
            input_ref_bundle = self._pending_aggregate_inputs[idx].popleft()
        except IndexError:
            if not should_finalize:
                return
            else:
                # When finalizing, we should still send a task to the aggregator
                # with no input to trigger finalization.
                input_ref_bundle = empty_ref_bundle
        # TODO(hchen): handle checkpointing.
        should_checkpoint = False
        agg_result_gen = self._aggregator_pool.get_aggregator(idx).aggregate.remote(
            should_finalize, should_checkpoint, *input_ref_bundle.block_refs
        )

        def _on_data_ready(ref_bundle: RefBundle):
            self._output_queue.append(ref_bundle)
            self._metrics.on_output_queued(ref_bundle)

        def _on_task_complete(error: Optional[Exception]):
            nonlocal idx, input_ref_bundle, should_finalize, should_checkpoint
            if error:
                # TODO(hchen): handle error.
                logger.error(f"Error in aggregate task {idx}: {error}")
            else:
                input_ref_bundle.destroy_if_owned()
                # Don't dequeue the empty ref bundle, because we never enqueued it in
                # the first place.
                if input_ref_bundle is not empty_ref_bundle:
                    self._metrics.on_input_dequeued(input_ref_bundle)
                self._aggregate_tasks.pop(idx)
                # Try to dispatch more inputs.
                self._dispatch_aggregate_tasks(idx)

        self._aggregate_tasks[idx] = DataOpTask(
            idx,
            agg_result_gen,
            _on_data_ready,
            _on_task_complete,
        )

    def get_stats(self) -> StatsDict:
        return {}

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        out = self._output_queue.popleft()
        self._metrics.on_output_dequeued(out)
        return out

    def get_active_tasks(self) -> List[OpTask]:
        res: List[OpTask] = []
        res.extend(self._aggregate_tasks.values())
        res.extend(self._shuffle_tasks.values())
        return res

    def progress_str(self) -> str:
        return (
            f"{len(self._shuffle_tasks)} shuffle tasks, "
            f"{len(self._aggregate_tasks)} aggregate tasks"
        )

    def implements_accurate_memory_accounting(self) -> bool:
        return True
