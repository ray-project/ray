import abc
import functools
import itertools
import logging
from collections import defaultdict, deque
from typing import (
    AsyncGenerator,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Union,
)

import pyarrow as pa

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.data import ExecutionOptions, ExecutionResources
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
)
from ray.data.block import Block, BlockAccessor, BlockMetadata

logger = logging.getLogger(__name__)


class StatefulAggregation(abc.ABC):
    """Interface for any stateful aggregation that wants to utilize
    `HashShuffleOperator` for hash-based shuffling and subsequent aggregation
    of the dataset
    """

    def accept(self, seq_index: int, shard: Block):
        raise NotImplementedError()

    def finalize(self) -> Block:
        raise NotImplementedError()


class Concat(StatefulAggregation):
    """Trivial aggregation recombining dataset's individual partition
    from the partition shards provided during shuffling stage. Returns
    single combined `Block` (Pyarrow `Table`)
    """

    def __init__(self):
        self._block_builder = ArrowBlockBuilder()

    def accept(self, seq_index: int, shard: Block):
        assert (
            seq_index == 0
        ), f"Concat is unary stateful aggregation, got sequence index of {seq_index}"

        self._block_builder.add_block(shard)

    def finalize(self) -> Block:
        return self._block_builder.build()


@ray.remote
def _shuffle_block(
    block: Block,
    input_index: int,
    key_columns: List[str],
    aggregators: List[ActorHandle],
) -> List[BlockMetadata]:
    """Shuffles provided block following the algorithm:

    1. Hash-partitions provided block into N partitions (where N is determined by
       the number of receiving aggregators)

    2. Individual (non-empty) partitions are subsequently submitted to respective
       aggregators

    Args:
        block: Incoming block (in the form of Pyarrow's `Table`) to be shuffled
        input_index: Id of the input sequence block belongs to
        key_columns: Columns to be used by hash-partitioning algorithm
        aggregators: `HashShuffleOperator`s aggregators that are due to receive
                     corresponding partitions (of the block)
    """

    num_partitions = len(aggregators)

    assert isinstance(block, pa.Table), f"Expected Pyarrow's `Table`, got {type(block)}"

    block_partitions = hash_partition(
        block, hash_cols=key_columns, num_partitions=num_partitions
    )

    awaitables = []
    metadata = []

    for partition_id, partition in block_partitions.items():
        assert partition.num_rows > 0, "Produced block has to be non-empty!"

        # TODO pass as object-ref?
        awaitable = aggregators[partition_id].submit.remote(input_index, partition)

        awaitables.append(awaitable)

        metadata.append(BlockAccessor.for_block(partition).get_metadata())

    # Before completing shuffling task await for all the blocks
    # to get accepted by corresponding aggregators
    ray.wait(awaitables, num_returns=len(awaitables))

    return metadata


# TODO for off sort-based shuffling
class HashShuffleOperator(PhysicalOperator):
    def __init__(
        self,
        input_ops: List[PhysicalOperator],
        *,
        key_columns: List[str],
        num_partitions: int,
        partition_aggregation_factory: Callable[
            [], StatefulAggregation
        ] = lambda: Concat(),
    ):
        super().__init__(
            name=(
                f"{self.__class__.__name__}"
                f"(key_columns={key_columns}, num_partitions={num_partitions})"
            ),
            input_dependencies=input_ops,
            target_max_block_size=None,
        )

        self._key_column_names: List[str] = key_columns
        self._num_partitions: int = num_partitions

        self._aggregator_pool: AggregatorPool = AggregatorPool(
            num_partitions, partition_aggregation_factory
        )

        self._next_shuffle_tasks_idx: int = 0
        # Shuffling tasks are mapped like following
        #   - Input sequence id -> Task id -> Task
        #
        # NOTE: Input sequences correspond to the outputs of the input operators
        self._shuffling_tasks: DefaultDict[
            int, Dict[int, MetadataOpTask]
        ] = defaultdict(dict)

        self._next_aggregate_task_idx: int = 0
        # Aggregating tasks are mapped like following
        #   - Task id -> Task
        #
        # NOTE: Aggregating tasks are invariant of the # of input operators, as
        #       aggregation is assumed to always produce a single sequence
        self._aggregating_tasks: Dict[int, DataOpTask] = dict()

        self._is_finalized = False

        self._output_queue: Deque[RefBundle] = deque()

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)

        self._aggregator_pool.start()

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        # TODO move to base class
        self._metrics.on_input_queued(input_bundle)

        input_blocks_refs: List[ObjectRef[Block]] = input_bundle.block_refs

        shuffle_task_ray_opts = {
            "num_returns": 1,
            "num_cpus": 1,
        }

        for block_ref in input_blocks_refs:
            # Fan out provided input block to "shuffle" it
            #   - Block is first hash-partitioned into N partitions
            #   - Individual partitions then are submitted to the corresponding
            #     aggregators
            partitioned_blocks_metadata_ref: ObjectRef[
                List[BlockMetadata]
            ] = _shuffle_block.options(**shuffle_task_ray_opts).remote(
                block_ref,
                input_index,
                self._key_column_names,
                self._aggregator_pool.aggregators,
            )

            cur_shuffle_task_idx = self._next_shuffle_tasks_idx
            self._next_shuffle_tasks_idx += 1

            def _on_partitioning_done():
                self._shuffling_tasks[input_index].pop(cur_shuffle_task_idx)

            self._shuffling_tasks[input_index][cur_shuffle_task_idx] = MetadataOpTask(
                task_index=cur_shuffle_task_idx,
                object_ref=partitioned_blocks_metadata_ref,
                task_done_callback=_on_partitioning_done,
            )

    def has_next(self) -> bool:
        self._try_finalize()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        return self._output_queue.popleft()

    def get_active_tasks(self) -> List[OpTask]:
        # Collect shuffling tasks for every input sequence
        shuffling_tasks: List[MetadataOpTask] = list(
            itertools.chain.from_iterable(
                [
                    input_shuffling_task_map.values()
                    for input_shuffling_task_map in self._shuffling_tasks.values()
                ]
            )
        )

        # Collect aggregating tasks for every input sequence
        aggregating_tasks: List[DataOpTask] = list(self._aggregating_tasks.values())

        return shuffling_tasks + aggregating_tasks

    def get_stats(self):
        # TODO impl
        return dict()

    def base_resource_usage(self) -> ExecutionResources:
        # TODO impl
        return ExecutionResources()

    def current_processor_usage(self) -> ExecutionResources:
        # TODO impls
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        # TODO impl
        return ExecutionResources()

    def _is_shuffling_done(self):
        return self._inputs_complete and all(
            [
                len(self._shuffling_tasks[input_seq_idx]) == 0
                for input_seq_idx in range(len(self._input_dependencies))
            ]
        )

    def _try_finalize(self):
        # Skip if already finalized
        if self._is_finalized:
            return

        # Finalization can only proceed once
        #   - All input sequences have been ingested
        #   - All outstanding shuffling tasks have completed
        if not self._is_shuffling_done():
            return

        logger.debug("Shuffling finished, finalization is in progress")

        def _on_bundle_ready(bundle: RefBundle):
            # Add finalized block to the output queue
            self._output_queue.append(bundle)

        def _on_aggregation_done(partition_id: int, exc: Optional[Exception]):
            if partition_id in self._aggregating_tasks:
                self._aggregating_tasks.pop(partition_id)

                if exc:
                    logger.error(
                        f"Aggregation of the {partition_id} partition "
                        f"failed with: {exc}",
                        exc_info=exc,
                    )

        for partition_id, aggregator in enumerate(self._aggregator_pool.aggregators):
            block_gen = aggregator.finalize.remote()

            self._aggregating_tasks[partition_id] = DataOpTask(
                task_index=partition_id,
                streaming_gen=block_gen,
                output_ready_callback=_on_bundle_ready,
                # TODO fix to pass in task_id into the callback
                task_done_callback=functools.partial(
                    _on_aggregation_done, partition_id
                ),
            )

        self._is_finalized = True


class AggregatorPool:
    def __init__(
        self,
        num_aggregators: int,
        aggregation_factory: Callable[[], StatefulAggregation],
    ):
        self._num_aggregators: int = num_aggregators
        self._aggregation_factory_ref: ObjectRef[
            Callable[[], StatefulAggregation]
        ] = ray.put(aggregation_factory)

        self._aggregators: List[ray.ActorHandle] = []

    def start(self):
        self._aggregators = [
            Aggregator.remote(self._aggregation_factory_ref)
            for _ in range(self._num_aggregators)
        ]

    @property
    def aggregators(self):
        return self._aggregators


@ray.remote
class Aggregator:
    def __init__(self, agg_factory: Callable[[], StatefulAggregation]):
        self._agg: StatefulAggregation = agg_factory()

    def submit(self, seq_index: int, partition_shard: Block):
        self._agg.accept(seq_index, partition_shard)

    def finalize(self) -> AsyncGenerator[Union[Block, BlockMetadata], None]:
        result = self._agg.finalize()

        # TODO break down blocks to target size
        yield result
        yield BlockAccessor.for_block(result).get_metadata()
