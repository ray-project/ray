import abc
import functools
import itertools
import logging
import math
from collections import defaultdict, deque
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.data import DataContext, ExecutionOptions, ExecutionResources
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.arrow_ops.transform_pyarrow import (
    create_empty_table,
    hash_partition,
)
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
)
from ray.data.block import Block, BlockAccessor, BlockMetadata

logger = logging.getLogger(__name__)


DEFAULT_SHUFFLE_AGGREGATOR_NUM_CPUS = 1.0
DEFAULT_SHUFFLE_AGGREGATOR_RAY_REMOTE_ARGS = {}


StatefulShuffleAggregationFactory = Callable[
    [int, List[int]], "StatefulShuffleAggregation"
]


class StatefulShuffleAggregation(abc.ABC):
    """Interface for a stateful aggregation to be used by hash-based shuffling
    operators (inheriting from `HashShufflingOperatorBase`) and subsequent
    aggregation of the dataset.

    Any stateful aggregation has to adhere to the following protocol:

        - Individual input sequence(s) will be (hash-)partitioned into N
        partitions each.

        - Accepting individual partition shards: for any given partition (identified
        by partition-id) of the input sequence (identified by input-id) aggregation
        will be receiving corresponding partition shards as the input sequence is
        being shuffled.

        - Upon completion of the shuffling (ie once whole sequence is shuffled)
        aggregation will receive a call to finalize the aggregation at which point
        it's expected to produce and return resulting block.
    """

    def __init__(self, aggregator_id: int):
        self._aggregator_id = aggregator_id

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        """Accepts corresponding partition shard for the partition identified by
        - Input sequence id
        - Partition id
        """
        raise NotImplementedError()

    def finalize(self, partition_id: int) -> Block:
        """Finalizes aggregation of partitions (identified by partition-id)
        from all input sequences returning resulting block.
        """

        raise NotImplementedError()


class Concat(StatefulShuffleAggregation):
    """Trivial aggregation recombining dataset's individual partition
    from the partition shards provided during shuffling stage. Returns
    single combined `Block` (Pyarrow `Table`)
    """

    def __init__(self, aggregator_id: int, target_partition_ids: List[int]):
        super().__init__(aggregator_id)

        # Block builders for individual partitions (identified by partition index)
        self._partition_block_builders: Dict[int, ArrowBlockBuilder] = {
            partition_id: ArrowBlockBuilder() for partition_id in target_partition_ids
        }

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        assert input_seq_id == 0, (
            f"Concat is unary stateful aggregation, got sequence "
            f"index of {input_seq_id}"
        )
        assert partition_id in self._partition_block_builders, (
            f"Received shard from unexpected partition '{partition_id}' "
            f"(expecting {self._partition_block_builders.keys()})"
        )

        self._partition_block_builders[partition_id].add_block(partition_shard)

    def finalize(self, partition_id: int) -> Block:
        return self._partition_block_builders[partition_id].build()


@ray.remote
def _shuffle_block(
    block: Block,
    input_index: int,
    key_columns: List[str],
    pool: "AggregatorPool",
    send_empty_blocks: bool = False,
) -> BlockMetadata:
    """Shuffles provided block following the algorithm:

    1. Hash-partitions provided block into N partitions (where N is determined by
       the number of receiving aggregators)

    2. Individual (non-empty) partitions are subsequently submitted to respective
       aggregators

    Args:
        block: Incoming block (in the form of Pyarrow's `Table`) to be shuffled
        input_index: Id of the input sequence block belongs to
        key_columns: Columns to be used by hash-partitioning algorithm
        pool: Hash-shuffling operator's pool of aggregators that are due to receive
              corresponding partitions (of the block)

    Returns:
        Metadata for the shuffled block
    """

    num_partitions = pool.num_partitions

    assert isinstance(block, pa.Table), f"Expected Pyarrow's `Table`, got {type(block)}"

    block_partitions = hash_partition(
        block, hash_cols=key_columns, num_partitions=num_partitions
    )

    awaitable_to_partition_map = {}

    for partition_id in range(num_partitions):
        partition_shard = block_partitions.get(partition_id)

        if partition_shard is None:
            # NOTE: Hash-based shuffle operator uses empty blocks to disseminate
            #       schema to aggregators that otherwise might not receive it,
            #       in cases when corresponding target partition is resulting into
            #       empty one during hash-partitioning
            if not send_empty_blocks:
                continue

            partition_shard = create_empty_table(block.schema)

        aggregator = pool.get_aggregator_for_partition(partition_id)
        # Put target partition shard into the Object Store to make sure partition shards
        # are managed t/h Object Store irrespective of their size
        partition_ref = ray.put(partition_shard)
        # NOTE: Shuffling task is only considered completed upon target aggregator
        #       accepting its respective partition shard
        awaitable = aggregator.submit.remote(input_index, partition_id, partition_ref)

        awaitable_to_partition_map[awaitable] = partition_id

    pending_submissions = list(awaitable_to_partition_map.keys())

    i = 0

    # Before completing shuffling task await for all the blocks
    # to get accepted by corresponding aggregators
    #
    # NOTE: This synchronization is crucial to make sure aggregations are not
    #       getting finalized before they receive corresponding partitions
    while len(pending_submissions) > 0:
        ready, unready = ray.wait(
            pending_submissions, num_returns=len(pending_submissions), timeout=1
        )

        # TODO clean up
        logger.debug(
            f">>> [DBG] ({i}) Submissions completed: "
            f"{[awaitable_to_partition_map[s] for s in ready]}"
        )
        logger.debug(
            f">>> [DBG] ({i}) Submissions pending: "
            f"{[awaitable_to_partition_map[s] for s in unready]}"
        )

        pending_submissions = unready

        i += 1

    # Return metadata for the original, shuffled block
    return BlockAccessor.for_block(block).get_metadata()


class HashShufflingOperatorBase(PhysicalOperator):
    """Physical operator base-class for any operators requiring hash-based
    shuffling.

    NOTE: This operator can perform hash-based shuffling for multiple sequences
          simultaneously (as required by Join operator for ex).
    """

    def __init__(
        self,
        name: str,
        input_ops: List[PhysicalOperator],
        data_context: DataContext,
        *,
        key_columns: List[Tuple[str]],
        num_partitions: int,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
        partition_aggregation_factory: StatefulShuffleAggregationFactory,
    ):
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
            target_max_block_size=None,
        )

        assert len(key_columns) == len(input_ops), (
            "Each input operation has to specify matching tuple of columns used as "
            "its hashing keys"
        )

        self._key_column_names: List[Tuple[str]] = key_columns
        self._num_partitions = num_partitions

        # Cap number of aggregators to not exceed max configured
        num_aggregators = min(
            num_partitions, data_context.get_current().max_hash_shuffle_aggregators
        )

        self._aggregator_pool: AggregatorPool = AggregatorPool(
            num_partitions=num_partitions,
            num_aggregators=num_aggregators,
            aggregation_factory=partition_aggregation_factory,
            aggregator_ray_remote_args=(
                aggregator_ray_remote_args_override
                or self._get_default_aggregator_ray_remote_args(
                    num_partitions=num_partitions,
                    num_aggregators=num_aggregators,
                )
            ),
        )

        self._shuffle_block_ray_remote_args = {
            "num_cpus": 1,
        }

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

        self._is_finalized: bool = False
        self._has_propagated_schemas: DefaultDict[int, bool] = defaultdict(bool)

        self._output_queue: Deque[RefBundle] = deque()

        self._output_blocks_metadata: List[BlockMetadata] = list()
        self._shuffling_blocks_metadata: List[BlockMetadata] = list()

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)

        self._aggregator_pool.start()

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        # TODO move to base class
        self._metrics.on_input_queued(input_bundle)
        try:
            self._do_add_input_inner(input_bundle, input_index)
        finally:
            self._metrics.on_input_dequeued(input_bundle)

    def _do_add_input_inner(self, input_bundle: RefBundle, input_index: int):
        input_blocks_refs: List[ObjectRef[Block]] = input_bundle.block_refs

        # TODO add concurrency control
        for block_ref in input_blocks_refs:
            # If operator hasn't propagated schemas (for this sequence) to its
            # aggregator pool, it will need to do that upon receiving of the
            # first block
            should_propagate_schemas = not self._has_propagated_schemas[input_index]
            input_key_column_names = self._key_column_names[input_index]

            # Fan out provided input blocks to "shuffle" it
            #   - Block is first hash-partitioned into N partitions
            #   - Individual partitions then are submitted to the corresponding
            #     aggregators
            shuffled_blocks_metadata_ref: ObjectRef[
                List[BlockMetadata]
            ] = _shuffle_block.options(
                **self._shuffle_block_ray_remote_args,
                num_returns=1,
            ).remote(
                block_ref,
                input_index,
                input_key_column_names,
                self._aggregator_pool,
                send_empty_blocks=should_propagate_schemas,
            )

            if should_propagate_schemas:
                self._has_propagated_schemas[input_index] = True

            cur_shuffle_task_idx = self._next_shuffle_tasks_idx
            self._next_shuffle_tasks_idx += 1

            def _on_partitioning_done():
                task = self._shuffling_tasks[input_index].pop(cur_shuffle_task_idx)
                # Fetch shuffled block metadata and add it to the list of processed
                # blocks
                block_metadata = ray.get(task.get_waitable(), timeout=0)
                self._shuffling_blocks_metadata.append(block_metadata)

            # TODO update metrics
            self._shuffling_tasks[input_index][cur_shuffle_task_idx] = MetadataOpTask(
                task_index=cur_shuffle_task_idx,
                object_ref=shuffled_blocks_metadata_ref,
                task_done_callback=_on_partitioning_done,
            )

    def has_next(self) -> bool:
        self._try_finalize()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()

        # TODO move to base class
        self._metrics.on_output_dequeued(bundle)

        self._output_blocks_metadata.extend(bundle.metadata)

        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        # Collect shuffling tasks for every input sequence
        shuffling_tasks = self._get_active_shuffling_tasks()

        # Collect aggregating tasks for every input sequence
        aggregating_tasks: List[DataOpTask] = list(self._aggregating_tasks.values())

        return shuffling_tasks + aggregating_tasks

    def _get_active_shuffling_tasks(self) -> List[MetadataOpTask]:
        return list(
            itertools.chain.from_iterable(
                [
                    input_shuffling_task_map.values()
                    for input_shuffling_task_map in self._shuffling_tasks.values()
                ]
            )
        )

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
            self._metrics.on_output_queued(bundle)

        def _on_aggregation_done(partition_id: int, exc: Optional[Exception]):
            if partition_id in self._aggregating_tasks:
                self._aggregating_tasks.pop(partition_id)

                if exc:
                    logger.error(
                        f"Aggregation of the {partition_id} partition "
                        f"failed with: {exc}",
                        exc_info=exc,
                    )

        for partition_id in range(self._num_partitions):
            aggregator = self._aggregator_pool.get_aggregator_for_partition(
                partition_id
            )
            # Finalize target partition
            block_gen = aggregator.finalize.remote(partition_id)

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

    def get_stats(self):
        return {
            # TODO factor in output blocks metadata
            self._name: self._shuffling_blocks_metadata,
        }

    def current_processor_usage(self) -> ExecutionResources:
        # Count both shuffle tasks and aggregator actors.
        base_usage = self.base_resource_usage()
        shuffling_tasks_cpus_used = len(
            self._get_active_shuffling_tasks()
        ) * self._shuffle_block_ray_remote_args.get("num_cpus")

        return ExecutionResources(
            cpu=base_usage.cpu + shuffling_tasks_cpus_used,
            gpu=0,
        )

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=(
                self._aggregator_pool.num_aggregators
                * self._aggregator_pool._aggregator_ray_remote_args["num_cpus"]
            ),
            object_store_memory=0,
            gpu=0,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            # TODO fix (this hack is currently to force Ray to spin up more tasks when
            #      shuffling to autoscale hardware capacity)
            cpu=0.01,
            # cpu=self._shuffle_block_ray_remote_args.get("num_cpus", 0),
            # TODO estimate (twice avg block size)
            object_store_memory=0,
            gpu=0,
        )

    @classmethod
    def _get_default_aggregator_ray_remote_args(
        cls, *, num_partitions: int, num_aggregators: int
    ):
        assert num_partitions >= num_aggregators

        # TODO elaborate
        partition_aggregator_ratio: int = math.ceil(num_partitions / num_aggregators)

        assert partition_aggregator_ratio >= 1

        remote_args = {
            **DEFAULT_SHUFFLE_AGGREGATOR_RAY_REMOTE_ARGS,
            "num_cpus": DEFAULT_SHUFFLE_AGGREGATOR_NUM_CPUS
            * partition_aggregator_ratio,
        }

        return remote_args


class ShuffleOperator(HashShufflingOperatorBase):
    def __init__(
        self,
        input_ops: List[PhysicalOperator],
        data_context: DataContext,
        *,
        key_columns: List[Tuple[str]],
        num_partitions: int,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=f"Shuffle(key_columns={key_columns}, num_partitions={num_partitions})",
            input_ops=input_ops,
            data_context=data_context,
            key_columns=key_columns,
            num_partitions=num_partitions,
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            partition_aggregation_factory=Concat.__init__,
        )


class AggregatorPool:
    def __init__(
        self,
        num_partitions: int,
        num_aggregators: int,
        aggregation_factory: StatefulShuffleAggregationFactory,
        aggregator_ray_remote_args: Dict[str, Any],
    ):
        assert (
            num_partitions >= 1
        ), f"Number of partitions has to be >= 1 (got {num_partitions})"

        self._num_partitions = num_partitions
        self._num_aggregators: int = num_aggregators
        self._aggregator_partition_map: Dict[
            int, List[int]
        ] = self._allocate_partitions(
            num_partitions=num_partitions,
        )

        self._aggregators: List[ray.ActorHandle] = []

        self._aggregation_factory_ref: ObjectRef[
            StatefulShuffleAggregationFactory
        ] = ray.put(aggregation_factory)
        self._aggregator_ray_remote_args: Dict[str, Any] = aggregator_ray_remote_args

        logger.debug(f"Aggregator's remote args: {aggregator_ray_remote_args}")

    def start(self):
        for aggregator_id in range(self._num_aggregators):
            target_partition_ids = self._aggregator_partition_map[aggregator_id]

            assert len(target_partition_ids) > 0

            aggregator = ShuffleAggregator.options(
                **self._aggregator_ray_remote_args
            ).remote(aggregator_id, target_partition_ids, self._aggregation_factory_ref)

            self._aggregators.append(aggregator)

    @property
    def num_partitions(self):
        return self._num_partitions

    @property
    def num_aggregators(self):
        return self._num_aggregators

    def get_aggregator_for_partition(self, partition_id: int) -> ActorHandle:
        return self._aggregators[self._get_aggregator_id_for_partition(partition_id)]

    def _allocate_partitions(self, *, num_partitions: int):
        assert num_partitions >= self._num_aggregators

        aggregator_to_partition_map: DefaultDict[int, List[int]] = defaultdict(list)

        for partition_id in range(num_partitions):
            aggregator_id = self._get_aggregator_id_for_partition(partition_id)
            aggregator_to_partition_map[aggregator_id].append(partition_id)

        return aggregator_to_partition_map

    def _get_aggregator_id_for_partition(self, partition_id: int) -> int:
        assert partition_id < self._num_partitions

        return partition_id % self._num_aggregators


@ray.remote
class ShuffleAggregator:
    def __init__(
        self,
        aggregator_id: int,
        target_partition_ids: List[int],
        agg_factory: StatefulShuffleAggregationFactory,
    ):
        self._agg: StatefulShuffleAggregation = agg_factory(
            aggregator_id, target_partition_ids
        )

    def submit(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        self._agg.accept(input_seq_id, partition_id, partition_shard)

    def finalize(
        self, partition_id: int
    ) -> AsyncGenerator[Union[Block, BlockMetadata], None]:
        result = self._agg.finalize(partition_id)

        # TODO break down blocks to target size
        yield result
        yield BlockAccessor.for_block(result).get_metadata()
