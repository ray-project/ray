import abc
import functools
import itertools
import logging
import math
import random
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    DefaultDict,
    Deque,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import numpy as np
import pyarrow as pa

import ray
from ray import ObjectRef
from ray._private.ray_constants import (
    env_integer,
)
from ray.actor import ActorHandle
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.arrow_ops.transform_pyarrow import (
    _create_empty_table,
    hash_partition,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    _create_sub_pb,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.util import GiB, MiB
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockMetadataWithSchema,
    BlockStats,
    BlockType,
    to_stats,
)
from ray.data.context import (
    DEFAULT_MAX_HASH_SHUFFLE_AGGREGATORS,
    DEFAULT_TARGET_MAX_BLOCK_SIZE,
    DataContext,
)

logger = logging.getLogger(__name__)


BlockTransformer = Callable[[Block], Block]

StatefulShuffleAggregationFactory = Callable[
    [int, List[int]], "StatefulShuffleAggregation"
]


DEFAULT_HASH_SHUFFLE_AGGREGATOR_MAX_CONCURRENCY = env_integer(
    "RAY_DATA_DEFAULT_HASH_SHUFFLE_AGGREGATOR_MAX_CONCURRENCY", 8
)

DEFAULT_HASH_SHUFFLE_AGGREGATOR_MEMORY_ALLOCATION = env_integer(
    "RAY_DATA_DEFAULT_HASH_SHUFFLE_AGGREGATOR_MEMORY_ALLOCATION", 1 * GiB
)


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

        - After successful finalization aggregation's `clear` method will be invoked
         to clear any accumulated state to release resources.
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

    def clear(self, partition_id: int):
        """Clears out any accumulated state for provided partition-id.

        NOTE: This method is invoked after aggregation is finalized for the given
              partition."""
        raise NotImplementedError()


class Concat(StatefulShuffleAggregation):
    """Trivial aggregation recombining dataset's individual partition
    from the partition shards provided during shuffling stage. Returns
    single combined `Block` (Pyarrow `Table`)
    """

    def __init__(
        self,
        aggregator_id: int,
        target_partition_ids: List[int],
        *,
        should_sort: bool,
        key_columns: Optional[Tuple[str]] = None,
    ):
        super().__init__(aggregator_id)

        assert (
            not should_sort or key_columns
        ), f"Key columns have to be specified when `should_sort=True` (got {list(key_columns)})"

        self._should_sort = should_sort
        self._key_columns = key_columns

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
        block = self._partition_block_builders[partition_id].build()

        if self._should_sort and len(block) > 0:
            block = block.sort_by([(k, "ascending") for k in self._key_columns])

        return block

    def clear(self, partition_id: int):
        self._partition_block_builders.pop(partition_id)


@ray.remote
def _shuffle_block(
    block: Block,
    input_index: int,
    key_columns: List[str],
    pool: "AggregatorPool",
    block_transformer: Optional[BlockTransformer] = None,
    send_empty_blocks: bool = False,
    override_partition_id: Optional[int] = None,
) -> Tuple[BlockMetadata, Dict[int, "_PartitionStats"]]:
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
        send_empty_blocks: If set to true, empty blocks will NOT be filtered and
            still be fanned out to individual aggregators to distribute schemas
            (only known once we receive incoming block)
        override_partition_id: Target (overridden) partition id that input block will be
            assigned to
        block_transformer: Block transformer that will be applied to every block prior
            to shuffling

    Returns:
        A tuple of
            - Metadata for the block being shuffled
            - Map of partition ids to partition shard stats produced from the
            shuffled block
    """
    stats = BlockExecStats.builder()
    assert (len(key_columns) > 0) ^ (override_partition_id is not None), (
        f"Either list of key columns to hash-partition by (got {key_columns} or "
        f"target partition id override (got {override_partition_id}) must be provided!"
    )

    # Apply block transformer prior to shuffling (if any)
    if block_transformer is not None:
        block = block_transformer(block)

    # Make sure we're handling Arrow blocks
    block: Block = TableBlockAccessor.try_convert_block_type(
        block, block_type=BlockType.ARROW
    )

    if block.num_rows == 0:
        empty = BlockAccessor.for_block(block).get_metadata(exec_stats=stats.build())
        return (empty, {})

    num_partitions = pool.num_partitions

    assert isinstance(block, pa.Table), f"Expected Pyarrow's `Table`, got {type(block)}"

    # In case when no target key columns have been provided shuffling is
    # reduced to just forwarding whole block to the target aggregator
    if key_columns:
        block_partitions = hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )
    else:
        assert (
            0 <= override_partition_id < num_partitions
        ), f"Expected override partition id < {num_partitions} (got {override_partition_id})"

        block_partitions = {override_partition_id: block}

    partition_shards_stats = {}
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

            partition_shard = _create_empty_table(block.schema)

        # Capture partition shard metadata
        #
        # NOTE: We're skipping over empty shards as these are used for schema
        #       broadcasting and aren't relevant to keep track of
        if partition_shard.num_rows > 0:
            partition_shards_stats[partition_id] = _PartitionStats.for_table(
                partition_shard
            )

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

        pending_submissions = unready
        i += 1

    original_block_metadata = BlockAccessor.for_block(block).get_metadata(
        exec_stats=stats.build()
    )

    if logger.isEnabledFor(logging.DEBUG):
        num_rows_series, byte_sizes_series = zip(
            *[(s.num_rows, s.byte_size) for s in partition_shards_stats.values()]
        )

        quantiles = [0, 50, 100]
        num_rows_quantiles = np.percentile(num_rows_series, quantiles)
        byte_sizes_quantiles = np.percentile(byte_sizes_series, quantiles)

        logger.debug(
            f"Shuffled block (rows={original_block_metadata.num_rows}, "
            f"bytes={original_block_metadata.size_bytes/MiB:.1f}MB) "
            f"into {len(partition_shards_stats)} partitions ("
            f"quantiles={'/'.join(map(str, quantiles))}, "
            f"rows={'/'.join(map(str, num_rows_quantiles))}, "
            f"bytes={'/'.join(map(str, byte_sizes_quantiles))})"
        )

    # Return metadata for the original, shuffled block
    return original_block_metadata, partition_shards_stats


@dataclass
class _PartitionStats:
    num_rows: int
    byte_size: int

    @staticmethod
    def from_block_metadata(block_metadata: BlockMetadata) -> "_PartitionStats":
        return _PartitionStats(
            num_rows=block_metadata.num_rows,
            byte_size=block_metadata.size_bytes,
        )

    @staticmethod
    def for_table(table: pa.Table) -> "_PartitionStats":
        return _PartitionStats(
            num_rows=table.num_rows,
            byte_size=table.nbytes,
        )

    @staticmethod
    def combine(one: "_PartitionStats", other: "_PartitionStats") -> "_PartitionStats":
        return _PartitionStats(
            num_rows=one.num_rows + other.num_rows,
            byte_size=one.byte_size + other.byte_size,
        )


class HashShuffleProgressBarMixin(SubProgressBarMixin):
    @property
    @abc.abstractmethod
    def shuffle_name(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def reduce_name(self) -> str:
        ...

    def _validate_sub_progress_bar_names(self):
        assert self.shuffle_name is not None, "shuffle_name should not be None"
        assert self.reduce_name is not None, "reduce_name should not be None"

    def initialize_sub_progress_bars(self, position: int) -> int:
        """Display all sub progress bars in the termainl, and return the number of bars."""
        self._validate_sub_progress_bar_names()

        # shuffle
        progress_bars_created = 0
        self.shuffle_bar = None
        self.shuffle_bar, position = _create_sub_pb(
            self.shuffle_name, self.num_output_rows_total(), position
        )
        progress_bars_created += 1
        self.shuffle_metrics = OpRuntimeMetrics(self)

        # reduce
        self.reduce_bar = None
        self.reduce_bar, position = _create_sub_pb(
            self.reduce_name, self.num_output_rows_total(), position
        )
        progress_bars_created += 1
        self.reduce_metrics = OpRuntimeMetrics(self)

        return progress_bars_created

    def close_sub_progress_bars(self):
        """Close all internal sub progress bars."""
        self.shuffle_bar.close()
        self.reduce_bar.close()

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        self._validate_sub_progress_bar_names()

        # shuffle
        self.shuffle_bar = None
        self.shuffle_metrics = OpRuntimeMetrics(self)

        # reduce
        self.reduce_bar = None
        self.reduce_metrics = OpRuntimeMetrics(self)

        return [self.shuffle_name, self.reduce_name]

    def set_sub_progress_bar(self, name, pg):
        # No type-hints due to circular imports. `name` should be a `str`
        # and `pg` should be a `SubProgressBar`
        if self.shuffle_name is not None and self.shuffle_name == name:
            self.shuffle_bar = pg
        elif self.reduce_name is not None and self.reduce_name == name:
            self.reduce_bar = pg


def _derive_max_shuffle_aggregators(
    total_cluster_resources: ExecutionResources,
    data_context: DataContext,
) -> int:
    # Motivation for derivation of max # of shuffle aggregators is based on the
    # following observations:
    #
    #   - Shuffle operation is necessarily a terminal operation: it terminates current
    #     shuffle stage (set of operators that can execute concurrently)
    #   - Shuffle operation has very low computation footprint until all preceding
    #     operation completes (ie until shuffle finalization)
    #   - When shuffle is finalized only shuffle operator is executing (ie it has
    #     all of the cluster resources available at its disposal)
    #
    # As such we establish that the max number of shuffle
    # aggregators (workers):
    #
    #   - Should not exceed total # of CPUs (to fully utilize cluster resources
    #   while avoiding thrashing these due to over-allocation)
    #   - Should be capped at fixed size (128 by default)
    return min(
        math.ceil(total_cluster_resources.cpu),
        data_context.max_hash_shuffle_aggregators
        or DEFAULT_MAX_HASH_SHUFFLE_AGGREGATORS,
    )


class HashShufflingOperatorBase(PhysicalOperator, HashShuffleProgressBarMixin):
    """Physical operator base-class for any operators requiring hash-based
    shuffling.

    Hash-based shuffling follows standard map-reduce architecture:

        1. Every incoming block is mapped (using provided `input_block_transformer`,
           if any)

        2. After mapping, every block is hash-partitioned into `num_partitions`
           partitions and distributed (shuffled) to corresponding aggregators.

        3. Aggregators perform "reducing" stage, aggregating individual partitions
           (using configured `StatefulAggregation`), and ultimately yield resutling
           blocks.

    NOTE: This operator can perform hash-based shuffling for multiple sequences
          simultaneously (as required by Join operator for ex).
    """

    def __init__(
        self,
        name_factory: Callable[[int], str],
        input_ops: List[PhysicalOperator],
        data_context: DataContext,
        *,
        key_columns: List[Tuple[str]],
        partition_aggregation_factory: StatefulShuffleAggregationFactory,
        num_partitions: Optional[int] = None,
        partition_size_hint: Optional[int] = None,
        input_block_transformer: Optional[BlockTransformer] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
        shuffle_progress_bar_name: Optional[str] = None,
        finalize_progress_bar_name: Optional[str] = None,
        disallow_block_splitting: bool = False,
    ):
        input_logical_ops = [
            input_physical_op._logical_operators[0] for input_physical_op in input_ops
        ]

        estimated_input_blocks = [
            input_op.estimated_num_outputs() for input_op in input_logical_ops
        ]

        # Derive target num partitions as either of
        #   - Requested target number of partitions
        #   - Max estimated target number of blocks generated by the input op(s)
        #   - Default configured hash-shuffle parallelism (200)
        target_num_partitions: int = (
            num_partitions
            or (max(estimated_input_blocks) if all(estimated_input_blocks) else None)
            or data_context.default_hash_shuffle_parallelism
        )

        super().__init__(
            name=name_factory(target_num_partitions),
            input_dependencies=input_ops,
            data_context=data_context,
        )

        assert partition_size_hint is None or partition_size_hint > 0

        if shuffle_progress_bar_name is None:
            shuffle_progress_bar_name = "Shuffle"
        if finalize_progress_bar_name is None:
            finalize_progress_bar_name = "Reduce"

        self._shuffle_name = shuffle_progress_bar_name
        self._reduce_name = finalize_progress_bar_name

        assert len(key_columns) == len(input_ops), (
            "Each input operation has to specify matching tuple of columns used as "
            "its hashing keys"
        )

        self._key_column_names: List[Tuple[str]] = key_columns
        self._num_partitions: int = target_num_partitions

        # Determine max number of shuffle aggregators (defaults to
        # `DataContext.min_parallelism`)
        total_available_cluster_resources = _get_total_cluster_resources()
        max_shuffle_aggregators = _derive_max_shuffle_aggregators(
            total_available_cluster_resources, data_context
        )

        # Cap number of aggregators to not exceed max configured
        num_aggregators = min(target_num_partitions, max_shuffle_aggregators)

        # Target dataset's size estimated as either of
        #   1. ``partition_size_hint`` multiplied by target number of partitions
        #   2. Estimation of input ops' outputs bytes
        if partition_size_hint is not None:
            # TODO replace with dataset-byte-size hint
            estimated_dataset_bytes = partition_size_hint * target_num_partitions
        else:
            estimated_dataset_bytes = _try_estimate_output_bytes(
                input_logical_ops,
            )

        ray_remote_args = self._get_default_aggregator_ray_remote_args(
            num_partitions=target_num_partitions,
            num_aggregators=num_aggregators,
            total_available_cluster_resources=total_available_cluster_resources,
            estimated_dataset_bytes=estimated_dataset_bytes,
        )

        if aggregator_ray_remote_args_override is not None:
            # Set default values missing for configs missing in the override
            ray_remote_args.update(aggregator_ray_remote_args_override)

        self._aggregator_pool: AggregatorPool = AggregatorPool(
            num_partitions=target_num_partitions,
            num_aggregators=num_aggregators,
            aggregation_factory=partition_aggregation_factory,
            aggregator_ray_remote_args=ray_remote_args,
            target_max_block_size=None
            if disallow_block_splitting
            else data_context.target_max_block_size,
        )

        # We track the running usage total because iterating
        # and summing over all shuffling tasks can be expensive
        # if the # of shuffling tasks is large
        self._shuffling_resource_usage = ExecutionResources.zero()

        self._input_block_transformer = input_block_transformer

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
        self._finalizing_tasks: Dict[int, DataOpTask] = dict()

        # This is a workaround to be able to distribute schemas to individual
        # aggregators (keeps track which input sequences have already broadcasted
        # their schemas)
        self._has_schemas_broadcasted: DefaultDict[int, bool] = defaultdict(bool)
        # Set of partitions still pending finalization
        self._pending_finalization_partition_ids: Set[int] = set(
            range(target_num_partitions)
        )

        self._output_queue: Deque[RefBundle] = deque()

        self._output_blocks_stats: List[BlockStats] = list()
        self._shuffled_blocks_stats: List[BlockStats] = list()

        # Incremental individual partition metadata accumulated separately for
        # individual input sequences during shuffling. Maps
        #
        #   input_sequence_id -> partition_id -> _PartitionStats
        #
        self._partitions_stats: DefaultDict[
            int, Dict[int, _PartitionStats]
        ] = defaultdict(dict)

        self._health_monitoring_started: bool = False
        self._health_monitoring_start_time: float = 0.0
        self._pending_aggregators_refs: Optional[List[ObjectRef[ActorHandle]]] = None

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)

        self._aggregator_pool.start()

    @property
    def shuffle_name(self) -> str:
        return self._shuffle_name

    @property
    def reduce_name(self) -> str:
        return self._reduce_name

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:

        # TODO move to base class
        self.shuffle_metrics.on_input_received(input_bundle)
        self._do_add_input_inner(input_bundle, input_index)

    def _do_add_input_inner(self, input_bundle: RefBundle, input_index: int):
        input_blocks_refs: List[ObjectRef[Block]] = input_bundle.block_refs
        input_blocks_metadata: List[BlockMetadata] = input_bundle.metadata

        for block_ref, block_metadata in zip(input_blocks_refs, input_blocks_metadata):
            # If operator hasn't propagated schemas (for this sequence) to its
            # aggregator pool, it will need to do that upon receiving of the
            # first block
            should_broadcast_schemas = not self._has_schemas_broadcasted[input_index]
            input_key_column_names = self._key_column_names[input_index]
            # Compose shuffling task resource bundle
            shuffle_task_resource_bundle = {
                "num_cpus": 0.5,
                "memory": self._estimate_shuffling_memory_req(
                    block_metadata,
                    target_max_block_size=(
                        self._data_context.target_max_block_size
                        or DEFAULT_TARGET_MAX_BLOCK_SIZE
                    ),
                ),
            }

            cur_shuffle_task_idx = self._next_shuffle_tasks_idx
            self._next_shuffle_tasks_idx += 1

            # NOTE: In cases when NO key-columns are provided for hash-partitioning
            #       to be performed on (legitimate scenario for global aggregations),
            #       shuffling is essentially reduced to round-robin'ing of the blocks
            #       among the aggregators
            override_partition_id = (
                cur_shuffle_task_idx % self._num_partitions
                if not input_key_column_names
                else None
            )

            # Fan out provided input blocks to "shuffle" it
            #   - Block is first hash-partitioned into N partitions
            #   - Individual partitions then are submitted to the corresponding
            #     aggregators
            input_block_partition_shards_metadata_tuple_ref: ObjectRef[
                Tuple[BlockMetadata, Dict[int, _PartitionStats]]
            ] = _shuffle_block.options(
                **shuffle_task_resource_bundle,
                num_returns=1,
                # Make sure tasks are retried indefinitely
                max_retries=-1,
            ).remote(
                block_ref,
                input_index,
                input_key_column_names,
                self._aggregator_pool,
                block_transformer=self._input_block_transformer,
                send_empty_blocks=should_broadcast_schemas,
                override_partition_id=override_partition_id,
            )

            if should_broadcast_schemas:
                self._has_schemas_broadcasted[input_index] = True

            def _on_partitioning_done(cur_shuffle_task_idx: int):
                task = self._shuffling_tasks[input_index].pop(cur_shuffle_task_idx)
                self._shuffling_resource_usage = (
                    self._shuffling_resource_usage.subtract(
                        task.get_requested_resource_bundle()
                    )
                )
                # Fetch input block and resulting partition shards block metadata and
                # handle obtained metadata
                #
                # NOTE: We set timeout equal to 1m here as an upper-bound to make
                #       sure that `ray.get(...)` invocation couldn't stall the pipeline
                #       indefinitely
                input_block_metadata, partition_shards_stats = ray.get(
                    task.get_waitable(), timeout=60
                )

                self._handle_shuffled_block_metadata(
                    input_index, input_block_metadata, partition_shards_stats
                )

                # Update Shuffle metrics on task output generated
                blocks = [(task.get_waitable(), input_block_metadata)]
                # NOTE: schema doesn't matter because we are creating a ref bundle
                # for metrics recording purposes
                out_bundle = RefBundle(blocks, schema=None, owns_blocks=False)
                self.shuffle_metrics.on_output_taken(input_bundle)
                self.shuffle_metrics.on_task_output_generated(
                    cur_shuffle_task_idx, out_bundle
                )
                self.shuffle_metrics.on_task_finished(cur_shuffle_task_idx, None)

                # Update Shuffle progress bar
                self.shuffle_bar.update(increment=input_block_metadata.num_rows or 0)

            # TODO update metrics
            task = self._shuffling_tasks[input_index][
                cur_shuffle_task_idx
            ] = MetadataOpTask(
                task_index=cur_shuffle_task_idx,
                object_ref=input_block_partition_shards_metadata_tuple_ref,
                task_done_callback=functools.partial(
                    _on_partitioning_done, cur_shuffle_task_idx
                ),
                task_resource_bundle=ExecutionResources.from_resource_dict(
                    shuffle_task_resource_bundle
                ),
            )
            if task.get_requested_resource_bundle() is not None:
                self._shuffling_resource_usage = self._shuffling_resource_usage.add(
                    task.get_requested_resource_bundle()
                )

            #  Update Shuffle Metrics on task submission
            self.shuffle_metrics.on_task_submitted(
                cur_shuffle_task_idx,
                RefBundle(
                    [(block_ref, block_metadata)], schema=None, owns_blocks=False
                ),
            )

            # Update Shuffle progress bar
            _, _, num_rows = estimate_total_num_of_blocks(
                cur_shuffle_task_idx + 1,
                self.upstream_op_num_outputs(),
                self.shuffle_metrics,
                total_num_tasks=None,
            )
            self.shuffle_bar.update(total=num_rows)

    def has_next(self) -> bool:
        self._try_finalize()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()

        # TODO move to base class
        self.reduce_metrics.on_output_dequeued(bundle)
        self.reduce_metrics.on_output_taken(bundle)

        self._output_blocks_stats.extend(to_stats(bundle.metadata))

        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        # Collect shuffling tasks for every input sequence
        shuffling_tasks = self._get_active_shuffling_tasks()

        # Collect aggregating tasks for every input sequence
        finalizing_tasks: List[DataOpTask] = list(self._finalizing_tasks.values())

        return shuffling_tasks + finalizing_tasks

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
        # Skip if finalization of all partitions had been already scheduled
        if self._is_finalized():
            return

        # Finalization can only proceed once
        #   - All input sequences have been ingested
        #   - All outstanding shuffling tasks have completed
        if not self._is_shuffling_done():
            return

        def _on_bundle_ready(partition_id: int, bundle: RefBundle):
            # Add finalized block to the output queue
            self._output_queue.append(bundle)

            # Update Finalize Metrics on task output generated
            self.reduce_metrics.on_output_queued(bundle)
            self.reduce_metrics.on_task_output_generated(
                task_index=partition_id, output=bundle
            )
            _, num_outputs, num_rows = estimate_total_num_of_blocks(
                partition_id + 1,
                self.upstream_op_num_outputs(),
                self.reduce_metrics,
                total_num_tasks=self._num_partitions,
            )
            self._estimated_num_output_bundles = num_outputs
            self._estimated_output_num_rows = num_rows

            # Update Finalize progress bar
            self.reduce_bar.update(
                increment=bundle.num_rows() or 0, total=self.num_output_rows_total()
            )

        def _on_aggregation_done(partition_id: int, exc: Optional[Exception]):
            if partition_id in self._finalizing_tasks:
                self._finalizing_tasks.pop(partition_id)

                # Update Finalize Metrics on task completion
                self.reduce_metrics.on_task_finished(
                    task_index=partition_id, exception=exc
                )

                if exc:
                    logger.error(
                        f"Aggregation of the {partition_id} partition "
                        f"failed with: {exc}",
                        exc_info=exc,
                    )

        # NOTE: Unless explicitly set finalization batch size defaults to the #
        #       of shuffle aggregators
        max_batch_size = (
            self.data_context.max_hash_shuffle_finalization_batch_size
            or self._aggregator_pool.num_aggregators
        )

        num_running_finalizing_tasks = len(self._finalizing_tasks)
        num_remaining_partitions = len(self._pending_finalization_partition_ids)

        # Finalization is executed in batches of no more than
        # `DataContext.max_hash_shuffle_finalization_batch_size` tasks at a time.
        #
        # Batch size is used as a lever to limit memory pressure on the nodes
        # where aggregators are run by limiting # of finalization tasks running
        # concurrently
        next_batch_size = min(
            num_remaining_partitions,
            max_batch_size - num_running_finalizing_tasks,
        )

        assert next_batch_size >= 0, (
            f"Finalization batch size must be greater than 0 "
            f"(got {next_batch_size}; "
            f"remaining={num_remaining_partitions}, "
            f"finalizing={num_running_finalizing_tasks}, "
            f"max_batch_size={max_batch_size})"
        )

        if next_batch_size == 0:
            return

        # We're sampling randomly next set of partitions to be finalized
        # to distribute finalization window uniformly across the nodes of the cluster
        # and avoid effect of "sliding lense" effect where we finalize the batch of
        # N *adjacent* partitions that may be co-located on the same node:
        #
        #   - Adjacent partitions i and i+1 are handled by adjacent
        #   aggregators (since membership is determined as i % num_aggregators)
        #
        #   - Adjacent aggregators have high likelihood of running on the
        #   same node (when num aggregators > num nodes)
        #
        # NOTE: This doesn't affect determinism, since this only impacts order
        #       of finalization (hence not required to be seeded)
        target_partition_ids = random.sample(
            list(self._pending_finalization_partition_ids), next_batch_size
        )

        logger.debug(
            f"Scheduling partitions {target_partition_ids} for finalization: "
            f"{[self._get_partition_stats(pid) for pid in target_partition_ids]}"
        )

        for partition_id in target_partition_ids:
            aggregator = self._aggregator_pool.get_aggregator_for_partition(
                partition_id
            )

            # Estimate (heap) memory requirement to execute finalization task
            # Compose shuffling task resource bundle
            finalize_task_resource_bundle = {
                # TODO currently not possible to specify the resources for an actor
                # "memory": self._estimate_finalization_memory_req(partition_id),
            }

            # Request finalization of the partition
            block_gen = aggregator.finalize.options(
                **finalize_task_resource_bundle,
            ).remote(partition_id)

            self._finalizing_tasks[partition_id] = DataOpTask(
                task_index=partition_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(_on_bundle_ready, partition_id),
                task_done_callback=functools.partial(
                    _on_aggregation_done, partition_id
                ),
                task_resource_bundle=(
                    ExecutionResources.from_resource_dict(finalize_task_resource_bundle)
                ),
            )

            # Pop partition id from remaining set
            self._pending_finalization_partition_ids.remove(partition_id)

            # Update Finalize Metrics on task submission
            # NOTE: This is empty because the input is directly forwarded from the
            # output of the shuffling stage, which we don't return.
            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self.reduce_metrics.on_task_submitted(partition_id, empty_bundle)

    def _do_shutdown(self, force: bool = False) -> None:
        self._aggregator_pool.shutdown(force=True)
        # NOTE: It's critical for Actor Pool to release actors before calling into
        #       the base method that will attempt to cancel and join pending.
        super()._do_shutdown(force)
        # Release any pending refs
        self._shuffling_tasks.clear()
        self._finalizing_tasks.clear()

    def _extra_metrics(self):
        shuffle_name = f"{self._name}_shuffle"
        finalize_name = f"{self._name}_finalize"

        self.shuffle_metrics.as_dict()

        return {
            shuffle_name: self.shuffle_metrics.as_dict(),
            finalize_name: self.reduce_metrics.as_dict(),
        }

    def get_stats(self):
        shuffle_name = f"{self._name}_shuffle"
        reduce_name = f"{self._name}_finalize"
        return {
            shuffle_name: self._shuffled_blocks_stats,
            reduce_name: self._output_blocks_stats,
        }

    def current_processor_usage(self) -> ExecutionResources:
        # Current processors resource usage is comprised by
        #   - Base Aggregator actors resource utilization (captured by
        #     `base_resource_usage` method)
        #   - Active shuffling tasks
        #   - Active finalizing tasks (actor tasks)
        base_usage = self.base_resource_usage
        running_usage = self._shuffling_resource_usage

        # TODO add memory to resources being tracked
        return base_usage.add(running_usage)

    @property
    def base_resource_usage(self) -> ExecutionResources:
        # TODO add memory to resources being tracked
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

    def completed(self) -> bool:
        # TODO separate marking as completed from the check
        return self._is_finalized() and super().completed()

    def implements_accurate_memory_accounting(self) -> bool:
        return True

    def _is_finalized(self):
        return len(self._pending_finalization_partition_ids) == 0

    def _handle_shuffled_block_metadata(
        self,
        input_seq_id: int,
        input_block_metadata: BlockMetadata,
        partition_shards_stats: Dict[int, _PartitionStats],
    ):
        # Keep track of the progress of shuffling incoming blocks
        self._shuffled_blocks_stats.append(input_block_metadata.to_stats())

        # Update incremental input sequence partitions metadata
        for partition_id, new_partition_shard_stats in partition_shards_stats.items():
            current_partition_stats: Optional[_PartitionStats] = self._partitions_stats[
                input_seq_id
            ].get(partition_id, None)

            self._partitions_stats[input_seq_id][partition_id] = (
                _PartitionStats.combine(
                    current_partition_stats,
                    new_partition_shard_stats,
                )
                if current_partition_stats
                else new_partition_shard_stats
            )

    def _get_partition_stats(
        self, partition_id: int
    ) -> Dict[int, Optional[_PartitionStats]]:
        return {
            # NOTE: Some partitions might be empty (and hence missing) in some sequences
            input_seq_id: partition_stats_map.get(partition_id)
            for input_seq_id, partition_stats_map in self._partitions_stats.items()
        }

    @classmethod
    def _estimate_shuffling_memory_req(
        cls,
        block_metadata: BlockMetadata,
        target_max_block_size: int,
    ):
        estimated_block_bytes = (
            block_metadata.size_bytes
            if block_metadata.size_bytes is not None
            else target_max_block_size
        )

        return estimated_block_bytes * 2

    def _get_default_aggregator_ray_remote_args(
        self,
        *,
        num_partitions: int,
        num_aggregators: int,
        total_available_cluster_resources: ExecutionResources,
        estimated_dataset_bytes: Optional[int],
    ):
        assert num_partitions >= num_aggregators

        if estimated_dataset_bytes is not None:
            estimated_aggregator_memory_required = self._estimate_aggregator_memory_allocation(
                num_aggregators=num_aggregators,
                num_partitions=num_partitions,
                # NOTE: If no partition size hint is provided we simply assume target
                #       max block size specified as the best partition size estimate
                estimated_dataset_bytes=estimated_dataset_bytes,
            )
        else:
            # NOTE: In cases when we're unable to estimate dataset size,
            #       we simply fallback to request the minimum of:
            #       - conservative 50% of total available memory for a join operation.
            #       - ``DEFAULT_HASH_SHUFFLE_AGGREGATOR_MEMORY_ALLOCATION`` worth of
            #       memory for every Aggregator.

            max_memory_per_aggregator = (
                total_available_cluster_resources.memory / num_aggregators
            )
            modest_memory_per_aggregator = max_memory_per_aggregator / 2

            estimated_aggregator_memory_required = min(
                modest_memory_per_aggregator,
                DEFAULT_HASH_SHUFFLE_AGGREGATOR_MEMORY_ALLOCATION,
            )

        remote_args = {
            "num_cpus": self._get_aggregator_num_cpus(
                total_available_cluster_resources,
                estimated_aggregator_memory_required,
                num_aggregators=num_aggregators,
            ),
            "memory": estimated_aggregator_memory_required,
            # NOTE: By default aggregating actors should be spread across available
            #       nodes to prevent any single node being overloaded with a "thundering
            #       herd"
            "scheduling_strategy": "SPREAD",
            # Allow actor tasks to execute out of order by default to prevent head-of-line
            # blocking scenario.
            "allow_out_of_order_execution": True,
        }

        return remote_args

    @abc.abstractmethod
    def _get_operator_num_cpus_override(self) -> int:
        pass

    def _get_aggregator_num_cpus(
        self,
        total_available_cluster_resources: ExecutionResources,
        estimated_aggregator_memory_required: int,
        num_aggregators: int,
    ) -> float:
        """Estimates number of CPU resources to be provisioned for individual
        Aggregators.

        Due to semantic of the Aggregator's role (outlined below), their CPU
        allocation is mostly playing a role of complimenting their memory allocation
        such that it serves as a protection mechanism from over-allocation of the
        tasks that do not specify their respective memory resources.
        """

        # First, check whether there is an override
        if self._get_operator_num_cpus_override() is not None:
            return self._get_operator_num_cpus_override()

        # Note that
        #
        #  - Shuffle aggregators have modest computational footprint until
        #    finalization stage
        #  - Finalization stage actually always executes standalone, since it only
        #    starts when all preceding operations complete
        #
        # Though we don't need to purposefully allocate any meaningful amount of
        # CPU resources to the shuffle aggregators, we're still allocating nominal
        # CPU resources to it such that to compliment its required memory allocation
        # and therefore protect from potential OOMs in case other tasks getting
        # scheduled onto the same node, but not specifying their respective memory
        # requirements.
        #
        # CPU allocation is determined like following
        #
        #   CPUs = Total memory required / 4 GiB (standard ratio in the conventional clouds)
        #
        # But no more than
        #   - 25% of total available CPUs but
        #   - No more than 4 CPUs per aggregator
        #
        cap = min(4.0, total_available_cluster_resources.cpu * 0.25 / num_aggregators)
        target_num_cpus = min(
            cap,
            estimated_aggregator_memory_required / (4 * GiB),
        )

        # Round resource to 2d decimal point (for readability)
        return round(target_num_cpus, 2)

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        estimated_dataset_bytes: int,
    ) -> int:
        raise NotImplementedError()

    @classmethod
    def _gen_op_name(cls, num_partitions: int) -> str:
        raise NotImplementedError()


class HashShuffleOperator(HashShufflingOperatorBase):
    # Add 30% buffer to account for data skew
    SHUFFLE_AGGREGATOR_MEMORY_ESTIMATE_SKEW_FACTOR = 1.3

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str],
        num_partitions: Optional[int] = None,
        should_sort: bool = False,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name_factory=(
                lambda num_partitions: f"Shuffle(key_columns={key_columns}, num_partitions={num_partitions})"
            ),
            input_ops=[input_op],
            data_context=data_context,
            key_columns=[key_columns],
            num_partitions=num_partitions,
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            partition_aggregation_factory=(
                lambda aggregator_id, target_partition_ids: Concat(
                    aggregator_id,
                    target_partition_ids,
                    should_sort=should_sort,
                    key_columns=key_columns,
                )
            ),
            shuffle_progress_bar_name="Shuffle",
            # NOTE: In cases like ``groupby`` blocks can't be split as this might violate an invariant that all rows
            #             with the same key are in the same group (block)
            disallow_block_splitting=True,
        )

    def _get_operator_num_cpus_override(self) -> float:
        return self.data_context.hash_shuffle_operator_actor_num_cpus_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        estimated_dataset_bytes: int,
    ) -> int:
        max_partitions_for_aggregator = math.ceil(
            num_partitions / num_aggregators
        )  # Max number of partitions that a single aggregator might handle
        partition_byte_size_estimate = math.ceil(
            estimated_dataset_bytes / num_partitions
        )  # Estimated byte size of a single partition

        # Inputs (object store) - memory for receiving shuffled partitions
        aggregator_shuffle_object_store_memory_required = math.ceil(
            partition_byte_size_estimate * max_partitions_for_aggregator
        )

        # Output (object store) - memory for output partitions
        output_object_store_memory_required = math.ceil(
            partition_byte_size_estimate * max_partitions_for_aggregator
        )

        aggregator_total_memory_required: int = (
            # Inputs (object store)
            aggregator_shuffle_object_store_memory_required
            +
            # Output (object store)
            output_object_store_memory_required
        )
        total_with_skew = math.ceil(
            aggregator_total_memory_required
            * cls.SHUFFLE_AGGREGATOR_MEMORY_ESTIMATE_SKEW_FACTOR
        )
        logger.info(
            f"Estimated memory requirement for shuffling aggregator "
            f"(partitions={num_partitions}, "
            f"aggregators={num_aggregators}, "
            f"dataset (estimate)={estimated_dataset_bytes / GiB:.1f}GiB): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / MiB:.1f}MiB, "
            f"output={output_object_store_memory_required / MiB:.1f}MiB, "
            f"total_base={aggregator_total_memory_required / MiB:.1f}MiB, "
            f"shuffle_aggregator_memory_estimate_skew_factor={cls.SHUFFLE_AGGREGATOR_MEMORY_ESTIMATE_SKEW_FACTOR}, "
            f"total_with_skew={total_with_skew / MiB:.1f}MiB"
        )

        return total_with_skew


@dataclass
class AggregatorHealthInfo:
    """Health information about aggregators for issue detection."""

    started_at: float
    ready_aggregators: int
    total_aggregators: int
    has_unready_aggregators: bool
    wait_time: float
    required_resources: ExecutionResources


class AggregatorPool:
    def __init__(
        self,
        num_partitions: int,
        num_aggregators: int,
        aggregation_factory: StatefulShuffleAggregationFactory,
        aggregator_ray_remote_args: Dict[str, Any],
        target_max_block_size: Optional[int],
    ):
        assert (
            num_partitions >= 1
        ), f"Number of partitions has to be >= 1 (got {num_partitions})"

        self._target_max_block_size = target_max_block_size
        self._num_partitions = num_partitions
        self._num_aggregators: int = num_aggregators
        self._aggregator_partition_map: Dict[
            int, List[int]
        ] = self._allocate_partitions(
            num_partitions=num_partitions,
        )

        self._aggregators: List[ray.actor.ActorHandle] = []

        self._aggregation_factory_ref: ObjectRef[
            StatefulShuffleAggregationFactory
        ] = ray.put(aggregation_factory)

        self._aggregator_ray_remote_args: Dict[
            str, Any
        ] = self._derive_final_shuffle_aggregator_ray_remote_args(
            aggregator_ray_remote_args,
            self._aggregator_partition_map,
        )

    def start(self):
        # Check cluster resources before starting aggregators
        self._check_cluster_resources()

        logger.debug(
            f"Starting {self._num_aggregators} shuffle aggregators with remote "
            f"args: {self._aggregator_ray_remote_args}"
        )

        for aggregator_id in range(self._num_aggregators):
            target_partition_ids = self._aggregator_partition_map[aggregator_id]

            assert len(target_partition_ids) > 0

            aggregator = HashShuffleAggregator.options(
                **self._aggregator_ray_remote_args
            ).remote(
                aggregator_id,
                target_partition_ids,
                self._aggregation_factory_ref,
                self._target_max_block_size,
            )

            self._aggregators.append(aggregator)

        # Start issue detector actor
        self.start_health_monitoring()

    def _check_cluster_resources(self) -> None:
        """Check if cluster has enough resources to schedule all aggregators.
        Raises:
            ValueError: If cluster doesn't have sufficient resources.
        """
        try:
            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
        except Exception as e:
            logger.warning(f"Failed to get cluster resources: {e}")
            return

        # Calculate required resources for all aggregators
        required_cpus = (
            self._aggregator_ray_remote_args.get("num_cpus", 1) * self._num_aggregators
        )
        required_memory = (
            self._aggregator_ray_remote_args.get("memory", 0) * self._num_aggregators
        )

        # Check CPU resources
        total_cpus = cluster_resources.get("CPU", 0)
        available_cpus = available_resources.get("CPU", 0)

        if required_cpus > total_cpus:
            logger.warning(
                f"Insufficient CPU resources in cluster for hash shuffle operation. "
                f"Required: {required_cpus} CPUs for {self._num_aggregators} aggregators, "
                f"but cluster only has {total_cpus} total CPUs. "
                f"Consider either increasing the cluster size or reducing the number of aggregators via `DataContext.max_hash_shuffle_aggregators`."
            )

        if required_cpus > available_cpus:
            logger.warning(
                f"Limited available CPU resources for hash shuffle operation. "
                f"Required: {required_cpus} CPUs, available: {available_cpus} CPUs. "
                f"Aggregators may take longer to start due to contention for resources."
            )

        # Check memory resources if specified
        if required_memory > 0:
            total_memory = cluster_resources.get("memory", 0)
            available_memory = available_resources.get("memory", 0)

            if required_memory > total_memory:
                logger.warning(
                    f"Insufficient memory resources in cluster for hash shuffle operation. "
                    f"Required: {required_memory / GiB:.1f} GiB for {self._num_aggregators} aggregators, "
                    f"but cluster only has {total_memory / GiB:.1f} GiB total memory. "
                    f"Consider reducing the number of partitions or increasing cluster size."
                )

            if required_memory > available_memory:
                logger.warning(
                    f"Limited available memory resources for hash shuffle operation. "
                    f"Required: {required_memory / GiB:.1f} GiB, available: {available_memory / GiB:.1f} GiB. "
                    f"Aggregators may take longer to start due to resource contention."
                )

            logger.debug(
                f"Resource check passed for hash shuffle operation: "
                f"required CPUs={required_cpus}, available CPUs={available_cpus}, "
                f"required memory={required_memory / GiB:.1f} GiB, available memory={available_memory / GiB:.1f} GiB"
            )

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

    @staticmethod
    def _derive_final_shuffle_aggregator_ray_remote_args(
        aggregator_ray_remote_args: Dict[str, Any],
        aggregator_partition_map: Dict[int, List[int]],
    ):
        max_partitions_per_aggregator = max(
            [len(ps) for ps in aggregator_partition_map.values()]
        )

        # Cap shuffle aggregator concurrency at the smaller of
        #   - Max number of partitions per aggregator
        #   - Threshold (8 by default)
        max_concurrency = min(
            max_partitions_per_aggregator,
            DEFAULT_HASH_SHUFFLE_AGGREGATOR_MAX_CONCURRENCY,
        )

        assert (
            max_concurrency >= 1
        ), f"{max_partitions_per_aggregator=}, {DEFAULT_MAX_HASH_SHUFFLE_AGGREGATORS}"

        # NOTE: ShuffleAggregator is configured as threaded actor to allow for
        #       multiple requests to be handled "concurrently" (par GIL) --
        #       while it's not a real concurrency in its fullest of senses, having
        #       multiple Python threads allows as to parallelize all activities not
        #       requiring the GIL (inside Ray Core) such that concurrent request
        #       handling tasks are only blocked on GIL and are ready to execute as
        #       soon as it's released.
        finalized_remote_args = {
            "max_concurrency": max_concurrency,
            **aggregator_ray_remote_args,
        }

        return finalized_remote_args

    def shutdown(self, force: bool):
        # Shutdown aggregators
        if force:
            for actor in self._aggregators:
                # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
                #       hence we're only doing that if this is a forced release
                ray.kill(actor)

        self._aggregators.clear()

    def check_aggregator_health(self) -> Optional[AggregatorHealthInfo]:
        """Get health information about aggregators for issue detection.

        Returns:
            AggregatorHealthInfo with health info or None if monitoring hasn't started.
        """
        if not self._health_monitoring_started:
            return None

        if self._pending_aggregators_refs is None:
            # Initialize readiness refs
            self._pending_aggregators_refs = [
                aggregator.__ray_ready__.remote() for aggregator in self._aggregators
            ]

        # Use ray.wait to check readiness in non-blocking fashion
        _, unready_refs = ray.wait(
            self._pending_aggregators_refs,
            num_returns=len(self._pending_aggregators_refs),
            timeout=0,  # Non-blocking
        )

        # Update readiness refs to only track the unready ones
        self._pending_aggregators_refs = unready_refs

        current_time = time.time()
        ready_aggregators = self._num_aggregators - len(unready_refs)
        required_cpus = (
            self._aggregator_ray_remote_args.get("num_cpus", 1) * self._num_aggregators
        )
        required_memory = (
            self._aggregator_ray_remote_args.get("memory", 0) * self._num_aggregators
        )

        return AggregatorHealthInfo(
            started_at=self._health_monitoring_start_time,
            ready_aggregators=ready_aggregators,
            total_aggregators=self._num_aggregators,
            has_unready_aggregators=len(unready_refs) > 0,
            wait_time=current_time - self._health_monitoring_start_time,
            required_resources=ExecutionResources(
                cpu=required_cpus, memory=required_memory
            ),
        )

    def start_health_monitoring(self):
        """Start health monitoring (without separate actor)."""
        self._health_monitoring_started = True
        self._health_monitoring_start_time = time.time()
        self._pending_aggregators_refs = None


@ray.remote(
    # Make sure tasks are retried indefinitely
    max_task_retries=-1
)
class HashShuffleAggregator:
    """Actor handling of the assigned partitions during hash-shuffle operation

    NOTE: This actor might have ``max_concurrency`` > 1 (depending on the number of
          assigned partitions, and has to be thread-safe!
    """

    def __init__(
        self,
        aggregator_id: int,
        target_partition_ids: List[int],
        agg_factory: StatefulShuffleAggregationFactory,
        target_max_block_size: Optional[int],
    ):
        self._lock = threading.Lock()
        self._agg: StatefulShuffleAggregation = agg_factory(
            aggregator_id, target_partition_ids
        )
        self._target_max_block_size = target_max_block_size

    def submit(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        with self._lock:
            self._agg.accept(input_seq_id, partition_id, partition_shard)

    def finalize(
        self, partition_id: int
    ) -> Generator[Union[Block, "BlockMetadataWithSchema"], None, None]:

        with self._lock:
            exec_stats_builder = BlockExecStats.builder()
            # Finalize given partition id
            block = self._agg.finalize(partition_id)
            exec_stats = exec_stats_builder.build()
            # Clear any remaining state (to release resources)
            self._agg.clear(partition_id)

        if self._target_max_block_size is None:
            yield block
            yield BlockMetadataWithSchema.from_block(block, stats=exec_stats)
        else:
            # Creating a block output buffer per partition finalize task because
            # retrying finalize tasks cause stateful output_bufer to be
            # fragmented (ie, adding duplicated blocks, calling finalize 2x)
            output_buffer = BlockOutputBuffer(
                output_block_size_option=OutputBlockSizeOption(
                    target_max_block_size=self._target_max_block_size
                )
            )

            output_buffer.add_block(block)
            output_buffer.finalize()
            while output_buffer.has_next():
                block = output_buffer.next()
                yield block
                yield BlockMetadataWithSchema.from_block(block, stats=exec_stats)


def _get_total_cluster_resources() -> ExecutionResources:
    """Retrieves total available cluster resources:

    1. If AutoscalerV2 is used, then corresponding max configured resources of
        the corresponding `ClusterConfig` is returned.
    2. In case `ClusterConfig` is not set then falls back to currently available
        cluster resources (retrieved by `ray.cluster_resources()`)

    """
    return ExecutionResources.from_resource_dict(
        ray._private.state.state.get_max_resources_from_cluster_config()
        or ray.cluster_resources()
    )


# TODO rebase on generic operator output estimation
def _try_estimate_output_bytes(
    input_logical_ops: List[LogicalOperator],
) -> Optional[int]:
    inferred_op_output_bytes = [
        op.infer_metadata().size_bytes for op in input_logical_ops
    ]

    # Return sum of input ops estimated output byte sizes,
    # if all are well defined
    if all(nbs is not None for nbs in inferred_op_output_bytes):
        return sum(inferred_op_output_bytes)

    return None
