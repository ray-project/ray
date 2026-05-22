from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    Aggregate,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.planner.aggregate import generate_aggregate_fn
from ray.data._internal.planner.random_shuffle import generate_random_shuffle_fn
from ray.data._internal.planner.randomize_blocks import generate_randomize_blocks_fn
from ray.data._internal.planner.repartition import generate_repartition_fn
from ray.data._internal.planner.sort import generate_sort_fn
from ray.data.context import DataContext, ShuffleStrategy


def _plan_gpu_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.gpu_shuffle.hash_shuffle import GPUShuffleOperator
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    schema = logical_op.infer_schema()
    columns = list(schema.names) if schema is not None else None

    return GPUShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),
        columns=columns,
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
    )


def _plan_hash_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Build the two-op (ShuffleMapOp → ShuffleReduceOp) DAG for V2 hash shuffle.

    Returns the reduce op; the executor crawls upstream via its
    input_dependencies to find the map op.
    """
    from ray.data._internal.execution.operators.hash_shuffle_v2 import (
        _SHUFFLE_MAP_RUNTIME_ENV,
        _concat_reduce,
        _make_hash_partition_fn,
        _sort_reduce,
    )
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
        ShuffleMapOp,
    )
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
        ShuffleReduceOp,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()
    key_list = list(normalized_key_columns)

    # Resolve target_num_partitions with the same fallback chain as the
    # previous monolithic operator: explicit → upstream estimate → context
    # default.
    input_logical_op = input_physical_op._logical_operators[0]
    estimated_input_blocks = input_logical_op.estimated_num_outputs()
    target_num_partitions = (
        logical_op.num_outputs
        or estimated_input_blocks
        or data_context.default_hash_shuffle_parallelism
    )

    partition_fn = _make_hash_partition_fn(key_list, target_num_partitions)
    reduce_fn = _sort_reduce(key_list) if logical_op.sort else _concat_reduce

    map_op = ShuffleMapOp(
        input_physical_op,
        data_context,
        num_partitions=target_num_partitions,
        partition_fn=partition_fn,
        map_runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
        name=(
            f"HashShuffleMap(keys={tuple(key_list)}, "
            f"partitions={target_num_partitions})"
        ),
    )
    reduce_op = ShuffleReduceOp(
        map_op,
        data_context,
        num_partitions=target_num_partitions,
        reduce_fn=reduce_fn,
        # Partition = block contract: each partition becomes one output
        # block.  Requires blocking reduce + no BlockOutputBuffer reshaping.
        streaming_reduce=False,
        disallow_block_splitting=True,
        name=(
            f"HashShuffleReduce(keys={tuple(key_list)}, "
            f"partitions={target_num_partitions})"
        ),
    )
    return reduce_op


def _plan_hash_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_aggregate import (
        HashAggregateOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.key).get_columns()

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        aggregation_fns=tuple(logical_op.aggs),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=logical_op.num_partitions,
        aggregator_ray_remote_args_override=logical_op.ray_remote_args,
    )


def plan_all_to_all_op(
    op: AbstractAllToAll,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op, data_context)
        # Randomize block order does not actually compute anything, so we
        # want to inherit the upstream op's target max block size.

    elif isinstance(op, RandomShuffle):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_random_shuffle_fn(
            data_context,
            op.seed_config,
            op.num_outputs,
            op.ray_remote_args,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Repartition):
        if op.keys:
            if data_context.shuffle_strategy == ShuffleStrategy.GPU_SHUFFLE:
                return _plan_gpu_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            elif data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
                return _plan_hash_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            else:
                raise ValueError(
                    "Key-based repartitioning only supported for "
                    f"`DataContext.shuffle_strategy=HASH_SHUFFLE` or "
                    f"`DataContext.shuffle_strategy=GPU_SHUFFLE` "
                    f"(got {data_context.shuffle_strategy})"
                )

        elif op.shuffle:
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        else:
            debug_limit_shuffle_execution_to_num_blocks = None

        fn = generate_repartition_fn(
            op.num_outputs,
            op.shuffle,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Sort):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_sort_fn(
            op.sort_key,
            op.batch_format,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy in (
            ShuffleStrategy.HASH_SHUFFLE,
            ShuffleStrategy.GPU_SHUFFLE,
        ):
            return _plan_hash_shuffle_aggregate(data_context, op, input_physical_dag)

        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_aggregate_fn(
            op.key,
            op.aggs,
            op.batch_format,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        num_outputs=op.num_outputs,
        sub_progress_bar_names=op.sub_progress_bar_names,
        name=op.name,
    )
