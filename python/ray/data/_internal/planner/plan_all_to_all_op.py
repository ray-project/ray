from typing import List

from ray.anyscale.data._internal.execution.operators.hash_aggregate import (
    HashAggregateOperator,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.logical.operators.all_to_all_operator import (
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


def _plan_aggregate_with_hash_based_shuffle(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(SortKey(logical_op._key).get_columns()),
        aggregation_fns=tuple(logical_op._aggs),
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=(
            logical_op._num_partitions or data_context.default_hash_shuffle_parallelism
        ),
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

    target_max_block_size = None

    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op)
        # Randomize block order does not actually compute anything, so we
        # want to inherit the upstream op's target max block size.

    elif isinstance(op, RandomShuffle):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_random_shuffle_fn(
            data_context,
            op._seed,
            op._num_outputs,
            op._ray_remote_args,
            debug_limit_shuffle_execution_to_num_blocks,
        )
        target_max_block_size = data_context.target_shuffle_max_block_size

    elif isinstance(op, Repartition):
        debug_limit_shuffle_execution_to_num_blocks = None
        if op._shuffle:
            target_max_block_size = data_context.target_shuffle_max_block_size
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        fn = generate_repartition_fn(
            op._num_outputs,
            op._shuffle,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Sort):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_sort_fn(
            op._sort_key,
            op._batch_format,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
        target_max_block_size = data_context.target_shuffle_max_block_size

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
            return _plan_aggregate_with_hash_based_shuffle(
                data_context, op, input_physical_dag
            )

        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_aggregate_fn(
            op._key,
            op._aggs,
            op._batch_format,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
        target_max_block_size = data_context.target_shuffle_max_block_size
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        target_max_block_size=target_max_block_size,
        num_outputs=op._num_outputs,
        sub_progress_bar_names=op._sub_progress_bar_names,
        name=op.name,
    )
