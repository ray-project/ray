from typing import List

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


def _plan_hash_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_shuffle import (
        HashShuffleOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op._keys).get_columns()

    return HashShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=(
            logical_op._num_outputs or data_context.default_hash_shuffle_parallelism
        ),
        should_sort=logical_op._sort,
        # TODO wire in aggregator args overrides
    )


def _plan_hash_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_aggregate import (
        HashAggregateOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op._key).get_columns()

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        aggregation_fns=tuple(logical_op._aggs),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=(
            logical_op._num_partitions or data_context.default_hash_shuffle_parallelism
        ),
        # TODO wire in aggregator args overrides
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

    elif isinstance(op, Repartition):
        if op._keys:
            if data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
                return _plan_hash_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            else:
                raise ValueError(
                    "Key-based repartitioning only supported for "
                    f"`DataContext.shuffle_strategy=HASH_SHUFFLE` "
                    f"(got {data_context.shuffle_strategy})"
                )

        elif op._shuffle:
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        else:
            debug_limit_shuffle_execution_to_num_blocks = None

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

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
            return _plan_hash_shuffle_aggregate(data_context, op, input_physical_dag)

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
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        target_max_block_size=None,
        num_outputs=op._num_outputs,
        sub_progress_bar_names=op._sub_progress_bar_names,
        name=op.name,
    )
