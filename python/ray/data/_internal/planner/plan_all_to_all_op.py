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
from ray.data.context import DataContext


def plan_all_to_all_op(
    op: AbstractAllToAll,
    input_physical_dag: PhysicalOperator,
) -> AllToAllOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    target_max_block_size = None
    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op)
        # Randomize block order does not actually compute anything, so we
        # want to inherit the upstream op's target max block size.
    elif isinstance(op, RandomShuffle):
        fn = generate_random_shuffle_fn(op._seed, op._num_outputs, op._ray_remote_args)
        target_max_block_size = DataContext.get_current().target_shuffle_max_block_size
    elif isinstance(op, Repartition):
        fn = generate_repartition_fn(op._num_outputs, op._shuffle)
        if op._shuffle:
            target_max_block_size = (
                DataContext.get_current().target_shuffle_max_block_size
            )
    elif isinstance(op, Sort):
        fn = generate_sort_fn(op._sort_key)
        target_max_block_size = DataContext.get_current().target_shuffle_max_block_size
    elif isinstance(op, Aggregate):
        fn = generate_aggregate_fn(op._key, op._aggs)
        target_max_block_size = DataContext.get_current().target_shuffle_max_block_size
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        target_max_block_size=target_max_block_size,
        num_outputs=op._num_outputs,
        sub_progress_bar_names=op._sub_progress_bar_names,
        name=op.name,
    )
