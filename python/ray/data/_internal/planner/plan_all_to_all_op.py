from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.logical.operators.all_to_all_operator import (
    AbstractAllToAll,
    RandomShuffle,
    RandomizeBlocks,
)
from ray.data._internal.planner.random_shuffle import generate_random_shuffle_fn
from ray.data._internal.planner.randomize_blocks import generate_randomize_blocks_fn


def _plan_all_to_all_op(
    op: AbstractAllToAll,
    input_physical_dag: PhysicalOperator,
) -> AllToAllOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op._seed)
    elif isinstance(op, RandomShuffle):
        fn = generate_random_shuffle_fn(op._seed, op._num_outputs)
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        num_outputs=op._num_outputs,
        name=op.name,
    )
