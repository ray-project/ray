from typing import Any, Dict, Optional

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.functions.randomize_blocks import (
    generate_randomize_blocks_fn,
)
from ray.data._internal.logical.interfaces import LogicalOperator


class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            num_outputs: The number of expected output bundles outputted by this
                operator.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, [input_op])
        self._num_outputs = num_outputs
        self._ray_remote_args = ray_remote_args or {}


class RandomizeBlocks(AbstractAllToAll):
    """Logical operator for randomize_block_order."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
    ):
        super().__init__(
            "RandomizeBlocks",
            input_op,
        )
        self._seed = seed


def plan_all_to_all_op(
    op: AbstractAllToAll,
    input_physical_dag: PhysicalOperator,
) -> AllToAllOperator:
    """Get the corresponding physical operators DAG for `op` logical operator.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op._seed)
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        num_outputs=op._num_outputs,
        name=op.name,
    )
