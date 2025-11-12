import logging

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorHasShuffleKeys,
    LogicalPlan,
    Operator,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import (
    Repartition,
)
from ray.data._internal.logical.rules.operator_fusion import are_remote_args_compatible
from ray.data._internal.progress_bar import truncate_operator_name

logger = logging.getLogger(__name__)


class ShuffleFusion(Rule):
    """Logical optimization rule that fuses shuffle operations together. This is different
    from FuseOperators, which operates on the physical-level.

    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self.fuse_with_upstream)

        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def fuse_with_upstream(cls, op: LogicalOperator) -> LogicalOperator:
        prev_ops = op.input_dependencies
        if len(prev_ops) == 1:

            prev_op = prev_ops[0]

            # NOTE: This str contains outer brackets to show
            # that it's logical fusion. TODO(justin): Please confirm
            # with team if ok.
            fused_name = truncate_operator_name(f"[{prev_op.name}->{op.name}]", 100)

            # Only fuse if the ops' remote arguments are compatible.
            if not are_remote_args_compatible(
                getattr(prev_op, "_ray_remote_args", {}),
                getattr(op, "_ray_remote_args", {}),
            ):
                return op

            if getattr(prev_op, "_ray_remote_args_fn", None) or getattr(
                op, "_ray_remote_args_fn", None
            ):
                return op

            if isinstance(prev_op, Repartition) and isinstance(op, Repartition):
                return _try_repartition_repartition_fusion(prev_op, op, fused_name)

        return op


def _disconnect_op_from_dag(curr_op: Operator):
    """Disconnect an operator from the DAG by connecting
    its prev_ops directly to its next_ops.

    Visually this transforms:
        Before: prev_op -> op -> next_op
        After:  prev_op -> next_op

    Args:
        curr_op: The operator to remove from the DAG
    """
    next_ops = curr_op.output_dependencies
    prev_ops = curr_op.input_dependencies

    for next_op in next_ops:
        next_op.input_dependencies.remove(curr_op)
        next_op.input_dependencies.extend(prev_ops)

    for prev_op in prev_ops:
        prev_op.output_dependencies.remove(curr_op)
        prev_op.output_dependencies.extend(next_ops)

    # curr_op is now disconnected


def _keys_can_fuse(
    prev_op: LogicalOperatorHasShuffleKeys,
    op: LogicalOperatorHasShuffleKeys,
) -> bool:
    """Check if prev and curr operators can fuse based on key matching.
    This helper function is used to compare if two shuffle operators
    have compatible keys to fuse together."""
    # Get parent keys based on operator type
    prev_keys = prev_op.get_partition_keys()

    # Get child keys based on operator type
    op_keys = op.get_partition_keys()

    # Compare keys: either both match or both are None
    if prev_keys and op_keys:
        return set(prev_keys) == set(op_keys)
    elif prev_keys is None and op_keys is None:
        return True
    else:
        return False


# Helper functions for each fusion pattern
def _try_repartition_repartition_fusion(
    prev_op: LogicalOperator, op: LogicalOperator, fused_name: str
) -> LogicalOperator:
    """Fuse Repartition -> Repartition operations."""
    if not _keys_can_fuse(prev_op, op):
        return op

    _disconnect_op_from_dag(prev_op)
    # If one of the operators full shuffles, then new_op should too.
    full_shuffle = op._full_shuffle or prev_op._full_shuffle

    # Similarly, if one of the operators randomly permutes, then the new_op
    # should randomly permute too.
    random_permute = op._random_permute or prev_op._random_permute

    new_op = Repartition(
        name=fused_name,
        input_op=prev_op.input_dependencies[0],
        num_outputs=op._num_outputs,
        full_shuffle=full_shuffle,
        random_permute=random_permute,
        keys=op._keys,
        sort=op._sort,
    )

    return new_op
