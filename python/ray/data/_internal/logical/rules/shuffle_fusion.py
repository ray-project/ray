import logging

from ray.data import DataContext
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorContainsPartitionKeys,
    LogicalPlan,
    Operator,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import (
    Aggregate,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.map_operator import (
    StreamingRepartition,
)
from ray.data._internal.logical.rules.operator_fusion import _are_remote_args_compatible

logger = logging.getLogger(__name__)


class ShuffleFusion(Rule):
    """Logical optimization rule that fuses shuffle operations together. This is different
    from FuseOperators, which operates on the physical-level.

    If there are redundant Shuffle operators, it removes the `Project` operator from
    the graph.
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

            # Only fuse if the ops' remote arguments are compatible.
            if not _are_remote_args_compatible(
                getattr(prev_op, "_ray_remote_args", {}),
                getattr(op, "_ray_remote_args", {}),
            ):
                return op

            if getattr(prev_op, "_ray_remote_args_fn", None) or getattr(
                op, "_ray_remote_args_fn", None
            ):
                return op

            if isinstance(prev_op, Repartition) and isinstance(op, Repartition):
                if _keys_can_fuse(prev_op, op):
                    _disconnect_op(prev_op)
                    # If one of the operators hash shuffles, then new_op should too.
                    hash_shuffle = op._hash_shuffle or prev_op._hash_shuffle

                    # Similarly, if one of the operators random permutation, then the new_op
                    # should randomly permute too.
                    random_permute = op._random_permute or prev_op._random_permute

                    new_op = Repartition(
                        input_op=prev_op.input_dependencies[0],
                        num_outputs=op._num_outputs,
                        hash_shuffle=hash_shuffle,
                        random_permute=random_permute,
                        keys=op._keys,
                        sort=op._sort,
                    )

                    return new_op

            if isinstance(prev_op, StreamingRepartition) and isinstance(
                op, Repartition
            ):
                _disconnect_op(prev_op)
                return op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, RandomShuffle):
                # We need to make sure at least one of the shuffles is non-deterministic
                if prev_op._seed is not None or op._seed is not None:
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Repartition) and isinstance(op, RandomShuffle):
                _disconnect_op(prev_op)

                new_op = RandomShuffle(
                    input_op=prev_op.input_dependencies[0],
                    name=op.name,
                    seed=op._seed,
                    # NOTE: Fallback
                    num_outputs=op._num_outputs or prev_op._num_outputs,
                )
                return new_op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, Repartition):
                _disconnect_op(prev_op)
                # Create new Repartition with shuffle enabled
                new_op = Repartition(
                    input_op=prev_op.input_dependencies[0],
                    num_outputs=op._num_outputs,
                    hash_shuffle=True,  # NOTE: the shuffle here
                    keys=op._keys,
                    sort=op._sort,
                )
                return new_op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, Sort):
                _disconnect_op(prev_op)
                return op

            if isinstance(prev_op, Repartition) and isinstance(op, Aggregate):
                # The number of outputs must match
                if prev_op._num_outputs == op._num_partitions and _keys_can_fuse(
                    prev_op, op
                ):
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Aggregate) and isinstance(op, Aggregate):
                if (
                    _keys_can_fuse(prev_op, op)
                    and op._batch_format == prev_op._batch_format
                ):
                    _disconnect_op(prev_op)
                    # Create new Aggregate with combined aggs
                    combined_aggs = prev_op._aggs + op._aggs
                    new_op = Aggregate(
                        input_op=prev_op.input_dependencies[0],
                        key=op._key,
                        aggs=combined_aggs,
                        # NOTE: Fallback
                        num_partitions=op._num_partitions or prev_op._num_partitions,
                        batch_format=op._batch_format,
                    )
                    return new_op

            if isinstance(prev_op, Sort) and isinstance(op, Aggregate):
                ctx = DataContext.get_current()
                if _keys_can_fuse(prev_op, op) and ctx.shuffle_strategy.is_sort_based():
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Sort) and isinstance(op, Sort):
                if prev_op._batch_format == op._batch_format:
                    _disconnect_op(prev_op)
                    # Create new Sort with combined columns
                    from ray.data._internal.planner.exchange.sort_task_spec import (
                        SortKey,
                    )

                    # NOTE: sort op first, then prev_op
                    combined_columns = (
                        op._sort_key._columns + prev_op._sort_key._columns
                    )
                    combined_desending = (
                        op._sort_key._descending + prev_op._sort_key._descending
                    )
                    combined_sort_key = SortKey(
                        columns=combined_columns,
                        descending=combined_desending,
                    )
                    new_op = Sort(
                        input_op=prev_op.input_dependencies[0],
                        sort_key=combined_sort_key,
                        batch_format=op._batch_format,
                    )
                    return new_op

        return op


# TODO(justin): apply this to other Rules
def _disconnect_op(op: Operator):
    """Disconnect an operator from the DAG by connecting
    its prev_ops directly to its next_ops.

    Visually this transforms:
        Before: prev_op -> op -> next_op
        After:  prev_op -> next_op

    Args:
        op: The operator to remove from the DAG
    """
    next_ops = op.output_dependencies
    prev_ops = op.input_dependencies

    for next_op in next_ops:
        next_op.input_dependencies.remove(op)
        next_op.input_dependencies.extend(prev_ops)

    for prev_op in prev_ops:
        prev_op.output_dependencies.remove(op)
        prev_op.output_dependencies.extend(next_ops)

    # the child_op is now disconnected


def _keys_can_fuse(
    parent_op: LogicalOperatorContainsPartitionKeys,
    child_op: LogicalOperatorContainsPartitionKeys,
) -> bool:
    """Check if parent and child operators can fuse based on key matching."""
    # Get parent keys based on operator type
    parent_keys = parent_op.get_partition_keys()

    # Get child keys based on operator type
    child_keys = child_op.get_partition_keys()

    # Compare keys: either both match or both are None
    if parent_keys and child_keys:
        return set(parent_keys) == set(child_keys)
    elif parent_keys is None and child_keys is None:
        return True
    else:
        return False
