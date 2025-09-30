import copy as cp
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
from ray.data._internal.logical.operators.join_operator import Join
from ray.data._internal.logical.operators.map_operator import (
    StreamingRepartition,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec

logger = logging.getLogger(__name__)


class ShuffleFusion(Rule):
    """Optimization rule that fuses shuffle operations together.

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

            # Simple disconnect cases - same operator types

            if isinstance(prev_op, Repartition) and isinstance(op, Repartition):
                _disconnect_op(prev_op)
                copy_op = cp.copy(op)
                copy_op._shuffle_blocks = op._shuffle_blocks or prev_op._shuffle_blocks
                if copy_op._shuffle_blocks:
                    copy_op._sub_progress_bar_names = [
                        ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                        ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                    ]
                else:
                    copy_op.sub_progress_bar_names = [
                        ShuffleTaskSpec.SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME,
                    ]

                return copy_op

            if isinstance(prev_op, StreamingRepartition) and isinstance(
                op, Repartition
            ):
                _disconnect_op(prev_op)
                return op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, RandomShuffle):
                _disconnect_op(prev_op)
                return op

            if isinstance(prev_op, Repartition) and isinstance(op, RandomShuffle):
                _disconnect_op(prev_op)
                copy_op = cp.copy(op)
                copy_op._num_outputs = prev_op._num_outputs
                return copy_op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, Sort):
                _disconnect_op(prev_op)
                return op

            if isinstance(prev_op, RandomShuffle) and isinstance(op, Repartition):
                _disconnect_op(prev_op)
                copy_op = cp.copy(op)
                copy_op._shuffle_blocks = True
                copy_op._random_shuffle = True
                # Update progress bar names to match shuffle configuration
                copy_op._sub_progress_bar_names = [
                    ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME,
                    ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME,
                ]
                return copy_op

            if isinstance(prev_op, Repartition) and isinstance(op, Join):
                # For joins, both left and right keys must match parent keys,
                # and they are guarenteed to be non-empty
                join_keys_match = (
                    prev_op.get_partition_keys()
                    and op._left_key_columns
                    and op._right_key_columns
                    and set(prev_op.get_partition_keys()) == set(op._left_key_columns)
                    and set(prev_op.get_partition_keys()) == set(op._right_key_columns)
                )
                if prev_op._num_outputs == op._num_outputs and join_keys_match:
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Repartition) and isinstance(op, Aggregate):
                if prev_op._num_outputs == op._num_partitions and _keys_can_fuse(
                    prev_op, op
                ):
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Aggregate) and isinstance(op, Aggregate):
                if _keys_can_fuse(prev_op, op):
                    _disconnect_op(prev_op)
                    copy_op = cp.copy(op)
                    copy_op._aggs.extend(prev_op._aggs)
                    return copy_op

            if isinstance(prev_op, Sort) and isinstance(op, Aggregate):
                ctx = DataContext.get_current()
                if _keys_can_fuse(prev_op, op) and ctx.shuffle_strategy.is_sort_based():
                    _disconnect_op(prev_op)
                    return op

            if isinstance(prev_op, Sort) and isinstance(op, Sort):
                if (
                    prev_op._sort_key._descending == op._sort_key._descending
                    and prev_op._batch_format == op._batch_format
                ):
                    _disconnect_op(prev_op)
                    copy_op = cp.copy(op)
                    copy_op._sort_key._columns.extend(prev_op._sort_key._columns)
                    return copy_op

        return op


# TODO(justin): apply this to other Rules
def _disconnect_op(op: Operator):
    """Disconnect a child operator from the DAG by connecting its parents directly to its grandchildren.

    Visually this transforms:
        Before: parent -> child -> grandchild
        After:  parent -> grandchild

    Args:
        child_op: The operator to remove from the DAG
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
