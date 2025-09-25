import logging
from typing import List, Tuple

from ray.data import DataContext
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
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
        child_op = op
        parent_ops = op.input_dependencies
        if len(parent_ops) == 1:

            parent_op = parent_ops[0]

            # Simple disown cases - same operator types
            if (
                (
                    isinstance(parent_op, Repartition)
                    and isinstance(child_op, Repartition)
                )
                or (
                    isinstance(parent_op, StreamingRepartition)
                    and isinstance(child_op, Repartition)
                )
                or (
                    isinstance(parent_op, RandomShuffle)
                    and isinstance(child_op, RandomShuffle)
                )
                or (
                    isinstance(parent_op, Repartition)
                    and isinstance(child_op, RandomShuffle)
                )
                or (isinstance(parent_op, RandomShuffle) and isinstance(child_op, Sort))
            ):
                _, child_op = _disconnect_op(parent_op, copy=False)
                return child_op[0]

            # Special case: RandomShuffle -> Repartition with shuffle flag
            elif isinstance(parent_op, RandomShuffle) and isinstance(
                child_op, Repartition
            ):
                _, child_op = _disconnect_op(parent_op)
                twin_op = child_op[0]
                assert isinstance(twin_op, Repartition)
                twin_op._shuffle = True
                return twin_op

            # Key-based fusion cases - Repartition with Join
            elif isinstance(parent_op, Repartition) and isinstance(child_op, Join):
                if parent_op._num_outputs == child_op._num_outputs and _keys_can_fuse(
                    parent_op, child_op
                ):
                    _, child_op = _disconnect_op(parent_op, copy=False)
                    return child_op[0]

            # Key-based fusion cases - Repartition with Aggregate
            elif isinstance(parent_op, Repartition) and isinstance(child_op, Aggregate):
                if (
                    parent_op._num_outputs == child_op._num_partitions
                    and _keys_can_fuse(parent_op, child_op)
                ):
                    _, child_op = _disconnect_op(parent_op, copy=False)
                    return child_op[0]

            # Aggregate -> Aggregate fusion
            elif isinstance(parent_op, Aggregate) and isinstance(child_op, Aggregate):
                if _keys_can_fuse(parent_op, child_op):
                    _, child_op = _disconnect_op(parent_op)
                    twin_op = child_op[0]
                    assert isinstance(twin_op, Aggregate)
                    twin_op._aggs.extend(parent_op._aggs)

            # Sort -> Aggregate fusion (sort-based shuffle only)
            elif isinstance(parent_op, Sort) and isinstance(child_op, Aggregate):
                ctx = DataContext.get_current()
                if (
                    _keys_can_fuse(parent_op, child_op)
                    and ctx.shuffle_strategy.is_sort_based()
                ):
                    _, child_op = _disconnect_op(parent_op, copy=False)
                    return child_op[0]

            # Sort -> Sort fusion
            elif isinstance(parent_op, Sort) and isinstance(child_op, Sort):
                if parent_op._sort_key._descending == child_op._sort_key._descending:
                    _, child_op = _disconnect_op(parent_op)
                    twin_op = child_op[0]
                    assert isinstance(twin_op, Sort)
                    twin_op._sort_key._columns.extend(parent_op._sort_key)
                    return twin_op

        return op


# TODO(justin): apply this to other Rules
def _disconnect_op(
    child_op: Operator, copy=True
) -> Tuple[List[Operator], List[Operator]]:
    """Disconnect a child operator from the DAG by connecting its parents directly to its grandchildren.

    Visually this transforms:
        Before: parent -> child -> grandchild
        After:  parent -> grandchild

    Args:
        child_op: The operator to remove from the DAG
        copy: If True, returns copies of the operators. If False, returns references.
              Use copy=True if you plan to modify the returned operators.

    Returns:
        Tuple of (parent_operators, grandchild_operators):
        - parent_operators: List of parent operators that were connected to child_op
        - grandchild_operators: List of grandchild operators that child_op was connected to

        If copy=True, both lists contain shallow copies of the operators.
        If copy=False, both lists contain references to the original operators.
    """
    grandchild_ops = child_op.output_dependencies
    parent_ops = child_op.input_dependencies

    for grandchild_op in grandchild_ops:
        grandchild_op.input_dependencies.remove(child_op)
        grandchild_op.input_dependencies.extend(parent_ops)

    for parent_op in parent_ops:
        parent_op.output_dependencies.remove(child_op)
        parent_op.output_dependencies.extend(grandchild_ops)

    # the child_op is now disconnected

    if copy:
        import copy as cp

        parent_copies = [cp.copy(parent) for parent in parent_ops]
        grandchild_copies = [cp.copy(grandchild_op) for grandchild_op in grandchild_ops]

        return parent_copies, grandchild_copies

    return parent_ops, grandchild_ops


# TODO(justin): Im thinking about a function for partitioning operators
# but joins are a bit quirky because they contain two keys.
def _keys_can_fuse(parent_op, child_op) -> bool:
    """Check if parent and child operators can fuse based on key matching."""
    # Get parent keys based on operator type
    parent_keys = None
    if isinstance(parent_op, (Repartition, StreamingRepartition)):
        parent_keys = parent_op._keys
    elif isinstance(parent_op, Aggregate):
        parent_keys = parent_op._key
    elif isinstance(parent_op, Sort):
        parent_keys = parent_op._sort_key._columns

    # Get child keys based on operator type
    child_keys = None
    if isinstance(child_op, (Repartition, StreamingRepartition)):
        child_keys = child_op._keys
    elif isinstance(child_op, Aggregate):
        child_keys = child_op._key
    elif isinstance(child_op, Sort):
        child_keys = child_op._sort_key._columns
    elif isinstance(child_op, Join):
        # For joins, both left and right keys must match parent keys,
        # and they are guarenteed to be non-empty
        if (
            parent_keys
            and child_op._left_key_columns
            and child_op._right_key_columns
            and set(parent_keys) == set(child_op._left_key_columns)
            and set(parent_keys) == set(child_op._right_key_columns)
        ):
            return True
        return False

    # Compare keys: either both match or both are None
    if parent_keys and child_keys:
        return set(parent_keys) == set(child_keys)
    elif parent_keys is None and child_keys is None:
        return True
    else:
        return False
