import logging

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
                return _disconnect_op(parent_op)

            # Special case: RandomShuffle -> Repartition with shuffle flag
            elif isinstance(parent_op, RandomShuffle) and isinstance(
                child_op, Repartition
            ):
                twin_op: Repartition = _disconnect_op(parent_op)
                twin_op._shuffle = True
                return twin_op

            # Key-based fusion cases - Repartition with key-based ops
            elif isinstance(parent_op, Repartition) and isinstance(
                child_op, (Aggregate, Join, Sort)
            ):
                if _keys_can_fuse(parent_op, child_op):
                    return _disconnect_op(parent_op)

            # Aggregate -> Aggregate fusion
            elif isinstance(parent_op, Aggregate) and isinstance(child_op, Aggregate):
                if _keys_can_fuse(parent_op, child_op):
                    twin_op = _disconnect_op(parent_op)
                    assert isinstance(twin_op, Aggregate)
                    twin_op._aggs.extend(parent_op._aggs)

            # Sort -> Aggregate fusion (sort-based shuffle only)
            elif isinstance(parent_op, Sort) and isinstance(child_op, Aggregate):
                ctx = DataContext.get_current()
                if (
                    _keys_can_fuse(parent_op, child_op)
                    and ctx.shuffle_strategy.is_sort_based()
                ):
                    return _disconnect_op(parent_op)

            # Sort -> Sort fusion
            elif isinstance(parent_op, Sort) and isinstance(child_op, Sort):
                twin_op: Sort = _disconnect_op(parent_op)
                twin_op._sort_key._columns.extend(parent_op._sort_key)
                return twin_op

        return op


# TODO(justin): apply this to other Rules
def _disconnect_op(child_op: Operator) -> Operator:
    """Visually it does this for AbstractOneToOne operators:

        Before: parent -> child -> grandchild
        After: parent -> grandchild

    This method will remove the child operator from the dag by
    performing a cross product between parent output dependencies with
    the grandchild's input dependencies. Be mindful that it will
    SHALLOW COPY the grandchild_op.

    Returns: The first grandchild operator.
    """

    grandchild_ops = child_op.output_dependencies
    assert len(grandchild_ops) > 0, "Must have >= 1 output dep"
    for grandchild_op in grandchild_ops:
        grandchild_op.input_dependencies.remove(child_op)

    parent_ops = child_op.input_dependencies
    for parent_op in parent_ops:
        parent_op.output_dependencies.remove(child_op)
        parent_op.output_dependencies.extend(grandchild_ops)

    del child_op
    import copy

    return copy.copy(grandchild_ops[0])


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
        parent_keys = parent_op._sort_key

    # Get child keys based on operator type
    child_keys = None
    if isinstance(child_op, (Repartition, StreamingRepartition)):
        child_keys = child_op._keys
    elif isinstance(child_op, Aggregate):
        child_keys = child_op._key
    elif isinstance(child_op, Sort):
        child_keys = child_op._sort_key
    elif isinstance(child_op, Join):
        # For joins, both left and right keys must match parent keys
        if (
            parent_keys
            and child_op._left_key_columns
            and child_op._right_key_columns
            and set(parent_keys) == set(child_op._left_key_columns)
            and set(parent_keys) == set(child_op._right_key_columns)
        ):
            return True
        # Or all are None
        elif (
            parent_keys is None
            and child_op._left_key_columns is None
            and child_op._right_key_columns is None
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
