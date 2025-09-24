import copy
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
            if isinstance(parent_op, Repartition) and isinstance(child_op, Repartition):
                return _disown_op(parent_op)
            elif isinstance(parent_op, StreamingRepartition) and isinstance(
                child_op, Repartition
            ):
                return _disown_op(parent_op)
            elif isinstance(parent_op, RandomShuffle) and isinstance(
                child_op, RandomShuffle
            ):
                return _disown_op(parent_op)
            elif isinstance(parent_op, RandomShuffle) and isinstance(
                child_op, Repartition
            ):
                uncle_op: Repartition = _disown_op(parent_op)
                uncle_op._shuffle = True
                return uncle_op
            elif isinstance(parent_op, Repartition) and isinstance(
                child_op, RandomShuffle
            ):
                return _disown_op(parent_op)
            elif isinstance(parent_op, Repartition) and isinstance(child_op, Aggregate):
                parent_keys = set(parent_op._keys)
                child_keys = set(child_op._key)
                if child_keys == parent_keys:
                    return _disown_op(parent_op)
            elif isinstance(parent_op, Repartition) and isinstance(child_op, Join):
                parent_keys = set(parent_op._keys)
                child_keys = set(child_op._left_key_columns)
                if child_keys == parent_keys:
                    return _disown_op(parent_op)
            elif isinstance(parent_op, Repartition) and isinstance(child_op, Sort):
                parent_keys = set(parent_op._keys)
                child_keys = set(child_op._sort_key)
                if child_keys == parent_keys:
                    return _disown_op(parent_op)
            elif isinstance(parent_op, Aggregate) and isinstance(child_op, Aggregate):
                parent_keys = set(parent_op._key)
                child_keys = set(child_op._key)
                if child_keys == parent_keys:
                    twin_op = _disown_op(parent_op)
                    assert isinstance(twin_op, Aggregate)
                    twin_op._aggs.extend(parent_op._aggs)
            elif isinstance(parent_op, Sort) and isinstance(child_op, Aggregate):
                parent_keys = set(parent_op._sort_key)
                child_keys = set(child_op._key)
                ctx = DataContext.get_current()
                if child_keys == parent_keys and ctx.shuffle_strategy.is_sort_based():
                    return _disown_op(parent_op)
            elif isinstance(parent_op, Sort) and isinstance(child_op, Sort):
                twin_op: Sort = _disown_op(parent_op)
                twin_op._sort_key._columns.extend(parent_op._sort_key)
                return twin_op
            elif isinstance(parent_op, Sort) and isinstance(child_op, RandomShuffle):
                return _disown_op(parent_op)
            elif isinstance(parent_op, RandomShuffle) and isinstance(child_op, Sort):
                return _disown_op(parent_op)

        return op


# TODO(justin): apply this to other Rules
def _disown_op(child_op: Operator) -> Operator:
    """Visually is does this:

        Before: parent -> child -> grandchild
        After: parent -> grandchild

    This method assumes that all of the operators are 1-to-1

    Returns: A new(copy) grandchild operator
    """

    parent_ops = child_op.input_dependencies
    grandchild_ops = child_op.output_dependencies

    assert len(parent_ops) == 1
    assert len(grandchild_ops) == 1

    grandtwin_op = copy.copy(grandchild_ops)
    grandtwin_op.input_dependencies = parent_ops
    parent_ops.output_dependencies = grandtwin_op

    del child_op

    return grandtwin_op
