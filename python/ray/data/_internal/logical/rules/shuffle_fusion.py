import copy
import logging

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Operator,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomShuffle,
    Repartition,
)
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
                twin_op: Repartition = _disown_op(parent_op)
                twin_op._shuffle = True
            else:
                ...

        return op


def _disown_op(child_op: Operator) -> Operator:
    """Visually is does this:

        Before: parent -> child -> grandchild
        After: parent -> grandchild

    This method assumes that the all of the operators are 1-to-1

    Returns: A new(copy) grandchild operator
    """

    # TODO(justin): check for boundary conditions
    parent_op = []
    if child_op.input_dependencies:
        parent_op = child_op.input_dependencies[0]

    grandchild_op = []
    if child_op.output_dependencies:
        grandchild_op = child_op.output_dependencies[0]

    grandtwin_op = copy.copy(grandchild_op)
    grandtwin_op.input_dependencies = [parent_op]
    parent_op.output_dependencies = [grandtwin_op]

    return grandtwin_op
