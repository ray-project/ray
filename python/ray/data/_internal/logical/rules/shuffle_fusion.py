import logging

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import (
    Repartition,
)
from ray.data._internal.logical.rules.operator_fusion import are_remote_args_compatible

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
                # If one of the operators full shuffles, then new_op should too.
                full_shuffle = op._full_shuffle or prev_op._full_shuffle

                return Repartition(
                    name=op.name,
                    input_op=prev_op.input_dependencies[0],
                    num_outputs=op._num_outputs,
                    full_shuffle=full_shuffle,
                    keys=op._keys,
                    sort=op._sort,
                )

        return op
