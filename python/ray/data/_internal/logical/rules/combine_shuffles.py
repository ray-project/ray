from dataclasses import replace

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators import (
    Aggregate,
    Repartition,
    Sort,
    StreamingRepartition,
)

__all__ = [
    "CombineShuffles",
]


class CombineShuffles(Rule):
    """This logical rule combines chained shuffles together. For example,
    ``Repartition`` and ``StreamingRepartition`` ops fusing them into a single one.
    """

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, LogicalPlan)

        original_dag = plan.dag
        transformed_dag = original_dag._apply_transform(self._combine)

        if transformed_dag is original_dag:
            return plan

        # TODO replace w/ Plan.copy
        return LogicalPlan(
            dag=transformed_dag,
            context=plan.context,
        )

    @classmethod
    def _combine(self, op: LogicalOperator) -> LogicalOperator:
        # Repartitions should have exactly 1 input
        if len(op.input_dependencies) != 1:
            return op

        input_op = op.input_dependencies[0]

        if isinstance(input_op, Repartition) and isinstance(op, Repartition):
            shuffle = input_op.shuffle or op.shuffle
            return replace(
                op,
                input_dependencies=[input_op.input_dependencies[0]],
                shuffle=shuffle,
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(
            op, StreamingRepartition
        ):
            strict = input_op.strict or op.strict
            return replace(
                op,
                input_dependencies=[input_op.input_dependencies[0]],
                strict=strict,
            )
        elif isinstance(input_op, Repartition) and isinstance(op, Aggregate):
            return replace(
                op,
                input_dependencies=[input_op.input_dependencies[0]],
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(op, Repartition):
            return replace(
                op,
                input_dependencies=[input_op.input_dependencies[0]],
            )
        elif isinstance(input_op, Sort) and isinstance(op, Sort):
            return replace(
                op,
                input_dependencies=[input_op.input_dependencies[0]],
            )

        return op
