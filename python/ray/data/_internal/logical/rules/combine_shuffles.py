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
            return Repartition(
                num_outputs=op.num_outputs,
                input_dependencies=[input_op.input_dependencies[0]],
                shuffle=shuffle,
                keys=op.keys,
                sort=op.sort,
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(
            op, StreamingRepartition
        ):
            strict = input_op.strict or op.strict
            return StreamingRepartition(
                target_num_rows_per_block=op.target_num_rows_per_block,
                input_dependencies=[input_op.input_dependencies[0]],
                strict=strict,
            )
        elif isinstance(input_op, Repartition) and isinstance(op, Aggregate):
            return Aggregate(
                key=op.key,
                aggs=op.aggs,
                input_dependencies=[input_op.input_dependencies[0]],
                num_partitions=op.num_partitions,
                batch_format=op.batch_format,
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(op, Repartition):
            return Repartition(
                num_outputs=op.num_outputs,
                input_dependencies=[input_op.input_dependencies[0]],
                shuffle=op.shuffle,
                keys=op.keys,
                sort=op.sort,
            )
        elif isinstance(input_op, Sort) and isinstance(op, Sort):
            return Sort(
                sort_key=op.sort_key,
                input_dependencies=[input_op.input_dependencies[0]],
                batch_format=op.batch_format,
            )

        return op
