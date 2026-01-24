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
            shuffle = input_op._shuffle or op._shuffle
            return Repartition(
                input_op.input_dependencies[0],
                num_outputs=op._num_outputs,
                shuffle=shuffle,
                keys=op._keys,
                sort=op._sort,
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(
            op, StreamingRepartition
        ):
            return StreamingRepartition(
                input_op.input_dependencies[0],
                target_num_rows_per_block=op.target_num_rows_per_block,
            )
        elif isinstance(input_op, Repartition) and isinstance(op, Aggregate):
            return Aggregate(
                input_op=input_op.input_dependencies[0],
                key=op._key,
                aggs=op._aggs,
                num_partitions=op._num_partitions,
                batch_format=op._batch_format,
            )
        elif isinstance(input_op, StreamingRepartition) and isinstance(op, Repartition):
            return Repartition(
                input_op.input_dependencies[0],
                num_outputs=op._num_outputs,
                shuffle=op._shuffle,
                keys=op._keys,
                sort=op._sort,
            )
        elif isinstance(input_op, Sort) and isinstance(op, Sort):
            return Sort(
                input_op.input_dependencies[0],
                sort_key=op._sort_key,
                batch_format=op._batch_format,
            )

        return op
