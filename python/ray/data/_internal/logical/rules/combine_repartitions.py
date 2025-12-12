from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import Repartition
from ray.data._internal.logical.operators.map_operator import StreamingRepartition


class CombineRepartitions(Rule):
    """This logical rule combines chained ``Repartition`` and
    ``StreamingRepartition`` ops fusing them into a single one.
    """

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, LogicalPlan)

        def _combine_repartitions(op: LogicalOperator) -> LogicalOperator:
            # Repartitions should have exactly 1 input
            if len(op.input_dependencies) != 1:
                return op

            input_op = op.input_dependencies[0]

            if isinstance(op, Repartition) and isinstance(input_op, Repartition):
                shuffle = input_op._shuffle or op._shuffle
                return Repartition(
                    input_op.input_dependencies[0],
                    num_outputs=op._num_outputs,
                    shuffle=shuffle,
                    keys=op._keys,
                    sort=op._sort,
                )
            elif isinstance(op, StreamingRepartition) and isinstance(
                input_op, StreamingRepartition
            ):
                return StreamingRepartition(
                    input_op.input_dependencies[0],
                    target_num_rows_per_block=op.target_num_rows_per_block,
                )

            return op

        original_dag = plan.dag
        transformed_dag = original_dag._apply_transform(_combine_repartitions)

        if transformed_dag is original_dag:
            return plan

        # TODO replace w/ Plan.copy
        return LogicalPlan(
            dag=transformed_dag,
            context=plan.context,
        )
