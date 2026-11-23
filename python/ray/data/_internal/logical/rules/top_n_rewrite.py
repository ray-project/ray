from typing import List, Type

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators import Limit, Sort, TopN
from ray.data._internal.logical.rules.combine_shuffles import CombineShuffles
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data.context import ShuffleStrategy


class TopNRewriteRule(Rule):
    """Rewrite eligible Sort + Limit and TopN + Limit plans into one TopN."""

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        # Normalize non-adjacent limits and consecutive sorts before matching TopN.
        return [LimitPushdownRule, CombineShuffles]

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, LogicalPlan)
        if plan.context.shuffle_strategy != ShuffleStrategy.SHUFFLE_V2:
            return plan
        if not plan.context.enable_top_n_rewrite:
            return plan

        max_limit = plan.context.top_n_rewrite_max_limit
        if type(max_limit) is not int or max_limit <= 0:
            raise ValueError(
                "DataContext.top_n_rewrite_max_limit must be a positive integer, "
                f"got {max_limit!r}."
            )

        def rewrite(operator: LogicalOperator) -> LogicalOperator:
            if not isinstance(operator, Limit):
                return operator
            if operator.limit <= 0:
                return operator

            input_operator = operator.input_dependencies[0]
            if isinstance(input_operator, TopN):
                return TopN(
                    sort_key=input_operator.sort_key,
                    limit=min(operator.limit, input_operator.limit),
                    input_dependencies=input_operator.input_dependencies,
                    ray_remote_args=input_operator.ray_remote_args,
                )

            if not isinstance(input_operator, Sort):
                return operator
            if operator.limit > max_limit:
                return operator
            if input_operator.sort_key.boundaries:
                return operator

            return TopN(
                sort_key=input_operator.sort_key,
                limit=operator.limit,
                input_dependencies=input_operator.input_dependencies,
                ray_remote_args=input_operator.ray_remote_args,
            )

        optimized_dag = plan.dag._apply_transform(rewrite)
        return LogicalPlan(dag=optimized_dag, context=plan.context)
