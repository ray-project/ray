from dataclasses import replace

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators import Join

__all__ = [
    "ExtractEquiJoinKeys",
]


class ExtractEquiJoinKeys(Rule):
    """Turn the equality conjuncts of a ``Join.condition`` into hash keys.

    ``join(on=("l_suppkey",), right_on=("s_suppkey",),
    condition=col("c_nationkey") == col("s_nationkey"))`` becomes a join on
    ``("l_suppkey", "c_nationkey") = ("s_suppkey", "s_nationkey")``, so the
    second equality is enforced by the hash partitioning instead of being
    checked after every probe match has been materialized. The remainder stays
    on the operator as the residual condition (ray-project/ray#66835).
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        dag = plan.dag
        new_dag = dag._apply_transform(self._extract_keys)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _extract_keys(cls, op: LogicalOperator) -> LogicalOperator:
        if not isinstance(op, Join) or op.condition is None:
            return op
        resolved = op.resolve_condition()
        if resolved is None:
            return op
        residual = resolved.residual
        if resolved.left_key_columns == op.left_key_columns and (
            residual is not None and residual.structurally_equals(op.condition)
        ):
            return op
        return replace(
            op,
            left_input_op=op.input_dependencies[0],
            right_input_op=op.input_dependencies[1],
            left_key_columns=resolved.left_key_columns,
            right_key_columns=resolved.right_key_columns,
            condition=residual,
        )
