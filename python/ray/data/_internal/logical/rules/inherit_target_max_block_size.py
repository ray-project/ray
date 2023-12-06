from typing import Optional

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule


class InheritTargetMaxBlockSizeRule(Rule):
    """For each op that has overridden the default target max block size,
    propagate to upstream ops until we reach an op that has also overridden the
    target max block size."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._propagate_target_max_block_size_to_upstream_ops(plan.dag)
        return plan

    def _propagate_target_max_block_size_to_upstream_ops(
        self, dag: PhysicalOperator, target_max_block_size: Optional[int] = None
    ):
        if dag.target_max_block_size is not None:
            # Set the target block size to inherit for
            # upstream ops.
            target_max_block_size = dag.target_max_block_size
        elif target_max_block_size is not None:
            # Inherit from downstream op.
            dag.set_target_max_block_size(target_max_block_size)

        for upstream_op in dag.input_dependencies:
            self._propagate_target_max_block_size_to_upstream_ops(
                upstream_op, target_max_block_size
            )
