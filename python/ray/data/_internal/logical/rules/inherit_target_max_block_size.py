from ray.data._internal.logical.interfaces import PhysicalPlan, Rule


class InheritTargetMaxBlockSizeRule(Rule):
    """For each op that has overridden the default target max block size,
    propagate to upstream ops until we reach an op that has also overridden the
    target max block size."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        target_max_block_size = None
        dag = plan.dag
        while True:
            if dag.target_max_block_size is not None:
                # Set the target block size to inherit for
                # upstream ops.
                target_max_block_size = dag.target_max_block_size
            elif target_max_block_size is not None:
                # Inherit from downstream op.
                dag.target_max_block_size = target_max_block_size

            if len(dag.input_dependencies) != 1:
                break
            dag = dag.input_dependencies[0]
        return plan
