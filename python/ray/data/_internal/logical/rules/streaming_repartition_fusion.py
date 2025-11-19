from typing import Dict, List, Type

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalOperator, PhysicalPlan, Rule
from ray.data._internal.logical.rules.operator_fusion import FuseOperators


class StreamingRepartitionFusion(Rule):
    """Fuse streaming_repartition -> map_batches"""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map: Dict["PhysicalOperator", LogicalOperator] = plan.op_map.copy()
        dag: PhysicalOperator = plan.dag
        upstream_ops = dag.input_dependencies
        while len(upstream_ops) == 1 and isinstance(upstream_ops[0], MapOperator):
            dag = self._fuse_streaming_repartition_map_batches(dag, upstream_ops[0])
            upstream

    @classmethod
    def dependents(cls) -> List[Type[Rule]]:
        return [FuseOperators]
