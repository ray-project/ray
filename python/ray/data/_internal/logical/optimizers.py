from typing import List

from ray.data._internal.logical.interfaces import (
    LogicalPlan,
    Optimizer,
    PhysicalPlan,
    Rule,
)
from ray.data._internal.logical.rules import (
    OperatorFusionRule,
    ReorderRandomizeBlocksRule,
)
from ray.data._internal.planner.planner import Planner

# TODO(scottjlee): add back LimitPushdownRule once we
# enforce number of input/output rows remains the same
# for Map/MapBatches ops.
LOGICAL_OPTIMIZER_RULES = [
    ReorderRandomizeBlocksRule,
]

PHYSICAL_OPTIMIZER_RULES = [
    OperatorFusionRule,
]


class LogicalOptimizer(Optimizer):
    """The optimizer for logical operators."""

    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in LOGICAL_OPTIMIZER_RULES]


class PhysicalOptimizer(Optimizer):
    """The optimizer for physical operators."""

    @property
    def rules(self) -> List["Rule"]:
        return [rule_cls() for rule_cls in PHYSICAL_OPTIMIZER_RULES]


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    logical_plan._dag = optimized_logical_plan.dag
    physical_plan = Planner().plan(optimized_logical_plan)
    return PhysicalOptimizer().optimize(physical_plan)
