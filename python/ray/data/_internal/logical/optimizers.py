from typing import List

from ray.data._internal.logical.interfaces import (
    Rule,
    Optimizer,
    LogicalPlan,
    PhysicalPlan,
)
from ray.data._internal.logical.rules import OperatorFusionRule
from ray.data._internal.planner.planner import Planner


class LogicalOptimizer(Optimizer):
    """The optimizer for logical operators."""

    @property
    def rules(self) -> List[Rule]:
        # TODO: Add logical optimizer rules.
        return []


class PhysicalOptimizer(Optimizer):
    """The optimizer for physical operators."""

    @property
    def rules(self) -> List["Rule"]:
        return [OperatorFusionRule()]


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    logical_plan = LogicalOptimizer().optimize(logical_plan)
    physical_plan = Planner().plan(logical_plan)
    return PhysicalOptimizer().optimize(physical_plan)
