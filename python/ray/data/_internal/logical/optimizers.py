from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.interfaces import Rule, Optimizer, LogicalOperator
from ray.data._internal.logical.planner import Planner


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
        # TODO: Add physical optimizer rules.
        return []


class LogicalPlan:
    """The plan with a DAG of logical operators."""

    def __init__(self, dag: LogicalOperator):
        self._dag = dag

    @property
    def dag(self) -> LogicalOperator:
        """Get the DAG of logical operators."""
        return self._dag


def get_execution_dag(logical_dag: LogicalOperator) -> PhysicalOperator:
    """Get the DAG of physical operators to execute.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    optimized_logical_dag = LogicalOptimizer().optimize(logical_dag)
    physical_dag = Planner().plan(optimized_logical_dag)
    return PhysicalOptimizer().optimize(physical_dag)
