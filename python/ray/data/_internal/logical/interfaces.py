from typing import List

from ray.data._internal.execution.interfaces import Operator, PhysicalOperator
from ray.data._internal.execution.legacy_compat import (
    _blocks_to_input_buffer,
    _stage_to_operator,
)
from ray.data._internal.plan import Stage


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(self, name: str, input_dependencies: List["LogicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x

    def set_input_dependencies(self, input_dependencies: List["LogicalOperator"]):
        self._input_dependencies = input_dependencies

    def set_legacy_stage(self, stage: Stage):
        self._stage = stage


class LogicalPlan:
    """The plan with a DAG of logical operators."""

    def __init__(self, dag: LogicalOperator):
        self._dag = dag

    def add(self, operator: LogicalOperator) -> "LogicalPlan":
        """Add the operator into DAG of logical operators."""
        assert self._dag is not None
        operator.set_input_dependencies([self._dag])
        self._dag = operator
        return self

    def get_execution_dag(self) -> PhysicalOperator:
        """Get the DAG of physical operators to execute.

        This process has 3 steps:
        (1).logical optimization: optimize logical plan.
        (2).planning: convert logical to physical plan.
        (3).physical optimization: optimize physical plan.
        """
        logical_optimizer = LogicalOptimizer()
        optimized_plan = logical_optimizer.optimize(self)
        physical_plan = logical_optimizer.plan(optimized_plan)
        final_plan = PhysicalOptimizer().optimize(physical_plan)
        return final_plan._dag


class PhysicalPlan:
    """The plan with a DAG of physical operators."""

    def __init__(self, dag: PhysicalOperator):
        self._dag = dag


class LogicalOptimizer:
    """The optimizer for logical plan."""

    def __init__(self):
        self._rules = []

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """Optimize logical plan with a list of rules."""
        for rule in self._rules:
            plan = rule.apply(plan)
        return plan

    def plan(self, logical_plan: LogicalPlan) -> PhysicalPlan:
        """Convert logical plan into physical plan."""
        return PhysicalPlan(self._plan_op(logical_plan._dag))

    def _plan_op(self, op: LogicalOperator) -> PhysicalOperator:
        """Convert logical operators to physical operators in post-order."""
        # Plan the input dependencies first.
        physical_children = []
        for child in op.input_dependencies:
            physical_children.append(self._plan_op(child))

        if op.name == "Read":
            assert not physical_children
            physical_op = _blocks_to_input_buffer(op._blocks, owns_blocks=True)
        elif op.name == "MapBatches":
            assert len(physical_children) == 1
            physical_op = _stage_to_operator(op._stage, physical_children[0])
        else:
            raise ValueError(f"Found unknown logical operator during planning: {op}")
        return physical_op


class PhysicalOptimizer:
    """The optimizer for physical plan."""

    def __init__(self):
        self._rules = []

    def optimize(self, plan: PhysicalPlan) -> PhysicalPlan:
        """Optimize physical plan with a list of rules."""
        for rule in self._rules:
            plan = rule.apply(plan)
        return plan


class LogicalRule:
    """Abstract optimization rule for logical plan."""

    def apply(plan: LogicalPlan) -> LogicalPlan:
        raise NotImplementedError


class PhysicalRule:
    """Abstract optimization rule for physical plan."""

    def apply(plan: PhysicalPlan) -> PhysicalPlan:
        raise NotImplementedError
