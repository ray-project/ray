from typing import List

from .logical_operator import LogicalOperator
from .plan import Plan


class LogicalPlan(Plan):
    """The plan with a DAG of logical operators."""

    def __init__(self, dag: LogicalOperator):
        self._dag = dag

    @property
    def dag(self) -> LogicalOperator:
        """Get the DAG of logical operators."""
        return self._dag

    def sources(self) -> List[LogicalOperator]:
        """List of operators that are sources for this plan's DAG."""
        # If an operator has no input dependencies, it's a source.
        if not any(self._dag.input_dependencies):
            return [self._dag]

        sources = []
        for op in self._dag.input_dependencies:
            sources.extend(LogicalPlan(op).sources())
        return sources
