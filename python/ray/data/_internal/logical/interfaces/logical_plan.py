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
