from typing import TYPE_CHECKING, Dict

from .logical_operator import LogicalOperator
from .plan import Plan

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data.context import DataContext


class PhysicalPlan(Plan):
    """The plan with a DAG of physical operators."""

    def __init__(
        self,
        dag: "PhysicalOperator",
        op_map: Dict["PhysicalOperator", LogicalOperator],
        context: "DataContext",
    ):
        super().__init__(context)
        self._dag = dag
        self._op_map = op_map

    @property
    def dag(self) -> "PhysicalOperator":
        """Get the DAG of physical operators."""
        return self._dag

    @property
    def op_map(self) -> Dict["PhysicalOperator", LogicalOperator]:
        """
        Get a mapping from physical operators to their corresponding logical operator.
        """
        return self._op_map
