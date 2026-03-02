from typing import TYPE_CHECKING, List, Optional

from .logical_operator import LogicalOperator
from .plan import Plan

if TYPE_CHECKING:
    from ray.data.context import DataContext


class LogicalPlan(Plan):
    """The plan with a DAG of logical operators."""

    def __init__(self, dag: LogicalOperator, context: "DataContext"):
        super().__init__(context)
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
            sources.extend(LogicalPlan(op, self._context).sources())
        return sources

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        from ray.data._internal.logical.operators import Read

        return all(isinstance(op, Read) for op in self.sources())

    def require_preserve_order(self) -> bool:
        """Whether this plan requires to preserve order."""
        from ray.data._internal.logical.operators import Zip

        return any(isinstance(op, Zip) for op in self.dag.post_order_iter())

    def input_files(self) -> Optional[List[str]]:
        """Get the input files of the dataset, if available."""
        return self.dag.infer_metadata().input_files

    def initial_num_blocks(self) -> Optional[int]:
        """Get the estimated number of blocks from the logical plan
        after applying execution plan optimizations, but prior to
        fully executing the dataset."""
        return self.dag.estimated_num_outputs()
