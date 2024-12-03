from typing import TYPE_CHECKING, Optional

from ray.anyscale.data._internal.logical.graph_utils import (
    add_op_between,
    make_copy_of_dag,
    remove_op,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Filter
from ray.data._internal.logical.operators.n_ary_operator import Union

if TYPE_CHECKING:
    import pyarrow.dataset as pd


class PredicatePushdown(Rule):
    """Pushes down predicates across the graph.

    If Filter operators chaining is found with filter expression, combine the
    filter expressions and fuse the Filter operator.

    If Filter operator is found, we combine the filter expressions and
    pushdown the combined filter expression to the ReadFiles operator.

    If Union operator is found, we duplicate the filter expression for each branch
    and pushdown the combined filter expression to the ReadFiles operator.

    For read files operator, we set the filter expression on the read files operator.
    """

    # TODO: (srinathk) There is one more optimization to this filter rule, i.e.
    # if Filter ops are mixed with UserDefinedFunction and expressions, we can just
    # reorder all the Filter expressions and combine together and attempt pushdown into
    # `ReadFiles`.
    # Refer: https://anyscale1.atlassian.net/browse/DATA-229

    # TODO: (srinathk)
    # Annotate Logical plan to indicate predicate pushdown occured
    # https://anyscale1.atlassian.net/browse/DATA-244
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag_copy = make_copy_of_dag(plan.dag)
        plan = LogicalPlan(dag_copy, plan.context)
        plan = self._process_operator(plan.dag, None, None, plan)
        return plan

    def _process_operator(
        self,
        op: LogicalOperator,
        prev_filter: Optional[Filter] = None,
        filter_expr_to_pushdown: Optional["pd.Expression"] = None,
        plan: Optional[LogicalPlan] = None,
    ) -> LogicalPlan:
        """Process a sub-DAG rooted at the given operator to push down predicates.

        Args:
            op: The operator to process
            prev_filter: The filter to push down
            filter_expr_to_pushdown: The filter expression to pushdown
            plan: The logical plan

        Returns:
            The modified logical plan
        """
        if prev_filter is not None:
            assert filter_expr_to_pushdown is not None
        if isinstance(op, Filter):
            if prev_filter is None:
                if op.is_expression_based():
                    prev_filter = op
                    filter_expr_to_pushdown = op._filter_expr
            elif not op.is_expression_based():
                # Filter Op pushdown supported for only filter expressions
                # so we reset the filter pushdown here
                prev_filter = None
                filter_expr_to_pushdown = None
            else:
                # Opportunity to combine filter expressions
                filter_expr_to_pushdown &= op._filter_expr
                plan = remove_op(prev_filter, plan)
                prev_filter = op
                prev_filter._filter_expr = filter_expr_to_pushdown
        elif isinstance(op, ReadFiles) and prev_filter:
            assert filter_expr_to_pushdown is not None
            readfiles = op
            if readfiles.filter_expr is not None:
                filter_expr_to_pushdown &= readfiles.filter_expr
            readfiles.filter_expr = filter_expr_to_pushdown
            plan = remove_op(prev_filter, plan)
            prev_filter = None
            filter_expr_to_pushdown = None
        elif isinstance(op, Union) and prev_filter:
            # For union operations, we need to process each branch independently
            # So we duplicate the filter expression for each branch
            for input_dep in op.input_dependencies:
                # Create a new Filter operator for each branch
                branch_filter = Filter(input_dep, filter_expr=filter_expr_to_pushdown)
                add_op_between(
                    branch_filter,
                    upstream_op=input_dep,
                    downstream_op=op,
                )
                # Process the branch with the new filter
                plan = self._process_operator(
                    branch_filter, None, filter_expr_to_pushdown, plan
                )
            # Remove the original filter after processing all branches
            plan = remove_op(prev_filter, plan)
            return plan

        else:
            prev_filter = None
            filter_expr_to_pushdown = None
        # DFS traversal of the DAG
        for input_dep in op.input_dependencies:
            plan = self._process_operator(
                input_dep, prev_filter, filter_expr_to_pushdown, plan
            )
        return plan
