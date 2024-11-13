import copy

from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project


def _make_copy_of_dag(
    op: LogicalOperator,
    original_downstream_op: LogicalOperator = None,
    copied_downstream_op: LogicalOperator = None,
) -> LogicalOperator:
    # We should have better mutability semantics
    # https://github.com/ray-project/ray/pull/48620

    op_copy = copy.copy(op)  # shallow copy

    # If downstream op is provided, update the current
    # op's output dependencies to point to the copied downstream op
    if original_downstream_op:
        op_copy._output_dependencies = [
            copied_downstream_op if op_ is original_downstream_op else op_
            for op_ in op.output_dependencies
        ]

    # Work up the DAG recursively
    input_deps = [
        _make_copy_of_dag(current_input, op, op_copy)
        for current_input in op.input_dependencies
    ]
    op_copy._input_dependencies = input_deps
    return op_copy


def _remove_op_from_graph(op: Project, plan):
    upstream = op.input_dependency
    upstream._output_dependencies = op.output_dependencies

    for current_output in op.output_dependencies:
        current_output._input_dependencies = [
            upstream if op_ is op else op_ for op_ in current_output.input_dependencies
        ]

    # if Op is at the end of the chain, then update LogicalPlan Dag as well
    if plan.dag is op:
        plan = LogicalPlan(upstream, plan.context)
    return plan


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule looks for `Project` operators that are immediately
    preceded by a `ReadFiles` operator and sets the
    projected columns on the `ReadFiles` operator.

    If there are redundant Project operators, it removes the `Project` operator from
    the graph.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag_copy = _make_copy_of_dag(plan.dag)
        plan = LogicalPlan(dag_copy, plan.context)
        ops = [dag_copy]
        projecting_op = None

        while len(ops) > 0:
            op = ops.pop(0)
            if isinstance(op, Project):
                if not projecting_op:
                    projecting_op = op
                else:
                    is_subset = set(projecting_op.cols).issubset(set(op.cols))
                    if not is_subset:
                        raise RuntimeError(
                            "Identified projections where the latter is "
                            "not a subset of the former: "
                            f"{op.cols} -> {projecting_op.cols}"
                        )
                    plan = _remove_op_from_graph(op, plan)

            # TODO(rliaw): Support Filter once that is an op
            # https://github.com/anyscale/rayturbo/issues/1199
            elif isinstance(op, ReadFiles) and projecting_op:
                readfiles = op
                readfiles.columns = projecting_op.cols
                plan = _remove_op_from_graph(projecting_op, plan)
            else:
                # If it is not a select nor a Readfiles, reset the projecting_op
                # This means that Map, Filter, etc. will reset the projecting_op
                projecting_op = None
            ops += op.input_dependencies
        return plan
