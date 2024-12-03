from ray.anyscale.data._internal.logical.graph_utils import make_copy_of_dag, remove_op
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule looks for `Project` operators that are immediately
    preceded by a `ReadFiles` operator and sets the
    projected columns on the `ReadFiles` operator.

    If there are redundant Project operators, it removes the `Project` operator from
    the graph.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag_copy = make_copy_of_dag(plan.dag)
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
                    plan = remove_op(op, plan)

            # TODO(rliaw): Support Filter once that is an op
            # https://github.com/anyscale/rayturbo/issues/1199
            elif isinstance(op, ReadFiles) and projecting_op:
                readfiles = op
                readfiles.columns = projecting_op.cols
                plan = remove_op(projecting_op, plan)
            else:
                # If it is not a select nor a Readfiles, reset the projecting_op
                # This means that Map, Filter, etc. will reset the projecting_op
                projecting_op = None
            ops += op.input_dependencies
        return plan
