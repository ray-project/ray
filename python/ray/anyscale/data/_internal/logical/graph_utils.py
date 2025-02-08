import copy

from ray.data._internal.logical.interfaces import LogicalPlan, Operator


def add_op_between(
    new_op: Operator,
    upstream_op: Operator,
    downstream_op: Operator,
):
    """Insert a new operator between existing upstream and downstream operators.

    Args:
        new_op: The operator to insert
        upstream_op: The operator that should precede new_op
        downstream_op: The operator that should follow new_op
    """
    # Set new_op's dependencies
    new_op._output_dependencies = [downstream_op]

    # Update upstream operator's outputs
    upstream_op._output_dependencies = [
        new_op if op_ is downstream_op else op_
        for op_ in upstream_op.output_dependencies
    ]

    # Update downstream operator's inputs
    downstream_op._input_dependencies = [
        new_op if op_ is upstream_op else op_
        for op_ in downstream_op.input_dependencies
    ]


def remove_op(op: Operator, plan: LogicalPlan) -> LogicalPlan:
    """Remove an operator from the graph, connecting its inputs to its outputs.

    Args:
        op: The operator to remove
        plan: The logical plan to modify

    Returns:
        Modified logical plan
    """
    if not op.input_dependencies:
        raise ValueError("Cannot remove operator with no inputs")

    if len(op.input_dependencies) > 1:
        raise ValueError("Operator with multiple inputs not supported")

    upstream_op = op.input_dependency

    # Connect upstream to downstream, skipping the removed op
    upstream_op._output_dependencies = [
        out_op for out_op in upstream_op.output_dependencies if out_op is not op
    ] + op.output_dependencies

    # Update all downstream ops to point to upstream
    for downstream_op in op.output_dependencies:
        downstream_op._input_dependencies = [
            upstream_op if in_op is op else in_op
            for in_op in downstream_op.input_dependencies
        ]

    # Update plan.dag if needed
    if plan.dag is op:
        plan = LogicalPlan(upstream_op, plan.context)
    return plan


def make_copy_of_dag(
    op: Operator,
    original_downstream_op: Operator = None,
    copied_downstream_op: Operator = None,
) -> Operator:
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
        make_copy_of_dag(current_input, op, op_copy)
        for current_input in op.input_dependencies
    ]
    op_copy._input_dependencies = input_deps
    return op_copy
