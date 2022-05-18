from typing import Any

from ray.workflow.common import WORKFLOW_OPTIONS, WorkflowStepRuntimeOptions, StepType

from ray.experimental.dag import DAGNode, FunctionNode, InputNode
from ray.experimental.dag.input_node import InputAttributeNode, DAGInputData


def _make_workflow_step_function(node: FunctionNode):
    from ray.workflow.step_function import WorkflowStepFunction

    bound_options = node._bound_options.copy()
    workflow_options = bound_options.pop("_metadata", {}).get(WORKFLOW_OPTIONS, {})
    # "_resolve_like_object_ref_in_args" indicates we should resolve the
    # workflow like an ObjectRef, when included in the arguments of
    # another workflow.
    bound_options["_resolve_like_object_ref_in_args"] = True
    step_options = WorkflowStepRuntimeOptions.make(
        step_type=StepType.FUNCTION,
        catch_exceptions=workflow_options.get("catch_exceptions", None),
        max_retries=workflow_options.get("max_retries", None),
        allow_inplace=workflow_options.get("allow_inplace", False),
        checkpoint=workflow_options.get("checkpoint", None),
        ray_options=bound_options,
    )

    return WorkflowStepFunction(
        node._body,
        step_options=step_options,
        name=workflow_options.get("name", None),
        metadata=workflow_options.pop("metadata", None),
    )


def transform_ray_dag_to_workflow(dag_node: DAGNode, input_context: DAGInputData):
    """
    Transform a Ray DAG to a workflow. Map FunctionNode to workflow step with
    the workflow decorator.

    Args:
        dag_node: The DAG to be converted to a workflow.
        input_context: The input data that wraps varibles for the input node of the DAG.
    """
    if not isinstance(dag_node, FunctionNode):
        raise TypeError("Currently workflow does not support classes as DAG inputs.")

    def _node_visitor(node: Any) -> Any:
        if isinstance(node, FunctionNode):
            # "_resolve_like_object_ref_in_args" indicates we should resolve the
            # workflow like an ObjectRef, when included in the arguments of
            # another workflow.
            workflow_step = _make_workflow_step_function(node)
            wf = workflow_step.step(*node._bound_args, **node._bound_kwargs)
            return wf
        if isinstance(node, InputAttributeNode):
            return node._execute_impl()  # get data from input node
        if isinstance(node, InputNode):
            return input_context  # replace input node with input data
        if not isinstance(node, DAGNode):
            return node  # return normal objects
        raise TypeError(f"Unsupported DAG node: {node}")

    return dag_node.apply_recursive(_node_visitor)
