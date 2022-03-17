from typing import Any

from ray import workflow

from ray.experimental.dag import DAGNode, FunctionNode, InputNode
from ray.experimental.dag.input_node import InputAtrributeNode, DAGInputData


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
            workflow_step = workflow.step(node._body).options(
                **node._bound_options, _resolve_like_object_ref_in_args=True
            )
            wf = workflow_step.step(*node._bound_args, **node._bound_kwargs)
            return wf
        if isinstance(node, InputAtrributeNode):
            return node._execute_impl()  # get data from input node
        if isinstance(node, InputNode):
            return input_context  # replace input node with input data
        if not isinstance(node, DAGNode):
            return node  # return normal objects
        raise TypeError(f"Unsupported DAG node: {node}")

    return dag_node.apply_recursive(_node_visitor)
