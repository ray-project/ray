from typing import Any, Dict, List

import ray
from ray import workflow

from ray.experimental.dag import DAGNode, FunctionNode, InputNode
from ray.experimental.dag.input_node import InputAtrributeNode, DAGInputData


def _process_input_value(v: Any, input_context):
    if isinstance(v, FunctionNode):
        return _transform_dag_recursive(v, input_context)
    if isinstance(v, InputAtrributeNode):
        return input_context[v._key]
    if not isinstance(v, DAGNode):
        return v
    raise TypeError(f"Unsupported DAG node: {v}")


def _transform_dag_recursive(dag_node: FunctionNode, input_context: DAGInputData):
    func_body = dag_node._body
    workflow_step = workflow.step(func_body)
    workflow_step_with_options = workflow_step.options(ray_options=dag_node.get_options())
    args = []
    kwargs = {}
    for arg in dag_node.get_args():
        args.append(_process_input_value(arg, input_context))
    for k, v in dag_node.get_kwargs().items():
        kwargs[k] = _process_input_value(v, input_context)
    return workflow_step.step(*args, **kwargs)


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
    return _transform_dag_recursive(dag_node, input_context)
