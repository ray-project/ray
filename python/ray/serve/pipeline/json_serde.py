from typing import Any, Dict, Tuple
from importlib import import_module

import json

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    FunctionNode,
)
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.utils import parse_import_path


def dagnode_to_json(dag_node: DAGNode):
    """JSON serialization is only used and enforced in ray serve from ray core
    API authored DAGNode(s).
    """
    try:
        # Base case if DAGNode inputs are JSON serializable
        return json.dumps(dag_node)
    except (TypeError, OverflowError):

        if not isinstance(dag_node, DAGNode):
            # TODO: (jiaodong) Figure out a nicer way to capture deeply nested args without hacking DAGNode class implementation
            if isinstance(dag_node, Tuple):
                elements = []
                for ele in dag_node:
                    elements.append(dagnode_to_json(ele))
                return json.dumps(tuple(elements))
        # Post-order JSON serialization
        else:
            args = dag_node.get_args()
            kwargs = dag_node.get_kwargs()
            options = dag_node.get_options()
            other_args_to_resolve = dag_node.get_other_args_to_resolve()

            # stable_uuid will be re-generated upon new constructor execution
            result_dict = {}
            import_path = ""
            if isinstance(dag_node, ClassNode):
                body = dag_node._body.__ray_actor_class__
                import_path = f"{body.__module__}.{body.__qualname__}"
            elif isinstance(dag_node, FunctionNode):
                body = dag_node._body
                import_path = f"{body.__module__}.{body.__qualname__}"
            elif isinstance(dag_node, DeploymentMethodNode):
                body = dag_node._body._func_or_class.__ray_actor_class__
                import_path = f"{body.__module__}.{body.__qualname__}"
                other_args_to_resolve = {
                    "deployment_name": dag_node._deployment_name,
                    "method_name": dag_node._method_name
                }
                result_dict.update(other_args_to_resolve)

            # TODO:(jiaodong) Use cache for idential objects
            result_dict.update({
                "type": dag_node.__class__.__name__,
                # TODO: (jiaodong) Support runtime_env with remote working_dir
                "import_path": import_path,
                "args": dagnode_to_json(args),
                "kwargs": dagnode_to_json(kwargs),
            })

            return result_dict


def dagnode_from_json(input_json: Dict[str, Any]) -> DAGNode:
    """JSON serialization is only used and enforced in ray serve from ray core
    API authored DAGNode(s).
    """
    # Pre-order JSON deserialization
    args = json.loads(input_json["args"])
    kwargs = json.loads(input_json["kwargs"])
    # options =
    # other_args_to_resolve =
    # TODO:(jiaodong) Use cache for idential objects
    module_name, attr_name = parse_import_path(input_json["import_path"])
    module = getattr(import_module(module_name), attr_name)

    if input_json["type"] == FunctionNode.__name__:
        # TODO: (jiaodong) Don't hard code this for prod
        return FunctionNode(
            module._function,
            args,
            kwargs,
            {},
            {},
        )
    elif input_json["type"] == ClassNode.__name__:
        return ClassNode(
            module.__ray_metadata__.modified_class,
            args,
            kwargs,
            {},
            {},
        )
    elif input_json["type"] == DeploymentMethodNode.__name__:
        pass
