from typing import Any, Dict, Tuple
from importlib import import_module

import json

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    FunctionNode,
    InputNode,
)
from ray.serve.api import Deployment, DeploymentConfig
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.utils import parse_import_path

# Reserved key to distinguish DAGNode type and avoid collision with user dict.
DAGNODE_TYPE_KEY = "__serve_pipeline_dag_node_type__"

class DAGNodeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DAGNode):
            dag_node = obj
            args = dag_node.get_args()
            kwargs = dag_node.get_kwargs()
            # options = dag_node.get_options()
            # other_args_to_resolve = dag_node.get_other_args_to_resolve()

            # stable_uuid will be re-generated upon new constructor execution
            result_dict = {
                DAGNODE_TYPE_KEY: dag_node.__class__.__name__
            }

            if isinstance(dag_node, InputNode):
                return result_dict

            # TODO: (jiaodong) Handle __main__ swap to filename
            import_path = ""
            if isinstance(dag_node, ClassNode):
                body = dag_node._body.__ray_actor_class__
            elif isinstance(dag_node, FunctionNode):
                body = dag_node._body
            elif isinstance(dag_node, DeploymentMethodNode):
                body = dag_node._body._func_or_class.__ray_actor_class__
                other_args_to_resolve = {
                    "deployment_name": dag_node._deployment_name,
                    "method_name": dag_node._method_name,
                    # TODO: (jiaodong) Support passing deployment handle
                    "deployment_init_args": json.dumps(dag_node._body.init_args, cls=DAGNodeEncoder),
                    "deployment_init_kwargs": json.dumps(dag_node._body.init_kwargs, cls=DAGNodeEncoder)
                }
                result_dict.update(other_args_to_resolve)

            # TODO:(jiaodong) Maybe use cache for idential objects
            result_dict.update({
                # TODO: (jiaodong) Support runtime_env with remote working_dir
                "import_path": f"{body.__module__}.{body.__qualname__}",
                "args": json.dumps(args, cls=DAGNodeEncoder),
                "kwargs": json.dumps(kwargs, cls=DAGNodeEncoder),
            })

            return result_dict
        else:
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)


def dagnode_from_json(input_json: Any) -> DAGNode:
    """JSON serialization is only used and enforced in ray serve from ray core
    API authored DAGNode(s).
    """
    # Base case
    if DAGNODE_TYPE_KEY not in input_json:
        return input_json
    try:
        # Check if input is JSON deserializable
        return json.loads(input_json)
    except Exception:
        if input_json[DAGNODE_TYPE_KEY] == InputNode.__name__:
            # TODO: (jiaodong) Support user passing inputs to InputNode in JSON
            return InputNode()

        # Pre-order JSON deserialization
        args = json.loads(input_json["args"], object_hook=dagnode_from_json)
        kwargs = json.loads(input_json["kwargs"], object_hook=dagnode_from_json)
        # options =
        # other_args_to_resolve =
        # TODO:(jiaodong) Use cache for idential objects
        module_name, attr_name = parse_import_path(input_json["import_path"])
        module = getattr(import_module(module_name), attr_name)
        if input_json[DAGNODE_TYPE_KEY] == FunctionNode.__name__:
            # TODO: (jiaodong) Don't hard code this for prod
            return FunctionNode(
                module._function,
                args,
                kwargs,
                {},
                {},
            )
        elif input_json[DAGNODE_TYPE_KEY] == ClassNode.__name__:
            return ClassNode(
                module.__ray_metadata__.modified_class,
                args,
                kwargs,
                {},
                {},
            )
        elif input_json[DAGNODE_TYPE_KEY] == DeploymentMethodNode.__name__:
            deployment_init_args = json.loads(input_json["deployment_init_args"], object_hook=dagnode_from_json)
            deployment_init_kwargs = json.loads(input_json["deployment_init_kwargs"], object_hook=dagnode_from_json)
            return DeploymentMethodNode(
                Deployment(
                    input_json["import_path"],
                    input_json["deployment_name"],
                    DeploymentConfig(),
                    init_args=tuple(deployment_init_args),
                    init_kwargs=deployment_init_kwargs,
                    _internal=True
                ),
                input_json["deployment_name"],
                input_json["method_name"],
                args,
                kwargs,
                {}
            )
