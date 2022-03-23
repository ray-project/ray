from typing import Any, Union
from importlib import import_module

import json

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    FunctionNode,
    InputNode,
    InputAtrributeNode,
    DAGNODE_TYPE_KEY,
)
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_function_node import DeploymentFunctionNode
from ray.serve.schema import (
    DeploymentSchema,
)
from ray.serve.utils import parse_import_path
from ray.serve.handle import (
    HandleOptions,
    RayServeHandle,
    RayServeLazySyncHandle,
    serve_handle_to_json_dict,
    serve_handle_from_json_dict,
)
from ray.serve.constants import SERVE_HANDLE_JSON_KEY
from ray.serve.api import RayServeDAGHandle


def convert_to_json_safe_obj(obj: Any, *, err_key: str) -> Any:
    # XXX: comment, err msg
    return json.loads(json.dumps(obj, cls=DAGNodeEncoder))


def convert_from_json_safe_obj(obj: Any) -> Any:
    # XXX: comment, err msg
    return json.loads(json.dumps(obj), object_hook=dagnode_from_json)


class DAGNodeEncoder(json.JSONEncoder):
    """
    Custom JSON serializer for DAGNode type that takes care of RayServeHandle
    used in deployment init args or kwargs, as well as all other DAGNode types
    with potentially deeply nested structure with other DAGNode instances.

    Enforcements:
        - All args, kwargs and other_args_to_resolve used in Ray DAG needs to
            be JSON serializable in order to be converted and deployed using
            Ray Serve.
        - All modules such as class or functions need to be visible and
            importable on top of its file, and can be resolved via a fully
            qualified import_path.
        - No DAGNode instance should appear in bound .options(), which should be
            JSON serializable with default encoder.
    """

    def default(self, obj):
        if isinstance(obj, DeploymentSchema):
            return {
                DAGNODE_TYPE_KEY: "DeploymentSchema",
                "schema": obj.dict(),
            }
        elif isinstance(obj, RayServeHandle):
            return serve_handle_to_json_dict(obj)
        elif isinstance(obj, RayServeDAGHandle):
            # TODO(simon) Do a proper encoder
            return {
                DAGNODE_TYPE_KEY: RayServeDAGHandle.__name__,
                "dag_node_json": obj.dag_node_json,
            }
        elif isinstance(obj, RayServeLazySyncHandle):
            return {
                DAGNODE_TYPE_KEY: RayServeLazySyncHandle.__name__,
                "deployment_name": obj.deployment_name,
                "handle_options_method_name": obj.handle_options.method_name,
            }
        # For all other DAGNode types.
        elif isinstance(obj, DAGNode):
            return obj.to_json(DAGNodeEncoder)
        else:
            # Let the base class default method raise the TypeError
            try:
                return json.JSONEncoder.default(self, obj)
            except Exception as e:
                raise TypeError(
                    "All args and kwargs used in Ray DAG building for serve "
                    "deployment need to be JSON serializable. Please JSON "
                    "serialize your args to make your ray application "
                    "deployment ready."
                    f"\n Original exception message: {e}"
                )


def dagnode_from_json(input_json: Any) -> Union[DAGNode, RayServeHandle, Any]:
    """
    Decode a DAGNode from given input json dictionary. JSON serialization is
    only used and enforced in ray serve from ray core API authored DAGNode(s).

    Covers both RayServeHandle and DAGNode types.

    Assumptions:
        - User object's JSON dict does not have keys that collide with our
            reserved DAGNODE_TYPE_KEY
        - RayServeHandle and Deployment can be re-constructed without losing
            states needed for their functionality or correctness.
        - DAGNode type can be re-constructed with new stable_uuid upon each
            deserialization without effective correctness of execution.
            - Only exception is ClassNode used as parent of ClassMethodNode
                that we perserve the same parent node.
        - .options() does not contain any DAGNode type
    """
    # Deserialize RayServeHandle type
    if SERVE_HANDLE_JSON_KEY in input_json:
        return serve_handle_from_json_dict(input_json)
    # Base case for plain objects
    elif DAGNODE_TYPE_KEY not in input_json:
        return input_json
    elif input_json[DAGNODE_TYPE_KEY] == RayServeDAGHandle.__name__:
        return RayServeDAGHandle(input_json["dag_node_json"])
    elif input_json[DAGNODE_TYPE_KEY] == "DeploymentSchema":
        return DeploymentSchema.parse_obj(input_json["schema"])
    elif input_json[DAGNODE_TYPE_KEY] == RayServeLazySyncHandle.__name__:
        return RayServeLazySyncHandle(
            input_json["deployment_name"],
            HandleOptions(input_json["handle_options_method_name"]),
        )
    # Deserialize DAGNode type
    elif input_json[DAGNODE_TYPE_KEY] == InputNode.__name__:
        return InputNode.from_json(input_json, object_hook=dagnode_from_json)
    elif input_json[DAGNODE_TYPE_KEY] == InputAtrributeNode.__name__:
        return InputAtrributeNode.from_json(input_json, object_hook=dagnode_from_json)
    elif input_json[DAGNODE_TYPE_KEY] == ClassMethodNode.__name__:
        return ClassMethodNode.from_json(input_json, object_hook=dagnode_from_json)
    elif input_json[DAGNODE_TYPE_KEY] == DeploymentNode.__name__:
        return DeploymentNode.from_json(input_json, object_hook=dagnode_from_json)
    elif input_json[DAGNODE_TYPE_KEY] == DeploymentMethodNode.__name__:
        return DeploymentMethodNode.from_json(input_json, object_hook=dagnode_from_json)
    elif input_json[DAGNODE_TYPE_KEY] == DeploymentFunctionNode.__name__:
        return DeploymentFunctionNode.from_json(
            input_json, object_hook=dagnode_from_json
        )
    else:
        # Class and Function nodes require original module as body.
        module_name, attr_name = parse_import_path(input_json["import_path"])
        module = getattr(import_module(module_name), attr_name)
        if input_json[DAGNODE_TYPE_KEY] == FunctionNode.__name__:
            return FunctionNode.from_json(
                input_json, module, object_hook=dagnode_from_json
            )
        elif input_json[DAGNODE_TYPE_KEY] == ClassNode.__name__:
            return ClassNode.from_json(
                input_json, module, object_hook=dagnode_from_json
            )
