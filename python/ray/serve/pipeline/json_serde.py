from typing import Any, Union
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
from ray.serve.handle import RayServeHandle
from ray.serve.utils import ServeHandleEncoder, serve_handle_object_hook
from ray.serve.constants import (
    SERVE_ASYNC_HANDLE_JSON_KEY,
    SERVE_SYNC_HANDLE_JSON_KEY,
)

# Reserved key to distinguish DAGNode type and avoid collision with user dict.
DAGNODE_TYPE_KEY = "__serve_pipeline_dag_node_type__"


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
        # TODO: (jiaodong) Have better abtraction for users to extend and use
        # JSON serialization of their own object types
        # For replaced deployment handles used as init args or kwargs.
        if isinstance(obj, RayServeHandle):
            return json.dumps(obj, cls=ServeHandleEncoder)
        # For all other DAGNode types.
        if isinstance(obj, DAGNode):
            dag_node = obj
            args = dag_node.get_args()
            kwargs = dag_node.get_kwargs()
            options = dag_node.get_options()
            other_args_to_resolve = dag_node.get_other_args_to_resolve()
            # stable_uuid will be re-generated upon new constructor execution
            # except for ClassNode used as parent of ClassMethodNode
            result_dict = {
                DAGNODE_TYPE_KEY: dag_node.__class__.__name__,
                # Placeholder for better ordering
                "import_path": "",
                "args": json.dumps(args, cls=DAGNodeEncoder),
                "kwargs": json.dumps(kwargs, cls=DAGNodeEncoder),
                # .options() should not contain any DAGNode type
                "options": json.dumps(options),
                "other_args_to_resolve": json.dumps(
                    other_args_to_resolve, cls=DAGNodeEncoder
                ),
            }
            # TODO: (jiaodong) Support arbitrary InputNode args and pydantic
            # input schema.
            import_path = ""
            if isinstance(dag_node, InputNode):
                return result_dict
            # TODO: (jiaodong) Handle __main__ swap to filename
            if isinstance(dag_node, ClassNode):
                body = dag_node._body.__ray_actor_class__
                import_path = f"{body.__module__}.{body.__qualname__}"
            elif isinstance(dag_node, ClassMethodNode):
                parent_class_node = dag_node._parent_class_node
                body = dag_node._parent_class_node._body.__ray_actor_class__
                import_path = f"{body.__module__}.{body.__qualname__}"
                result_dict.update(
                    {
                        "method_name": dag_node._method_name,
                        "parent_class_node_stable_uuid": parent_class_node._stable_uuid,
                    }
                )
            elif isinstance(dag_node, FunctionNode):
                body = dag_node._body
                import_path = f"{body.__module__}.{body.__qualname__}"
            elif isinstance(dag_node, (DeploymentNode, DeploymentMethodNode)):
                result_dict.update({"deployment_name": dag_node._deployment.name})
                if isinstance(dag_node._deployment._func_or_class, str):
                    # We're processing a deserilized JSON node where import_path
                    # is dag_node body.
                    import_path = dag_node._deployment._func_or_class
                else:
                    body = dag_node._deployment._func_or_class.__ray_actor_class__
                    import_path = f"{body.__module__}.{body.__qualname__}"
                if isinstance(dag_node, DeploymentMethodNode):
                    result_dict.update({"method_name": dag_node._method_name})

            # TODO: (jiaodong) Maybe use cache for idential objects
            # TODO: (jiaodong) Support runtime_env with remote working_dir
            result_dict["import_path"] = import_path

            return result_dict
        else:
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)


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
    # TODO: (jiaodong) Have better abtraction for users to extend and use
    # JSON serialization of their own object types
    # Deserialize a RayServeHandle.
    if (
        SERVE_SYNC_HANDLE_JSON_KEY in input_json
        or SERVE_ASYNC_HANDLE_JSON_KEY in input_json
    ):
        return json.loads(input_json, object_hook=serve_handle_object_hook)
    # Base case for plain objects.
    elif DAGNODE_TYPE_KEY not in input_json:
        return input_json

    if input_json[DAGNODE_TYPE_KEY] == InputNode.__name__:
        # TODO: (jiaodong) Support user passing inputs to InputNode in JSON
        return InputNode()

    # Post-order JSON deserialization
    args = json.loads(input_json["args"], object_hook=dagnode_from_json)
    kwargs = json.loads(input_json["kwargs"], object_hook=dagnode_from_json)
    # .options() should not contain any DAGNode type
    options = json.loads(input_json["options"])
    other_args_to_resolve = json.loads(
        input_json["other_args_to_resolve"], object_hook=dagnode_from_json
    )
    # TODO:(jiaodong) Use cache for idential objects
    module_name, attr_name = parse_import_path(input_json["import_path"])
    module = getattr(import_module(module_name), attr_name)
    if input_json[DAGNODE_TYPE_KEY] == FunctionNode.__name__:
        # TODO: (jiaodong) Don't hard code this for prod
        return FunctionNode(
            module._function,
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )
    elif input_json[DAGNODE_TYPE_KEY] == ClassNode.__name__:
        return ClassNode(
            module.__ray_metadata__.modified_class,
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )
    elif input_json[DAGNODE_TYPE_KEY] == ClassMethodNode.__name__:
        # For ClassNode types in Ray DAG we need to ensure all executions
        # re-uses the same ClassNode instance across all ClassMethodNode
        # calls, thus stable_uuid needs to be preserved to match ray DAG's
        # _copy implementation.
        node = ClassMethodNode(
            input_json["method_name"],
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )
        node._parent_class_node._stable_uuid = input_json[
            "parent_class_node_stable_uuid"
        ]
        return node
    elif input_json[DAGNODE_TYPE_KEY] == DeploymentNode.__name__:
        return DeploymentNode(
            input_json["import_path"],
            input_json["deployment_name"],
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )
    elif input_json[DAGNODE_TYPE_KEY] == DeploymentMethodNode.__name__:
        return DeploymentMethodNode(
            Deployment(
                input_json["import_path"],
                input_json["deployment_name"],
                # TODO: (jiaodong) Support deployment config from user input
                DeploymentConfig(),
                init_args=args,
                init_kwargs=kwargs,
                ray_actor_options=options,
                _internal=True,
            ),
            input_json["method_name"],
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )
    else:
        # Not supported DAGNode type, just return default deserialization
        return json.loads(input_json)
