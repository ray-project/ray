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
    SERVE_ASYNC_HANDLE_JSON_PREFIX_KEY,
    SERVE_SYNC_HANDLE_JSON_PREFIX_KEY,
)

# Reserved key to distinguish DAGNode type and avoid collision with user dict.
DAGNODE_TYPE_KEY = "__serve_pipeline_dag_node_type__"


class DAGNodeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, RayServeHandle):
            return json.dumps(obj, cls=ServeHandleEncoder)
        if isinstance(obj, DAGNode):
            dag_node = obj
            args = dag_node.get_args()
            kwargs = dag_node.get_kwargs()
            options = dag_node.get_options()
            other_args_to_resolve = dag_node.get_other_args_to_resolve()
            # stable_uuid will be re-generated upon new constructor execution
            result_dict = {
                DAGNODE_TYPE_KEY: dag_node.__class__.__name__,
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
            elif isinstance(dag_node, FunctionNode):
                body = dag_node._body
                import_path = f"{body.__module__}.{body.__qualname__}"
            elif isinstance(dag_node, (DeploymentNode, DeploymentMethodNode)):
                result_dict.update(
                    {"deployment_name": dag_node._deployment.name}
                )
                if isinstance(dag_node._deployment._func_or_class, str):
                    # We're processing a deserilized JSON node where import_path
                    # is dag_node body.
                    import_path = dag_node._deployment._func_or_class
                else:
                    body = (
                        dag_node._deployment._func_or_class.__ray_actor_class__
                    )
                    import_path = f"{body.__module__}.{body.__qualname__}"
                if isinstance(dag_node, DeploymentMethodNode):
                    result_dict.update({"method_name": dag_node._method_name})

            # TODO:(jiaodong) Maybe use cache for idential objects
            result_dict.update(
                # TODO: (jiaodong) Support runtime_env with remote working_dir
                {"import_path": import_path}
            )

            return result_dict
        else:
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)


def dagnode_from_json(input_json: Any) -> Union[DAGNode, RayServeHandle, Any]:
    """JSON serialization is only used and enforced in ray serve from ray core
    API authored DAGNode(s).
    """
    # Deserialize a RayServeHandle.
    if (
        SERVE_SYNC_HANDLE_JSON_PREFIX_KEY in input_json
        or SERVE_ASYNC_HANDLE_JSON_PREFIX_KEY in input_json
    ):
        return json.loads(input_json, object_hook=serve_handle_object_hook)
    # Base case for plain objects.
    elif DAGNODE_TYPE_KEY not in input_json:
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
                input_json["deployment_name"],
                input_json["method_name"],
                args,
                kwargs,
                options,
                other_args_to_resolve=other_args_to_resolve,
            )
