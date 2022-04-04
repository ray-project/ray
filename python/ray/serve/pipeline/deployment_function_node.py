import inspect
from typing import Any, Callable, Dict, List, Union
from ray import ObjectRef

from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.serve.api import schema_to_deployment
from ray.serve.deployment import Deployment
from ray.serve.config import DeploymentConfig
from ray.serve.schema import DeploymentSchema
from ray.serve.handle import RayServeLazySyncHandle
from ray.serve.utils import get_deployment_import_path


class DeploymentFunctionNode(DAGNode):
    """Represents a function node decorated by @serve.deployment in a serve DAG."""

    def __init__(
        self,
        func_body: Union[Callable, str],
        deployment_name,
        func_args,
        func_kwargs,
        func_options,
        other_args_to_resolve=None,
    ):
        self._body = func_body
        self._deployment_name = deployment_name
        super().__init__(
            func_args,
            func_kwargs,
            func_options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if "deployment_schema" in self._bound_other_args_to_resolve:
            deployment_schema: DeploymentSchema = self._bound_other_args_to_resolve[
                "deployment_schema"
            ]
            deployment_shell = schema_to_deployment(deployment_schema)

            # Prefer user specified name to override the generated one.
            if (
                inspect.isfunction(func_body)
                and deployment_shell.name != func_body.__name__
            ):
                self._deployment_name = deployment_shell.name

            # Set the route prefix, prefer the one user supplied,
            # otherwise set it to /deployment_name
            if (
                deployment_shell.route_prefix is None
                or deployment_shell.route_prefix != f"/{deployment_shell.name}"
            ):
                route_prefix = deployment_shell.route_prefix
            else:
                route_prefix = f"/{deployment_name}"

            self._deployment = deployment_shell.options(
                func_or_class=func_body,
                name=self._deployment_name,
                init_args=(),
                init_kwargs=dict(),
                route_prefix=route_prefix,
            )
        else:
            self._deployment: Deployment = Deployment(
                func_body,
                deployment_name,
                DeploymentConfig(),
                init_args=tuple(),
                init_kwargs=dict(),
                ray_actor_options=func_options,
                _internal=True,
            )
        # TODO (jiaodong): Polish with async handle support later
        self._deployment_handle = RayServeLazySyncHandle(deployment_name)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentFunctionNode(
            self._body,
            self._deployment_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs) -> ObjectRef:
        """Executor of DeploymentFunctionNode by calling .remote() on the
        deployment handle.

        Deployment method always default to __call__.
        """
        return self._deployment_handle.remote(*self._bound_args, **self._bound_kwargs)

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))

    def get_deployment_name(self):
        return self._deployment_name

    def get_import_path(self):
        if (
            "is_from_serve_deployment" in self._bound_other_args_to_resolve
        ):  # built by serve top level api, this is ignored for serve.run
            return "dummy"
        return get_deployment_import_path(self._deployment)

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, DeploymentFunctionNode.__name__)
        json_dict["import_path"] = self.get_import_path()
        json_dict["deployment_name"] = self.get_deployment_name()
        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentFunctionNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        node = cls(
            input_json["import_path"],
            input_json["deployment_name"],
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
        node._stable_uuid = args_dict["uuid"]
        return node
