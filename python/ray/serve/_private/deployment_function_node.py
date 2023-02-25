import inspect
from typing import Any, Callable, Dict, List, Union

from ray.dag.dag_node import DAGNode
from ray.dag.format_utils import get_dag_node_str
from ray.serve.deployment import Deployment, schema_to_deployment
from ray.serve.config import DeploymentConfig
from ray.serve.handle import RayServeDeploymentHandle
from ray.serve.schema import DeploymentSchema


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
                init_kwargs={},
                route_prefix=route_prefix,
                _internal=True,
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
        self._deployment_handle = RayServeDeploymentHandle(self._deployment.name)

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

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))

    def get_deployment_name(self):
        return self._deployment_name
