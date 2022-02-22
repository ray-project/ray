from typing import Any, Dict, Tuple, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.serve.api import Deployment
from ray.serve.handle import RayServeHandle

class DeploymentMethodNode(DAGNode):
    """Represents a deployment method invocation in a Ray function DAG."""

    def __init__(
        self,
        deployment_body: Deployment,
        deployment_name: str,
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
    ):
        self._body = deployment_body
        self._deployment_name: str = deployment_name
        self._method_name: str = method_name
        self._deployment_handle: RayServeHandle = deployment_body.get_handle()

        self._bound_args = method_args or []
        self._bound_kwargs = method_kwargs or {}
        self._bound_options = method_options or {}

        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve={},
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentMethodNode(
            self._body,
            self._deployment_name,
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
        )

    def _execute_impl(self, *args):
        """Executor of DeploymentMethodNode by ray.remote()"""
        # Execute with bound args.
        method_body = getattr(self._deployment_handle, self._method_name)

        return method_body.options(**self._bound_options).remote(
            *self._bound_args,
            **self._bound_kwargs,
        )


    def __str__(self) -> str:
        return get_dag_node_str(
            self,
            str(self._method_name) + "() @ " + str(self._body)
        )