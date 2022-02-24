from typing import Any, Dict, Optional, Tuple, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.serve.api import Deployment
from ray.serve.handle import RayServeSyncHandle, RayServeHandle


class DeploymentMethodNode(DAGNode):
    """Represents a deployment method invocation of a DeploymentNode in DAG."""

    def __init__(
        self,
        deployment: Deployment,
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        self._body = deployment
        self._method_name: str = method_name
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )
        # Serve handle is sync by default.
        if (
            "sync_handle" in self._bound_other_args_to_resolve
            and self._bound_other_args_to_resolve.get("sync_handle") is False
        ):
            self._deployment_handle: RayServeHandle = deployment.get_handle(sync=False)
        else:
            self._deployment_handle: RayServeSyncHandle = deployment.get_handle(
                sync=True
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
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
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
            self, str(self._method_name) + "() @ " + str(self._body)
        )
