from typing import Any, Dict, Optional, List, Tuple, Union

from ray.experimental.dag import DAGNode, InputNode
from ray.serve.api import Deployment
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY
from ray.experimental.dag.format_utils import get_dag_node_str


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        deployment,
        cls_args: Tuple[Any],
        cls_kwargs: Dict[str, Any],
        cls_options: Dict[str, Any],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        self._body: Deployment = deployment
        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._deployment_handle: Union[
            RayServeHandle, RayServeSyncHandle
        ] = self._get_serve_deployment_handle(deployment, other_args_to_resolve)

        if self._contains_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in the DeploymentNode constructor because it is not available "
                "at class construction or binding time."
            )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args):
        """Executor of DeploymentNode by ray.remote()"""
        return self._deployment_handle.options(**self._bound_options).remote(
            *self._bound_args, **self._bound_kwargs
        )

    def _get_serve_deployment_handle(
        self,
        deployment: Deployment,
        bound_other_args_to_resolve: Dict[str, Any],
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """
        Return a sync or async handle of the encapsulated Deployment based on
        config.

        Args:
            deployment (Deployment): Deployment instance wrapped in the DAGNode.
            bound_other_args_to_resolve (Dict[str, Any]): Contains args used
                to configure DeploymentNode.

        Returns:
            RayServeHandle: Default and catch-all is to return sync handle.
                return async handle only if user explicitly set
                USE_SYNC_HANDLE_KEY with value of False.
        """
        if USE_SYNC_HANDLE_KEY not in bound_other_args_to_resolve:
            # Return sync RayServeSyncHandle
            return deployment.get_handle(sync=True)
        elif bound_other_args_to_resolve.get(USE_SYNC_HANDLE_KEY) is True:
            # Return sync RayServeSyncHandle
            return deployment.get_handle(sync=True)
        elif bound_other_args_to_resolve.get(USE_SYNC_HANDLE_KEY) is False:
            # Return async RayServeHandle
            return deployment.get_handle(sync=False)
        else:
            raise ValueError(
                f"{USE_SYNC_HANDLE_KEY} should only be set with a boolean value."
            )

    def _contains_input_node(self) -> bool:
        """Check if InputNode is used in children DAGNodes with current node
        as the root.
        """
        children_dag_nodes = self._get_all_child_nodes()
        for child in children_dag_nodes:
            if isinstance(child, InputNode):
                return True
        return False

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._body.func_or_class, method_name)
        call_node = DeploymentMethodNode(
            self._body,
            method_name,
            (),
            {},
            {},
            other_args_to_resolve=self._bound_other_args_to_resolve,
        )
        return call_node

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))
