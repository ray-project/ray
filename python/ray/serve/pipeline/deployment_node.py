from typing import Any, Dict, Optional, List, Tuple

from ray.experimental.dag import DAGNode, InputNode
from ray.serve.api import Deployment
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.experimental.dag.format_utils import get_dag_node_str


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        deployment_body,
        cls_args: Tuple[Any],
        cls_kwargs: Dict[str, Any],
        cls_options: Dict[str, Any],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        self._body: Deployment = deployment_body
        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )
        # Serve handle is sync by default.
        if (
            "sync_handle" in self._bound_other_args_to_resolve
            and self._bound_other_args_to_resolve.get("sync_handle") is True
        ):
            self._deployment_handle: RayServeSyncHandle = deployment_body.get_handle(
                sync=True
            )
        else:
            self._deployment_handle: RayServeHandle = deployment_body.get_handle(
                sync=False
            )

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
