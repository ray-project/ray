from typing import Any, Dict, Optional, List, Tuple

from ray.dag import DAGNode
from ray.serve.handle import RayServeDeploymentHandle

from ray.dag.constants import PARENT_CLASS_NODE_KEY
from ray.dag.format_utils import get_dag_node_str
from ray.serve._private.deployment_method_node import DeploymentMethodNode
from ray.serve.deployment import Deployment


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        # For serve structured deployment, deployment body can be import path
        # to the class or function instead.
        deployment: Deployment,
        deployment_init_args: Tuple[Any],
        deployment_init_kwargs: Dict[str, Any],
        ray_actor_options: Dict[str, Any],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        # Assign instance variables in base class constructor.
        super().__init__(
            deployment_init_args,
            deployment_init_kwargs,
            ray_actor_options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._deployment = deployment
        self._deployment_handle = RayServeDeploymentHandle(self._deployment.name)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentNode(
            self._deployment,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._deployment.func_or_class, method_name)
        call_node = DeploymentMethodNode(
            self._deployment,
            method_name,
            (),
            {},
            {},
            other_args_to_resolve={
                **self._bound_other_args_to_resolve,
                PARENT_CLASS_NODE_KEY: self,
            },
        )
        return call_node

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment))

    def get_deployment_name(self):
        return self._deployment.name
