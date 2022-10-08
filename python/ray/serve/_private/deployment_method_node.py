from typing import Any, Dict, Optional, Tuple, List

from ray.dag import DAGNode
from ray.dag.format_utils import get_dag_node_str
from ray.dag.constants import PARENT_CLASS_NODE_KEY
from ray.serve.deployment import Deployment


class DeploymentMethodNode(DAGNode):
    """Represents a deployment method invocation of a DeploymentNode in DAG."""

    def __init__(
        self,
        deployment: Deployment,
        deployment_method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        self._deployment = deployment
        self._deployment_method_name: str = deployment_method_name
        self._deployment_handle = other_args_to_resolve[PARENT_CLASS_NODE_KEY]
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentMethodNode(
            self._deployment,
            self._deployment_method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def __str__(self) -> str:
        return get_dag_node_str(
            self,
            str(self._deployment_method_name) + "() @ " + str(self._deployment),
        )

    def get_deployment_name(self) -> str:
        return self._deployment.name

    def get_deployment_method_name(self) -> str:
        return self._deployment_method_name
