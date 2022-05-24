from typing import Any, Dict, Optional, Tuple, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY, PARENT_CLASS_NODE_KEY
from ray.serve.deployment import Deployment
from ray.serve.config import DeploymentConfig


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

    def _execute_impl(self, *args, **kwargs):
        """Executor of DeploymentMethodNode by ray.remote()"""
        # Execute with bound args.
        method_body = getattr(self._deployment_handle, self._deployment_method_name)
        return method_body.remote(
            *self._bound_args,
            **self._bound_kwargs,
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

    def get_import_path(self) -> str:
        if (
            "is_from_serve_deployment"
            in self._bound_other_args_to_resolve[
                "parent_class_node"
            ]._bound_other_args_to_resolve
        ):  # built by serve top level api, this is ignored for serve.run
            return "dummy"
        elif isinstance(self._deployment._func_or_class, str):
            # We're processing a deserilized JSON node where import_path
            # is dag_node body.
            return self._deployment._func_or_class
        else:
            body = self._deployment._func_or_class.__ray_actor_class__
            return f"{body.__module__}.{body.__qualname__}"

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: DeploymentMethodNode.__name__,
            "deployment_name": self.get_deployment_name(),
            "deployment_method_name": self.get_deployment_method_name(),
            # Will be overriden by build()
            "import_path": self.get_import_path(),
            "args": self.get_args(),
            "kwargs": self.get_kwargs(),
            # .options() should not contain any DAGNode type
            "options": self.get_options(),
            "other_args_to_resolve": self.get_other_args_to_resolve(),
            "uuid": self.get_stable_uuid(),
        }

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentMethodNode.__name__
        return cls(
            Deployment(
                input_json["import_path"],
                input_json["deployment_name"],
                # TODO: (jiaodong) Support deployment config from user input
                DeploymentConfig(),
                init_args=input_json["args"],
                init_kwargs=input_json["kwargs"],
                ray_actor_options=input_json["options"],
                _internal=True,
            ),
            input_json["deployment_method_name"],
            input_json["args"],
            input_json["kwargs"],
            input_json["options"],
            other_args_to_resolve=input_json["other_args_to_resolve"],
        )
