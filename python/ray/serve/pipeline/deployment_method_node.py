from typing import Any, Dict, Optional, Tuple, List, Union

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.serve.handle import RayServeLazySyncHandle, RayServeSyncHandle, RayServeHandle
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.serve.api import Deployment, DeploymentConfig


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
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._deployment_handle: Union[
            RayServeLazySyncHandle, RayServeHandle, RayServeSyncHandle
        ] = self._get_serve_deployment_handle(deployment, other_args_to_resolve)

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
        # TODO (jiaodong): Support configurable async handle
        if USE_SYNC_HANDLE_KEY not in bound_other_args_to_resolve:
            # Return sync RayServeLazySyncHandle
            return RayServeLazySyncHandle(deployment.name)
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

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, DeploymentMethodNode.__name__)
        json_dict["deployment_name"] = self.get_deployment_name()
        json_dict["deployment_method_name"] = self.get_deployment_method_name()
        json_dict["import_path"] = self.get_import_path()

        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentMethodNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        return cls(
            Deployment(
                input_json["import_path"],
                input_json["deployment_name"],
                # TODO: (jiaodong) Support deployment config from user input
                DeploymentConfig(),
                init_args=args_dict["args"],
                init_kwargs=args_dict["kwargs"],
                ray_actor_options=args_dict["options"],
                _internal=True,
            ),
            input_json["deployment_method_name"],
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
