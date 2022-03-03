from typing import Any, Callable, Dict, Optional, List, Tuple, Union

from ray.experimental.dag import DAGNode, InputNode
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.constants import USE_SYNC_HANDLE_KEY
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.serve.api import Deployment, DeploymentConfig


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        # For serve structured deployment, deployment body can be import path
        # to the class or function instead.
        func_or_class: Union[Callable, str],
        deployment_name: str,
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
        if self._contains_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in the DeploymentNode constructor because it is not available "
                "at class construction or binding time."
            )
        # Deployment can be passed into other DAGNodes as init args. This is
        # supported pattern in ray DAG that user can instantiate and pass class
        # instances as init args to others.

        # However in ray serve we send init args via .remote() that requires
        # pickling, and all DAGNode types are not picklable by design.

        # Thus we need convert all DeploymentNode used in init args into
        # deployment handles (executable and picklable) in ray serve DAG to make
        # serve DAG end to end executable.
        (
            replaced_deployment_init_args,
            replaced_deployment_init_kwargs,
        ) = self._apply_functional(
            [deployment_init_args, deployment_init_kwargs],
            predictate_fn=lambda node: isinstance(
                node, (DeploymentNode, DeploymentMethodNode)
            ),
            apply_fn=lambda node: node._get_serve_deployment_handle(
                node._deployment, node._bound_other_args_to_resolve
            ),
        )
        self._deployment: Deployment = Deployment(
            func_or_class,
            deployment_name,
            # TODO: (jiaodong) Support deployment config from user input
            DeploymentConfig(),
            init_args=replaced_deployment_init_args,
            init_kwargs=replaced_deployment_init_kwargs,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )
        self._deployment_handle: Union[
            RayServeHandle, RayServeSyncHandle
        ] = self._get_serve_deployment_handle(self._deployment, other_args_to_resolve)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentNode(
            self._deployment.func_or_class,
            self._deployment.name,
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
        getattr(self._deployment.func_or_class, method_name)
        call_node = DeploymentMethodNode(
            self._deployment,
            method_name,
            (),
            {},
            {},
            other_args_to_resolve=self._bound_other_args_to_resolve,
        )
        return call_node

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment))

    def get_deployment_name(self):
        return self._deployment.name

    def get_import_path(self):
        if isinstance(self._deployment._func_or_class, str):
            # We're processing a deserilized JSON node where import_path
            # is dag_node body.
            return self._deployment._func_or_class
        else:
            body = self._deployment._func_or_class.__ray_actor_class__
            return f"{body.__module__}.{body.__qualname__}"

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, DeploymentNode.__name__)
        json_dict["deployment_name"] = self.get_deployment_name()
        json_dict["import_path"] = self.get_import_path()

        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        return cls(
            input_json["import_path"],
            input_json["deployment_name"],
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
