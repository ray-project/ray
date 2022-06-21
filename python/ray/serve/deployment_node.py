import inspect
from typing import Any, Callable, Dict, Optional, List, Tuple, Union

from ray.dag import DAGNode
from ray.serve.deployment_executor_node import DeploymentExecutorNode
from ray.serve.deployment_function_executor_node import (
    DeploymentFunctionExecutorNode,
)
from ray.serve.deployment_method_executor_node import (
    DeploymentMethodExecutorNode,
)
from ray.serve.handle import RayServeLazySyncHandle

from ray.dag.constants import PARENT_CLASS_NODE_KEY
from ray.dag.format_utils import get_dag_node_str
from ray.serve.deployment_method_node import DeploymentMethodNode
from ray.serve.deployment_function_node import DeploymentFunctionNode
from ray.serve.deployment import Deployment, schema_to_deployment
from ray.serve.config import DeploymentConfig
from ray.serve.schema import DeploymentSchema


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
        # Deployment can be passed into other DAGNodes as init args. This is
        # supported pattern in ray DAG that user can instantiate and pass class
        # instances as init args to others.

        # However in ray serve we send init args via .remote() that requires
        # pickling, and all DAGNode types are not picklable by design.

        # Thus we need convert all DeploymentNode used in init args into
        # deployment handles (executable and picklable) in ray serve DAG to make
        # serve DAG end to end executable.
        # TODO(jiaodong): This part does some magic for DAGDriver and will throw
        # error with weird pickle replace table error. Move this out.
        def replace_with_handle(node):
            if isinstance(node, DeploymentNode):
                return RayServeLazySyncHandle(node._deployment.name)
            elif isinstance(node, DeploymentExecutorNode):
                return node._deployment_handle

        (
            replaced_deployment_init_args,
            replaced_deployment_init_kwargs,
        ) = self.apply_functional(
            [deployment_init_args, deployment_init_kwargs],
            predictate_fn=lambda node: isinstance(
                node,
                # We need to match and replace all DAGNodes even though they
                # could be None, because no DAGNode replacement should run into
                # re-resolved child DAGNodes, otherwise with KeyError
                (
                    DeploymentNode,
                    DeploymentMethodNode,
                    DeploymentFunctionNode,
                    DeploymentExecutorNode,
                    DeploymentFunctionExecutorNode,
                    DeploymentMethodExecutorNode,
                ),
            ),
            apply_fn=replace_with_handle,
        )

        if "deployment_schema" in self._bound_other_args_to_resolve:
            deployment_schema: DeploymentSchema = self._bound_other_args_to_resolve[
                "deployment_schema"
            ]
            deployment_shell = schema_to_deployment(deployment_schema)

            # Prefer user specified name to override the generated one.
            if (
                inspect.isclass(func_or_class)
                and deployment_shell.name != func_or_class.__name__
            ):
                deployment_name = deployment_shell.name

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
                func_or_class=func_or_class,
                name=deployment_name,
                init_args=replaced_deployment_init_args,
                init_kwargs=replaced_deployment_init_kwargs,
                route_prefix=route_prefix,
            )
        else:
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
        self._deployment_handle = RayServeLazySyncHandle(self._deployment.name)

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
