from typing import Any, Dict, Tuple, Optional, List

from ray.experimental.dag import (
    DAGNode,
    InputNode,
    ClassNode,
    ClassMethodNode,
)
from ray.experimental.dag.function_node import FunctionNode
from ray.serve.api import Deployment, DeploymentConfig
from ray.serve.handle import RayServeHandle


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        deployment_body,
        deployment_name,
        cls_args,
        cls_kwargs,
        cls_options,
        other_args_to_resolve=None,
    ):
        self._body: Deployment = deployment_body
        self._deployment_name: str = deployment_name
        # TODO: (jiaodong) Support async handle
        self._deployment_handle: RayServeHandle = deployment_body.get_handle()

        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if self._contain_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in DeploymentNode constructor because it is not available at "
                "class construction or binding time."
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
        """Executor of ClassNode by ray.remote()"""
        return self._deployment_handle.options(**self._bound_options).remote(
            *self._bound_args, **self._bound_kwargs
        )


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
        """Executor of ClassMethodNode by ray.remote()"""
        # Execute with bound args.
        return self._deployment_handle.options(**self._bound_options).remote(
            *self._bound_args,
            **self._bound_kwargs,
        )


def generate_deployments_from_ray_dag(ray_dag_root: DAGNode):
    """
    ** Experimental **
    Given a ray DAG with given root node, generate a list of deployments
    for further iterative development.
    """

    deployments = []
    # TODO: (jiaodong) make DAG manipulation methods not private
    # dag_root_copy = ray_dag_root._apply_recursive(
    #     lambda node: node._copy(
    #         node.get_args(),
    #         node.get_kwargs(),
    #         node.get_options(),
    #         node.get_other_args_to_resolve(),
    #     )
    # )


    def convert_to_deployments(dag_node):
        print(f">>>> processing {dag_node}")
        if isinstance(dag_node, ClassNode):
            deployment_name = (
                dag_node.get_options().get("name", None)
                or dag_node._body.__name__
            )
            # Clean up keys with default value
            ray_actor_options = {
                k: v for k, v in dag_node.get_options().items() if v
            }
            if ray_actor_options.get("placement_group") == "default":
                del ray_actor_options["placement_group"]
            if ray_actor_options.get("placement_group_bundle_index") == -1:
                del ray_actor_options["placement_group_bundle_index"]
            if ray_actor_options.get("max_pending_calls") == -1:
                del ray_actor_options["max_pending_calls"]

            args = dag_node.get_args()
            init_args = []
            for arg in args:
                if isinstance(arg, DeploymentNode):
                    init_args.append(arg._deployment_handle)
                else:
                    init_args.append(arg)
            # Deployment class cannot bind with DeploymentNode
            new_deployment = Deployment(
                dag_node._body,
                deployment_name,
                DeploymentConfig(),
                init_args=tuple(init_args), # replace DeploymentNode with handle
                init_kwargs=dag_node.get_kwargs(),
                ray_actor_options=ray_actor_options,
                _internal=True,
            )
            deployment_node = DeploymentNode(
                new_deployment,
                deployment_name,
                dag_node.get_args(),
                dag_node.get_kwargs(),
                ray_actor_options,
                other_args_to_resolve=None,
            )
            deployments.append(new_deployment)

            return deployment_node
        elif isinstance(dag_node, ClassMethodNode):
            other_args_to_resolve = dag_node.get_other_args_to_resolve()
            parent_deployment_node = other_args_to_resolve["parent_class_node"]

            deployment_method_node = DeploymentMethodNode(
                parent_deployment_node._body,
                parent_deployment_node._deployment_name,
                dag_node._method_name,
                dag_node.get_args(),
                dag_node.get_kwargs(),
                dag_node.get_options()
            )
            return deployment_method_node
        elif isinstance(dag_node, FunctionNode):
            return dag_node
        else:

            return dag_node

    serve_dag_root = ray_dag_root._apply_recursive(
        lambda node: convert_to_deployments(node)
    )

    return serve_dag_root, deployments
