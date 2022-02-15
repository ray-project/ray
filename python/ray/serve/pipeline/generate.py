from typing import Any, Dict, Tuple, Optional, List
import uuid

from ray.experimental.dag import (
    DAGNode,
    InputNode,
    ClassNode,
    ClassMethodNode,
)
from ray.experimental.dag.format_utils import (
    get_args_lines,
    get_kwargs_lines,
    get_options_lines,
    get_other_args_to_resolve_lines,
    get_indentation,
)
from ray.experimental.dag.function_node import FunctionNode
from ray.serve.api import Deployment, DeploymentConfig
from ray.serve.handle import RayServeHandle
import ray
from ray import serve


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

    def __str__(self) -> str:
        indent = get_indentation()

        method_line = str(self._method_name) + "()"
        body_line = str(self._body)

        args_line = get_args_lines(self._bound_args)
        kwargs_line = get_kwargs_lines(self._bound_kwargs)
        options_line = get_options_lines(self._bound_options)
        other_args_to_resolve_line = get_other_args_to_resolve_lines(
            self._bound_other_args_to_resolve
        )
        node_type = f"{self.__class__.__name__}"

        return (
            f"({node_type})(\n"
            f"{indent}method={method_line}\n"
            f"{indent}deployment={body_line}\n"
            f"{indent}args={args_line}\n"
            f"{indent}kwargs={kwargs_line}\n"
            f"{indent}options={options_line}\n"
            f"{indent}other_args_to_resolve={other_args_to_resolve_line}\n"
            f")"
        )


def pipeline_root_wrapper(
    serve_dag_root: DAGNode,
    pipeline_root_name: str
):
    @serve.deployment(name=pipeline_root_name)
    class DAGRunner:
        def __init__(self):
            # TODO: (jiaodong) Make this class take JSON serialized dag
            self.dag = serve_dag_root

        def __call__(self, request):
            return ray.get(self.dag(request))

    return DAGRunner


def generate_deployments_from_ray_dag(
    ray_dag_root: DAGNode,
    pipeline_root_name=None
):
    """
    ** Experimental **
    Given a ray DAG with given root node, generate a list of deployments
    for further iterative development.
    """

    deployments = []

    def convert_to_deployments(dag_node):
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
                init_args=tuple(
                    init_args
                ),  # replace DeploymentNode with handle
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
                dag_node.get_options(),
            )
            return deployment_method_node
        elif isinstance(dag_node, FunctionNode):
            return dag_node
        else:

            return dag_node

    serve_dag_root = ray_dag_root._apply_recursive(
        lambda node: convert_to_deployments(node)
    )

    pipeline_root_name = (
        pipeline_root_name or f"serve_pipeline_root_{uuid.uuid4().hex}"
    )

    serve_dag_root_deployment = pipeline_root_wrapper(
        serve_dag_root, pipeline_root_name
    )
    deployments.insert(0, serve_dag_root_deployment)

    return deployments
