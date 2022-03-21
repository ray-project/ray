from typing import Any, Dict, List, Union
import threading
from collections import OrderedDict

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    PARENT_CLASS_NODE_KEY,
)
from ray.experimental.dag.function_node import FunctionNode
from ray.experimental.dag.input_node import InputNode
from ray.serve.api import Deployment
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.deployment_function_node import DeploymentFunctionNode


class DeploymentNameGenerator(object):
    """
    Generate unique suffix for each given deployment_name requested for name.
    By default uses deployment_name for the very first time, then append
    monotonic increasing id to it.
    """

    __lock = threading.Lock()
    __shared_state = dict()

    @classmethod
    def get_deployment_name(cls, dag_node: Union[ClassNode, FunctionNode]):
        assert isinstance(dag_node, (ClassNode, FunctionNode)), (
            "get_deployment_name() should only be called on ClassNode or "
            "FunctionNode instances."
        )
        with cls.__lock:
            deployment_name = (
                dag_node.get_options().get("name", None) or dag_node._body.__name__
            )
            if deployment_name not in cls.__shared_state:
                cls.__shared_state[deployment_name] = 0
                return deployment_name
            else:
                cls.__shared_state[deployment_name] += 1
                suffix_num = cls.__shared_state[deployment_name]

                return f"{deployment_name}_{suffix_num}"

    @classmethod
    def reset(cls):
        with cls.__lock:
            cls.__shared_state = dict()


def _remove_non_default_ray_actor_options(ray_actor_options: Dict[str, Any]):
    """
    In Ray DAG building we pass full ray_actor_options regardless if a field
    was explicitly set. Since some values are invalid, we need to remove them
    from ray_actor_options.
    """
    # TODO: (jiaodong) Revisit when we implement build() when user explicitly
    # pass default value
    ray_actor_options = {k: v for k, v in ray_actor_options.items() if v}
    if ray_actor_options.get("placement_group") == "default":
        del ray_actor_options["placement_group"]
    if ray_actor_options.get("placement_group_bundle_index") == -1:
        del ray_actor_options["placement_group_bundle_index"]
    if ray_actor_options.get("max_pending_calls") == -1:
        del ray_actor_options["max_pending_calls"]

    return ray_actor_options


def transform_ray_dag_to_serve_dag(dag_node):
    """
    Transform a Ray DAG to a Serve DAG. Map ClassNode to DeploymentNode with
    ray decorated body passed in, ans ClassMethodNode to DeploymentMethodNode.
    """
    if isinstance(dag_node, ClassNode):
        deployment_name = DeploymentNameGenerator.get_deployment_name(dag_node)
        ray_actor_options = _remove_non_default_ray_actor_options(
            dag_node.get_options()
        )
        return DeploymentNode(
            dag_node._body,
            deployment_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            ray_actor_options,
            # TODO: (jiaodong) Support .options(metadata=xxx) for deployment
            other_args_to_resolve=dag_node.get_other_args_to_resolve(),
        )

    elif isinstance(dag_node, ClassMethodNode):
        other_args_to_resolve = dag_node.get_other_args_to_resolve()
        # TODO: (jiaodong) Need to capture DAGNodes in the parent node
        parent_deployment_node = other_args_to_resolve[PARENT_CLASS_NODE_KEY]

        return DeploymentMethodNode(
            parent_deployment_node._deployment,
            dag_node._method_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
            other_args_to_resolve=dag_node.get_other_args_to_resolve(),
        )
    elif isinstance(
        dag_node,
        FunctionNode
        # TODO (jiaodong): We do not convert ray function to deployment function
        # yet, revisit this later
    ) and dag_node.get_other_args_to_resolve().get("is_from_serve_deployment"):
        deployment_name = DeploymentNameGenerator.get_deployment_name(dag_node)
        return DeploymentFunctionNode(
            dag_node._body,
            deployment_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
            other_args_to_resolve=dag_node.get_other_args_to_resolve(),
        )
    else:
        # TODO: (jiaodong) Support FunctionNode or leave it as ray task
        return dag_node


def extract_deployments_from_serve_dag(
    serve_dag_root: DAGNode,
) -> List[Deployment]:
    """Extract deployment python objects from a transformed serve DAG. Should
    only be called after `transform_ray_dag_to_serve_dag`, otherwise nothing
    to return.

    Args:
        serve_dag_root (DAGNode): Transformed serve dag root node.
    Returns:
        deployments (List[Deployment]): List of deployment python objects
            fetched from serve dag.
    """
    deployments = OrderedDict()

    def extractor(dag_node):
        if isinstance(dag_node, (DeploymentNode, DeploymentFunctionNode)):
            deployment = dag_node._deployment
            # In case same deployment is used in multiple DAGNodes
            deployments[deployment.name] = deployment
        return dag_node

    serve_dag_root.apply_recursive(extractor)

    return list(deployments.values())


def get_pipeline_input_node(serve_dag_root_node: DAGNode):
    """Return the InputNode singleton node from serve dag, and throw
    exceptions if we didn't find any, or found more than one.

    Args:
        ray_dag_root_node: DAGNode acting as root of a Ray authored DAG. It
            should be executable via `ray_dag_root_node.execute(user_input)`
            and should have `InputNode` in it.
    Returns
        pipeline_input_node: Singleton input node for the serve pipeline.
    """

    input_nodes = []

    def extractor(dag_node):
        if isinstance(dag_node, InputNode):
            input_nodes.append(dag_node)

    serve_dag_root_node.apply_recursive(extractor)
    assert len(input_nodes) == 1, (
        "There should be one and only one InputNode in the DAG. "
        f"Found {len(input_nodes)} InputNode(s) instead."
    )

    return input_nodes[0]


def process_ingress_deployment_in_serve_dag(
    deployments: List[Deployment],
) -> List[Deployment]:
    """Mark the last fetched deployment in a serve dag as exposed with default
    prefix.
    """
    if len(deployments) == 0:
        return deployments

    # Last element of the list is the root deployment if it's applicable type
    # that wraps an deployment, given Ray DAG traversal is done bottom-up.
    ingress_deployment = deployments[-1]
    if ingress_deployment.route_prefix in [None, f"/{ingress_deployment.name}"]:
        # Override default prefix to "/" on the ingress deployment, if user
        # didn't provide anything in particular.
        new_ingress_deployment = ingress_deployment.options(route_prefix="/")
        deployments[-1] = new_ingress_deployment

    # Erase all non ingress deployment route prefix
    for i, deployment in enumerate(deployments[:-1]):
        if (
            deployment.route_prefix is not None
            and deployment.route_prefix != f"/{deployment.name}"
        ):
            raise ValueError(
                "Route prefix is only configurable on the ingress deployment. "
                "Please do not set non-default route prefix: "
                f"{deployment.route_prefix} on non-ingress deployment of the "
                "serve DAG. "
            )
        else:
            # Earse all default prefix to None for non-ingress deployments to
            # disable HTTP
            deployments[i] = deployment.options(route_prefix=None)

    return deployments
