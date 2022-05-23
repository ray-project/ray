from typing import List
from collections import OrderedDict

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    PARENT_CLASS_NODE_KEY,
)
from ray.experimental.dag.function_node import FunctionNode
from ray.experimental.dag.input_node import InputNode
from ray.experimental.dag.utils import DAGNodeNameGenerator
from ray.serve.deployment import Deployment
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.deployment_function_node import DeploymentFunctionNode


def transform_ray_dag_to_serve_dag(
    dag_node: DAGNode, node_name_generator: DAGNodeNameGenerator
):
    """
    Transform a Ray DAG to a Serve DAG. Map ClassNode to DeploymentNode with
    ray decorated body passed in, and ClassMethodNode to DeploymentMethodNode.
    """
    if isinstance(dag_node, ClassNode):
        deployment_name = node_name_generator.get_node_name(dag_node)
        return DeploymentNode(
            dag_node._body,
            deployment_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
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
        deployment_name = node_name_generator.get_node_name(dag_node)
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
