from typing import Any, Dict
import uuid

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
)
from ray.serve.api import Deployment, DeploymentConfig
from ray import serve
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode


def to_json(dag_node: DAGNode):
    pass

def from_json(json: Dict[str, Any]) -> DAGNode:
    pass


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
        else:
            return dag_node

    serve_dag_root = ray_dag_root._apply_recursive(
        lambda node: convert_to_deployments(node)
    )

    pipeline_root_name = (
        pipeline_root_name or f"serve_pipeline_root_{uuid.uuid4().hex}"
    )

    serve_dag_root_deployment = serve.deployment(
        name="pipeline_root_name",
    )("ray.serve.pipeline.generate.DAGRunner")
    serve_dag_root_deployment._init_args = (serve_dag_root,)

    deployments.insert(0, serve_dag_root_deployment)

    return deployments
