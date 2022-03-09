from typing import Any, Dict, List
import threading
import json

from ray import serve
from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    PARENT_CLASS_NODE_KEY,
)
from ray.serve.api import Deployment
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode
from ray.serve.pipeline.json_serde import DAGNodeEncoder
from ray.serve.pipeline.ingress import Ingress
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode

DEFAULT_INGRESS_DEPLOYMENT_NAME = "ingress"


class DeploymentNameGenerator(object):
    """
    Generate unique suffix for each given deployment_name requested for name.
    By default uses deployment_name for the very first time, then append
    monotonic increasing id to it.
    """

    __lock = threading.Lock()
    __shared_state = dict()

    @classmethod
    def get_deployment_name(cls, dag_node: ClassNode):
        assert isinstance(
            dag_node, ClassNode
        ), "get_deployment_name() should only be called on ClassNode instances."
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
        List[Deployment]: List of deployment python objects fetched from serve
            dag.
    """
    deployments = {}

    def extractor(dag_node):
        if isinstance(dag_node, DeploymentNode):
            deployment = dag_node._deployment
            # In case same deployment is used in multiple DAGNodes
            deployments[deployment.name] = deployment
        return dag_node

    serve_dag_root._apply_recursive(extractor)

    return list(deployments.values())


def get_ingress_deployment(
    serve_dag_root_node: DAGNode, pipeline_input_node: PipelineInputNode
) -> Deployment:
    """Return an Ingress deployment to handle user HTTP inputs.

    Args:
        serve_dag_root_node (DAGNode): Transformed  as serve DAG's root. User
            inputs are translated to serve_dag_root_node.execute().
        pipeline_input_node (DAGNode): Singleton PipelineInputNode instance that
            contains input preprocessor info.
    Returns:
        ingress (Deployment): Generated pipeline ingress deployment to serve
            user HTTP requests.
    """
    serve_dag_root_json = json.dumps(serve_dag_root_node, cls=DAGNodeEncoder)
    preprocessor_import_path = pipeline_input_node.get_preprocessor_import_path()
    serve_dag_root_deployment = serve.deployment(Ingress).options(
        name=DEFAULT_INGRESS_DEPLOYMENT_NAME,
        init_args=(
            serve_dag_root_json,
            preprocessor_import_path,
        ),
    )

    return serve_dag_root_deployment
