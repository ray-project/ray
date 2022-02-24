from typing import Any, Dict, List, Tuple
import threading

from ray.experimental.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    PARENT_CLASS_NODE_KEY,
)

from ray.serve.api import Deployment, DeploymentConfig
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
from ray.serve.pipeline.deployment_node import DeploymentNode


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
        assert isinstance(dag_node, ClassNode), (
            "get_deployment_name() should only be called on ClassNode " "instances."
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


def _remove_non_default_ray_actor_options(ray_actor_options: Dict[str, Any]):
    """
    In Ray DAG building we pass full ray_actor_options regardless if a field
    was explicitly set. Since some values are invalid, we need to remove them
    from ray_actor_options.
    """
    ray_actor_options = {k: v for k, v in ray_actor_options.items() if v}
    if ray_actor_options.get("placement_group") == "default":
        del ray_actor_options["placement_group"]
    if ray_actor_options.get("placement_group_bundle_index") == -1:
        del ray_actor_options["placement_group_bundle_index"]
    if ray_actor_options.get("max_pending_calls") == -1:
        del ray_actor_options["max_pending_calls"]

    return ray_actor_options


def _replace_init_args_with_deployment_handle(
    args: List[Any], kwargs: Dict[str, Any]
) -> Tuple[Tuple[Any], Dict[str, Any]]:
    """
    Deployment can be passed into other DAGNodes as init args. This is supported
    pattern in ray DAG. Thus we need convert them into deployment handles in
    ray serve DAG to make end to end DAG executable.
    """
    init_args = []
    init_kwargs = {}

    # TODO: (jiaodong) Need to handle deeply nested deployment in args
    for arg in args:
        if isinstance(arg, DeploymentNode):
            init_args.append(arg._deployment_handle)
        else:
            init_args.append(arg)
    for key, value in kwargs.items():
        if isinstance(value, DeploymentNode):
            init_kwargs[key] = value._deployment_handle
        else:
            init_kwargs[key] = value
    return tuple(init_args), init_kwargs


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
        init_args, init_kwargs = _replace_init_args_with_deployment_handle(
            dag_node.get_args(), dag_node.get_kwargs()
        )
        # Deployment class cannot bind with DeploymentNode
        new_deployment = Deployment(
            dag_node._body,
            deployment_name,
            DeploymentConfig(),
            init_args=init_args,
            init_kwargs=init_kwargs,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )
        deployment_node = DeploymentNode(
            new_deployment,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            # TODO: (jiaodong) Support .options(metadata=xxx) for deployment
            {},  # Deployment options are not ray actor options.
            other_args_to_resolve=dag_node.get_other_args_to_resolve(),
        )
        return deployment_node

    elif isinstance(dag_node, ClassMethodNode):
        other_args_to_resolve = dag_node.get_other_args_to_resolve()
        # TODO: (jiaodong) Need to capture DAGNodes in the parent node
        parent_deployment_node = other_args_to_resolve[PARENT_CLASS_NODE_KEY]

        deployment_method_node = DeploymentMethodNode(
            parent_deployment_node._body,
            dag_node._method_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
            other_args_to_resolve=dag_node.get_other_args_to_resolve(),
        )
        return deployment_method_node
    else:
        # TODO: (jiaodong) Support FunctionNode
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
            deployment = dag_node._body
            # In case same deployment is used in multiple DAGNodes
            deployments[deployment.name] = deployment
        # elif DeploymentMethodNode

    serve_dag_root._apply_recursive(extractor)

    return list(deployments.values())
