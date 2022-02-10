from ray.experimental.dag import (
    DAGNode,
    ClassNode,
)
from ray.serve.api import Deployment, DeploymentConfig


def generate_deployments_from_ray_dag(ray_dag_root: DAGNode):
    """
    ** Experimental **
    Given a ray DAG with given root node, generate a list of deployments
    for further iterative development.
    """

    deployments = []

    def convert_to_deployments(dag_node):
        if isinstance(dag_node, ClassNode):
            deployment_name = (
                dag_node.get_options().get("name", None) or dag_node._body.__name__
            )
            # Clean up keys with default value
            ray_actor_options = {k: v for k, v in dag_node.get_options().items() if v}
            if ray_actor_options.get("placement_group") == "default":
                del ray_actor_options["placement_group"]
            if ray_actor_options.get("placement_group_bundle_index") == -1:
                del ray_actor_options["placement_group_bundle_index"]
            if ray_actor_options.get("max_pending_calls") == -1:
                del ray_actor_options["max_pending_calls"]

            # Replace bound Deployment args in a ClassNode with handle
            args = dag_node.get_args()
            init_args = []
            for arg in args:
                if isinstance(arg, Deployment):
                    init_args.append(arg.get_handle())
                else:
                    init_args.append(arg)

            new_deployment = Deployment(
                dag_node._body,
                deployment_name,
                DeploymentConfig(),
                init_args=tuple(init_args),
                init_kwargs=dag_node.get_kwargs(),
                ray_actor_options=ray_actor_options,
                _internal=True,
            )
            deployments.append(new_deployment)
            return new_deployment

    ray_dag_root._apply_recursive(lambda node: convert_to_deployments(node))

    return deployments
