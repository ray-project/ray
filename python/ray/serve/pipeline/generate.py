from ray.experimental.dag import (
    DAGNode,
    FunctionNode,
    ClassMethodNode,
    ClassNode,
    DAG_ENTRY_POINT,
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
                dag_node.get_options().get("name", None)
                or dag_node._body.__name__
            )
            deployments.append(
                Deployment(
                    dag_node._body,
                    deployment_name,
                    DeploymentConfig(),
                    init_args=dag_node.get_args(),
                    init_kwargs=dag_node.get_kwargs(),
                    ray_actor_options=dag_node.get_options(),
                    _internal=True,
                )
            )

    ray_dag_root._apply_recursive(lambda node: convert_to_deployments(node))

    print(ray_dag_root)
    print(deployments)
    return deployments
