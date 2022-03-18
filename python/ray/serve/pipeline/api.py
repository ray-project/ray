from typing import List, Optional
from ray.experimental.dag import DAGNode
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    mark_exposed_deployment_in_serve_dag,
)
from ray.serve.api import Deployment


def build(
    ray_dag_root_node: DAGNode, default_route_prefix: Optional[str] = "/"
) -> List[Deployment]:
    """Do all the DAG transformation, extraction and generation needed to
    produce a runnable and deployable serve pipeline application from a valid
    DAG authored with Ray DAG API.

    This should be the only user facing API that user interacts with.

    Assumptions:
        Following enforcements are only applied at generating and applying
        pipeline artifact, but not blockers for local development and testing.

        - ALL args and kwargs used in DAG building should be JSON serializable.
            This means in order to ensure your pipeline application can run on
            a remote cluster potentially with different runtime environment,
            among all options listed:

                1) binding in-memory objects
                2) Rely on pickling
                3) Enforce JSON serialibility on all args used

            We believe both 1) & 2) rely on unstable in-memory objects or
            cross version pickling / closure capture, where JSON serialization
            provides the right contract needed for proper deployment.

        - ALL classes and methods used should be visible on top of the file and
            importable via a fully qualified name. Thus no inline class or
            function definitions should be used.

    Args:
        ray_dag_root_node: DAGNode acting as root of a Ray authored DAG. It
            should be executable via `ray_dag_root_node.execute(user_input)`
            and should have `InputNode` in it.

    Returns:
        deployments: All deployments needed for an e2e runnable serve pipeline,
            accessible via python .remote() call.

    Examples:
        >>> with InputNode() as dag_input:
        ...    m1 = Model.bind(1)
        ...    m2 = Model.bind(2)
        ...    m1_output = m1.forward.bind(dag_input[0])
        ...    m2_output = m2.forward.bind(dag_input[1])
        ...    ray_dag = ensemble.bind(m1_output, m2_output)

        Assuming we have non-JSON serializable or inline defined class or
        function in local pipeline development.

        >>> deployments = serve.build(ray_dag) # it can be method node
        >>> deployments = serve.build(m1) # or just a regular node.
    """
    serve_root_dag = ray_dag_root_node.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    deployments_with_http = mark_exposed_deployment_in_serve_dag(
        deployments, default_route_prefix=default_route_prefix
    )

    return deployments_with_http


def get_and_validate_exposed_deployment(
    deployments: List[Deployment], default_route_prefix="/"
) -> Deployment:
    """Validation for http route prefixes for a list of deployments in pipeline.

    Ensures:
        1) One and only one exposed deployment with given route prefix.
        2) All other not exposed deployments should have prefix of None.
        3) Exposed deployment should only have given route prefix, not others.
    """

    exposed_deployments = []
    for deployment in deployments:
        if deployment.route_prefix == default_route_prefix:
            exposed_deployments.append(deployment)
        # Condition 2) and 3)
        elif deployment.route_prefix is not None:
            raise ValueError(
                "Exposed deployment should not have route prefix other than "
                f"{default_route_prefix}, found: {deployment.route_prefix}"
            )

    # Condition 1)
    if len(exposed_deployments) != 1:
        raise ValueError(
            "Only one deployment in an Serve Application or DAG can have "
            f"non-None route prefix. {len(exposed_deployments)} exposed "
            f"deployments with prefix {default_route_prefix} found."
        )

    return exposed_deployments[0]
