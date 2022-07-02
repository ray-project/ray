import json
from typing import List
from collections import OrderedDict

from ray.serve.deployment import Deployment
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.deployment_method_node import DeploymentMethodNode
from ray.serve.deployment_node import DeploymentNode
from ray.serve.deployment_function_node import DeploymentFunctionNode
from ray.serve.deployment_executor_node import DeploymentExecutorNode
from ray.serve.deployment_method_executor_node import DeploymentMethodExecutorNode
from ray.serve.deployment_function_executor_node import DeploymentFunctionExecutorNode
from ray.serve.json_serde import DAGNodeEncoder


from ray.dag import (
    DAGNode,
    ClassNode,
    ClassMethodNode,
    PARENT_CLASS_NODE_KEY,
)
from ray.dag.function_node import FunctionNode
from ray.dag.input_node import InputNode
from ray.dag.utils import _DAGNodeNameGenerator


def build(ray_dag_root_node: DAGNode) -> List[Deployment]:
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

        >>> from ray.serve.api import build as build_app
        >>> deployments = build_app(ray_dag) # it can be method node
        >>> deployments = build_app(m1) # or just a regular node.
    """
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag_root_node.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)

    # After Ray DAG is transformed to Serve DAG with deployments and their init
    # args filled, generate a minimal weight executor serve dag for perf
    serve_executor_root_dag = serve_root_dag.apply_recursive(
        transform_serve_dag_to_serve_executor_dag
    )
    root_driver_deployment = deployments[-1]
    new_driver_deployment = generate_executor_dag_driver_deployment(
        serve_executor_root_dag, root_driver_deployment
    )
    # Replace DAGDriver deployment with executor DAGDriver deployment
    deployments[-1] = new_driver_deployment
    # Validate and only expose HTTP for the endpoint
    deployments_with_http = process_ingress_deployment_in_serve_dag(deployments)

    return deployments_with_http


def get_and_validate_ingress_deployment(
    deployments: List[Deployment],
) -> Deployment:
    """Validation for http route prefixes for a list of deployments in pipeline.

    Ensures:
        1) One and only one ingress deployment with given route prefix.
        2) All other not ingress deployments should have prefix of None.
    """

    ingress_deployments = []
    for deployment in deployments:
        if deployment.route_prefix is not None:
            ingress_deployments.append(deployment)

    if len(ingress_deployments) != 1:
        raise ValueError(
            "Only one deployment in an Serve Application or DAG can have "
            f"non-None route prefix. {len(ingress_deployments)} ingress "
            f"deployments found: {ingress_deployments}"
        )

    return ingress_deployments[0]


def transform_ray_dag_to_serve_dag(
    dag_node: DAGNode, node_name_generator: _DAGNodeNameGenerator
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
        serve_dag_root: Transformed serve dag root node.
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


def transform_serve_dag_to_serve_executor_dag(serve_dag_root_node: DAGNode):
    """Given a runnable serve dag with deployment init args and options
    processed, transform into an equivalent, but minimal dag optimized for
    execution.
    """
    if isinstance(serve_dag_root_node, DeploymentNode):
        return DeploymentExecutorNode(
            serve_dag_root_node._deployment_handle,
            serve_dag_root_node.get_args(),
            serve_dag_root_node.get_kwargs(),
        )
    elif isinstance(serve_dag_root_node, DeploymentMethodNode):
        return DeploymentMethodExecutorNode(
            # Deployment method handle
            serve_dag_root_node._deployment_method_name,
            serve_dag_root_node.get_args(),
            serve_dag_root_node.get_kwargs(),
            other_args_to_resolve=serve_dag_root_node.get_other_args_to_resolve(),
        )
    elif isinstance(serve_dag_root_node, DeploymentFunctionNode):
        return DeploymentFunctionExecutorNode(
            serve_dag_root_node._deployment_handle,
            serve_dag_root_node.get_args(),
            serve_dag_root_node.get_kwargs(),
        )
    else:
        return serve_dag_root_node


def generate_executor_dag_driver_deployment(
    serve_executor_dag_root_node: DAGNode, original_driver_deployment: Deployment
):
    """Given a transformed minimal execution serve dag, and original DAGDriver
    deployment, generate new DAGDriver deployment that uses new serve executor
    dag as init_args.

    Args:
        serve_executor_dag_root_node: Transformed
            executor serve dag with only barebone deployment handles.
        original_driver_deployment: User's original DAGDriver
            deployment that wrapped Ray DAG as init args.
    Returns:
        executor_dag_driver_deployment: New DAGDriver deployment
            with executor serve dag as init args.
    """

    def replace_with_handle(node):
        if isinstance(node, DeploymentExecutorNode):
            return node._deployment_handle
        elif isinstance(
            node,
            (
                DeploymentMethodExecutorNode,
                DeploymentFunctionExecutorNode,
            ),
        ):
            serve_dag_root_json = json.dumps(node, cls=DAGNodeEncoder)
            return RayServeDAGHandle(serve_dag_root_json)

    (
        replaced_deployment_init_args,
        replaced_deployment_init_kwargs,
    ) = serve_executor_dag_root_node.apply_functional(
        [
            serve_executor_dag_root_node.get_args(),
            serve_executor_dag_root_node.get_kwargs(),
        ],
        predictate_fn=lambda node: isinstance(
            node,
            (
                DeploymentExecutorNode,
                DeploymentFunctionExecutorNode,
                DeploymentMethodExecutorNode,
            ),
        ),
        apply_fn=replace_with_handle,
    )

    return original_driver_deployment.options(
        init_args=replaced_deployment_init_args,
        init_kwargs=replaced_deployment_init_kwargs,
    )


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
