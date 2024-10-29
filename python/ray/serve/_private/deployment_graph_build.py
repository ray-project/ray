import inspect
from collections import OrderedDict
from typing import List

from ray.dag import ClassNode, DAGNode
from ray.dag.function_node import FunctionNode
from ray.dag.utils import _DAGNodeNameGenerator
from ray.experimental.gradio_utils import type_to_string
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.deployment_function_node import DeploymentFunctionNode
from ray.serve._private.deployment_node import DeploymentNode
from ray.serve.deployment import Deployment, schema_to_deployment
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import DeploymentSchema


def build(
    ray_dag_root_node: DAGNode, name: str = SERVE_DEFAULT_APP_NAME
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
        name: Application name,. If provided, formatting all the deployment name to
            {name}_{deployment_name}, if not provided, the deployment name won't be
            updated.

    Returns:
        deployments: All deployments needed for an e2e runnable serve pipeline,
            accessible via python .remote() call.

    Examples:

        .. code-block:: python

            with InputNode() as dag_input:
                m1 = Model.bind(1)
                m2 = Model.bind(2)
                m1_output = m1.forward.bind(dag_input[0])
                m2_output = m2.forward.bind(dag_input[1])
                ray_dag = ensemble.bind(m1_output, m2_output)

        Assuming we have non-JSON serializable or inline defined class or
        function in local pipeline development.

        .. code-block:: python

            from ray.serve.api import build as build_app
            deployments = build_app(ray_dag) # it can be method node
            deployments = build_app(m1) # or just a regular node.
    """
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag_root_node.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, name)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)

    # If the ingress deployment is a function and it is bound to other deployments,
    # reject.
    if isinstance(serve_root_dag, DeploymentFunctionNode) and len(deployments) != 1:
        raise ValueError(
            "The ingress deployment to your application cannot be a function if there "
            "are multiple deployments. If you want to compose them, use a class. If "
            "you're using the DAG API, the function should be bound to a DAGDriver."
        )

    # The last deployment in the list is the ingress.
    return deployments


def transform_ray_dag_to_serve_dag(
    dag_node: DAGNode, node_name_generator: _DAGNodeNameGenerator, app_name: str
):
    """
    Transform a Ray DAG to a Serve DAG. Map ClassNode to DeploymentNode with
    ray decorated body passed in.
    """
    if isinstance(dag_node, ClassNode):
        deployment_name = node_name_generator.get_node_name(dag_node)
        (
            replaced_deployment_init_args,
            replaced_deployment_init_kwargs,
        ) = dag_node.apply_functional(
            [dag_node.get_args(), dag_node.get_kwargs()],
            predictate_fn=lambda node: isinstance(
                node,
                # We need to match and replace all DAGNodes even though they
                # could be None, because no DAGNode replacement should run into
                # re-resolved child DAGNodes, otherwise with KeyError
                (
                    DeploymentNode,
                    DeploymentFunctionNode,
                ),
            ),
            apply_fn=lambda node: DeploymentHandle(node._deployment.name, app_name),
        )

        # ClassNode is created via bind on serve.deployment decorated class
        # with no serve specific configs.
        deployment_schema: DeploymentSchema = dag_node._bound_other_args_to_resolve[
            "deployment_schema"
        ]

        deployment_shell: Deployment = schema_to_deployment(deployment_schema)

        # Prefer user specified name to override the generated one.
        if (
            inspect.isclass(dag_node._body)
            and deployment_shell.name != dag_node._body.__name__
        ):
            deployment_name = deployment_shell.name

        deployment = deployment_shell.options(
            func_or_class=dag_node._body,
            name=deployment_name,
            _init_args=replaced_deployment_init_args,
            _init_kwargs=replaced_deployment_init_kwargs,
            _internal=True,
        )

        return DeploymentNode(
            deployment,
            app_name,
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

        other_args_to_resolve = dag_node.get_other_args_to_resolve()
        if "return" in dag_node._body.__annotations__:
            other_args_to_resolve["result_type_string"] = type_to_string(
                dag_node._body.__annotations__["return"]
            )

        # Set the deployment name if the user provides.
        if "deployment_schema" in dag_node._bound_other_args_to_resolve:
            schema = dag_node._bound_other_args_to_resolve["deployment_schema"]
            if (
                inspect.isfunction(dag_node._body)
                and schema.name != dag_node._body.__name__
            ):
                deployment_name = schema.name

        return DeploymentFunctionNode(
            dag_node._body,
            deployment_name,
            app_name,
            dag_node.get_args(),
            dag_node.get_kwargs(),
            dag_node.get_options(),
            other_args_to_resolve=other_args_to_resolve,
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
