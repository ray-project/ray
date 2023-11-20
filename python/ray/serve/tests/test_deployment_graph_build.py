import pytest
import requests

import ray
from ray import serve
from ray.dag.utils import _DAGNodeNameGenerator
from ray.serve._private import api as _private_api
from ray.serve._private.deployment_graph_build import (
    extract_deployments_from_serve_dag,
    transform_ray_dag_to_serve_dag,
)
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.common.test_modules import NESTED_HANDLE_KEY, Combine, Model


def _validate_consistent_python_output(
    deployment, dag, handle_by_name, input=None, output=None
):
    """Assert same input lead to same outputs across the following:
    1) Deployment handle returned from Deployment instance get_handle()
    2) Original executable Ray DAG
    3) Deployment handle return from serve public API get_deployment()
    """
    deployment_handle = deployment._get_handle()
    assert ray.get(deployment_handle.remote(input)) == output
    handle_by_name = _private_api.get_deployment(handle_by_name)._get_handle()
    assert ray.get(handle_by_name.remote(input)) == output


@serve.deployment
class Add:
    def __call__(self, m1_output, m2_output, kwargs_output=0):
        return m1_output + m2_output + kwargs_output


def test_build_simple_func_dag(serve_instance):
    ray_dag = Add.bind()
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(
                node, node_name_generator, "default"
            )
        )

    # serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0]._deploy()

    deployment_handle = deployments[0]._get_handle()
    # Because the bound kwarg is stored in dag, so it has to be explicitly passed in.
    assert ray.get(deployment_handle.remote(1, 2, kwargs_output=1)) == 4


def test_simple_single_class(serve_instance):
    ray_dag = Model.bind(2, ratio=0.3)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0]._deploy()
    _validate_consistent_python_output(
        deployments[0], ray_dag, "Model", input=1, output=0.6
    )


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    """
    Test we can pass deployments as init_arg or init_kwarg, instantiated
    multiple times for the same class, and we can still correctly replace
    args with deployment handle and parse correct deployment instances.
    """
    m1 = Model.bind(2)
    m2 = Model.bind(3)
    ray_dag = Combine.bind(m1, m2=m2)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    for deployment in deployments:
        deployment._deploy()

    _validate_consistent_python_output(
        deployments[2], ray_dag, "Combine", input=1, output=5
    )


def test_shared_deployment_handle(serve_instance):
    """
    Test we can re-use the same deployment handle multiple times or in
    multiple places, without incorrectly parsing duplicated deployments.
    """
    m = Model.bind(2)
    ray_dag = Combine.bind(m, m2=m)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 2
    for deployment in deployments:
        deployment._deploy()

    _validate_consistent_python_output(
        deployments[1], ray_dag, "Combine", input=1, output=4
    )


def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    """
    Test we can pass deployments with **nested** init_arg or init_kwarg,
    instantiated multiple times for the same class, and we can still correctly
    replace args with deployment handle and parse correct deployment instances.
    """
    m1 = Model.bind(2)
    m2 = Model.bind(3)
    ray_dag = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    # Ensure Deployments with other deployment nodes in init arg are replaced
    # with correct handle
    combine_deployment = deployments[2]
    init_arg_handle = combine_deployment.init_args[0]
    assert isinstance(init_arg_handle, DeploymentHandle)
    assert init_arg_handle.deployment_name == "Model"
    init_kwarg_handle = combine_deployment.init_kwargs["m2"][NESTED_HANDLE_KEY]
    assert isinstance(init_kwarg_handle, DeploymentHandle)
    assert init_kwarg_handle.deployment_name == "Model_1"

    for deployment in deployments:
        deployment._deploy()

    _validate_consistent_python_output(
        deployments[2], ray_dag, "Combine", input=1, output=5
    )


def test_unique_name_reset_upon_build(serve_instance):
    m1 = Model.bind(2)
    m2 = Model.bind(3)
    ray_dag = Combine.bind(m1, m2=m2)
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert deployments[0].name == "Model"
    assert deployments[1].name == "Model_1"

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator, "")
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    # Assert we don't keep increasing suffix id between build() calls
    assert deployments[0].name == "Model"
    assert deployments[1].name == "Model_1"


def test_deployment_function_node_build(serve_instance):
    @serve.deployment
    class Forward:
        def __init__(self, handle: DeploymentHandle):
            self.handle = handle

        async def __call__(self, *args, **kwargs):
            return await self.handle.remote()

    @serve.deployment
    def no_op():
        return "No-op"

    app = Forward.bind(Forward.bind(Forward.bind(no_op.bind())))

    serve.run(app)
    assert requests.get("http://localhost:8000/").text == "No-op"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
