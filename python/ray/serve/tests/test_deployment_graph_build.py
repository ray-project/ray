import pytest

import ray
from ray import serve
from ray.dag import InputNode
from ray.serve.handle import RayServeLazySyncHandle
from ray.serve.deployment_graph_build import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    transform_serve_dag_to_serve_executor_dag,
    get_pipeline_input_node,
)
from ray.serve.tests.resources.test_modules import (
    Model,
    NESTED_HANDLE_KEY,
    combine,
)
from ray.serve.tests.resources.test_dags import (
    get_simple_class_with_class_method_dag,
    get_func_class_with_class_method_dag,
    get_multi_instantiation_class_deployment_in_init_args_dag,
    get_shared_deployment_handle_dag,
    get_multi_instantiation_class_nested_deployment_arg_dag,
    get_simple_func_dag,
)
from ray.dag.utils import _DAGNodeNameGenerator


def _validate_consistent_python_output(
    deployment, dag, handle_by_name, input=None, output=None
):
    """Assert same input lead to same outputs across the following:
    1) Deployment handle returned from Deployment instance get_handle()
    2) Original executable Ray DAG
    3) Deployment handle return from serve public API get_deployment()
    """
    deployment_handle = deployment.get_handle()
    assert ray.get(deployment_handle.remote(input)) == output
    assert ray.get(dag.execute(input)) == output
    handle_by_name = serve.get_deployment(handle_by_name).get_handle()
    assert ray.get(handle_by_name.remote(input)) == output


@pytest.mark.skip(
    "No supporting converting ray.remote task directly to Serve deployments right now."
)
def test_build_simple_func_dag(serve_instance):
    ray_dag, _ = get_simple_func_dag()
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )

    serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()

    deployment_handle = deployments[0].get_handle()
    # Because the bound kwarg is stored in dag, so it has to be explicitly passed in.
    assert ray.get(deployment_handle.remote(1, 2, kwargs_output=1)) == 4
    assert ray.get(ray_dag.execute([1, 2])) == 4


def test_simple_single_class(serve_instance):
    ray_dag, _ = get_simple_class_with_class_method_dag()

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_python_output(
        deployments[0], ray_dag, "Model", input=1, output=0.6
    )


def test_single_class_with_valid_ray_options(serve_instance):
    with InputNode() as dag_input:
        model = Model.options(num_cpus=1, memory=1000).bind(2, ratio=0.3)
        ray_dag = model.forward.bind(dag_input)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_python_output(
        deployments[0], ray_dag, deployments[0].name, input=1, output=0.6
    )

    deployment = serve.get_deployment(deployments[0].name)
    assert deployment.ray_actor_options.get("num_cpus") == 1
    assert deployment.ray_actor_options.get("memory") == 1000
    assert deployment.ray_actor_options.get("runtime_env") == {}


def test_single_class_with_invalid_deployment_options(serve_instance):
    with InputNode() as dag_input:
        model = Model.options(name="my_deployment").bind(2, ratio=0.3)
        ray_dag = model.forward.bind(dag_input)

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    with pytest.raises(
        ValueError, match="Specifying 'name' in ray_actor_options is not allowed"
    ):
        deployments[0].deploy()


def test_func_class_with_class_method_dag(serve_instance):
    ray_dag, _ = get_func_class_with_class_method_dag()

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    serve_executor_root_dag = serve_root_dag.apply_recursive(
        transform_serve_dag_to_serve_executor_dag
    )
    assert len(deployments) == 2
    for deployment in deployments:
        deployment.deploy()

    assert ray.get(ray_dag.execute(1, 2, 3)) == 8
    assert ray.get(serve_executor_root_dag.execute(1, 2, 3)) == 8


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    """
    Test we can pass deployments as init_arg or init_kwarg, instantiated
    multiple times for the same class, and we can still correctly replace
    args with deployment handle and parse correct deployment instances.
    """
    ray_dag, _ = get_multi_instantiation_class_deployment_in_init_args_dag()

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_python_output(
        deployments[2], ray_dag, "Combine", input=1, output=5
    )


def test_shared_deployment_handle(serve_instance):
    """
    Test we can re-use the same deployment handle multiple times or in
    multiple places, without incorrectly parsing duplicated deployments.
    """
    ray_dag, _ = get_shared_deployment_handle_dag()

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 2
    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_python_output(
        deployments[1], ray_dag, "Combine", input=1, output=4
    )


def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    """
    Test we can pass deployments with **nested** init_arg or init_kwarg,
    instantiated multiple times for the same class, and we can still correctly
    replace args with deployment handle and parse correct deployment instances.
    """
    ray_dag, _ = get_multi_instantiation_class_nested_deployment_arg_dag()

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    # Ensure Deployments with other deployment nodes in init arg are replaced
    # with correct handle
    combine_deployment = deployments[2]
    init_arg_handle = combine_deployment.init_args[0]
    assert isinstance(init_arg_handle, RayServeLazySyncHandle)
    assert init_arg_handle.deployment_name == "Model"
    init_kwarg_handle = combine_deployment.init_kwargs["m2"][NESTED_HANDLE_KEY]
    assert isinstance(init_kwarg_handle, RayServeLazySyncHandle)
    assert init_kwarg_handle.deployment_name == "Model_1"

    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_python_output(
        deployments[2], ray_dag, "Combine", input=1, output=5
    )


def test_get_pipeline_input_node():
    # 1) No InputNode found
    ray_dag = combine.bind(1, 2)
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    with pytest.raises(
        AssertionError, match="There should be one and only one InputNode"
    ):
        get_pipeline_input_node(serve_dag)

    # 2) More than one InputNode found
    with InputNode() as dag_input:
        a = combine.bind(dag_input[0], dag_input[1])
    with InputNode() as dag_input_2:
        b = combine.bind(dag_input_2[0], dag_input_2[1])
        ray_dag = combine.bind(a, b)
    with pytest.raises(
        AssertionError, match="Each DAG should only have one unique InputNode"
    ):
        with _DAGNodeNameGenerator() as node_name_generator:
            serve_dag = ray_dag.apply_recursive(
                lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
            )
        get_pipeline_input_node(serve_dag)


def test_unique_name_reset_upon_build(serve_instance):
    ray_dag, _ = get_multi_instantiation_class_deployment_in_init_args_dag()
    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert deployments[0].name == "Model"
    assert deployments[1].name == "Model_1"

    with _DAGNodeNameGenerator() as node_name_generator:
        serve_root_dag = ray_dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, node_name_generator)
        )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    # Assert we don't keep increasing suffix id between build() calls
    assert deployments[0].name == "Model"
    assert deployments[1].name == "Model_1"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
