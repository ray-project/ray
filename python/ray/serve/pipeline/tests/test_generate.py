import pytest

import ray
from ray import serve
from ray.experimental.dag import InputNode
from ray.serve.handle import RayServeSyncHandle
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)
from ray.serve.pipeline.tests.test_modules import (
    Model,
    Combine,
    NESTED_HANDLE_KEY,
)


def _validate_consistent_output(
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


def test_simple_single_class(serve_instance):
    # Assert converting both arg and kwarg
    model = Model._bind(2, ratio=0.3)
    ray_dag = model.forward._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(deployments[0], ray_dag, "Model", input=1, output=0.6)


def test_single_class_with_valid_ray_options(serve_instance):
    model = Model.options(num_cpus=1, memory=1000)._bind(2, ratio=0.3)
    ray_dag = model.forward._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], ray_dag, deployments[0].name, input=1, output=0.6
    )

    deployment = serve.get_deployment(deployments[0].name)
    assert deployment.ray_actor_options.get("num_cpus") == 1
    assert deployment.ray_actor_options.get("memory") == 1000
    assert deployment.ray_actor_options.get("runtime_env") == {}


def test_single_class_with_invalid_deployment_options(serve_instance):
    model = Model.options(name="my_deployment")._bind(2, ratio=0.3)
    ray_dag = model.forward._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    with pytest.raises(
        ValueError, match="Specifying name in ray_actor_options is not allowed"
    ):
        deployments[0].deploy()


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    """
    Test we can pass deployments as init_arg or init_kwarg, instantiated
    multiple times for the same class, and we can still correctly replace
    args with deployment handle and parse correct deployment instances.
    """
    m1 = Model._bind(2)
    m2 = Model._bind(3)
    combine = Combine._bind(m1, m2=m2)
    ray_dag = combine.__call__._bind(InputNode())
    print(f"Ray DAG: \n{ray_dag}")

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_output(deployments[2], ray_dag, "Combine", input=1, output=5)


def test_shared_deployment_handle(serve_instance):
    """
    Test we can re-use the same deployment handle multiple times or in
    multiple places, without incorrectly parsing duplicated deployments.
    """
    m = Model._bind(2)
    combine = Combine._bind(m, m2=m)
    ray_dag = combine.__call__._bind(InputNode())
    print(f"Ray DAG: \n{ray_dag}")

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 2
    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_output(deployments[1], ray_dag, "Combine", input=1, output=4)


def test_multi_instantiation_class_nested_deployment_arg(serve_instance):
    """
    Test we can pass deployments with **nested** init_arg or init_kwarg,
    instantiated multiple times for the same class, and we can still correctly
    replace args with deployment handle and parse correct deployment instances.
    """
    m1 = Model._bind(2)
    m2 = Model._bind(3)
    combine = Combine._bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
    ray_dag = combine.__call__._bind(InputNode())
    print(f"Ray DAG: \n{ray_dag}")

    serve_root_dag = ray_dag._apply_recursive(transform_ray_dag_to_serve_dag)
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    # Ensure Deployments with other deployment nodes in init arg are replaced
    # with correct handle
    combine_deployment = deployments[2]
    init_arg_handle = combine_deployment.init_args[0]
    assert isinstance(init_arg_handle, RayServeSyncHandle)
    assert init_arg_handle.deployment_name == "Model"
    init_kwarg_handle = combine_deployment.init_kwargs["m2"][NESTED_HANDLE_KEY]
    assert isinstance(init_kwarg_handle, RayServeSyncHandle)
    assert init_kwarg_handle.deployment_name == "Model_1"

    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_output(deployments[2], ray_dag, "Combine", input=1, output=5)


def test_simple_function(serve_instance):
    # TODO: (jiaodong) Support function deployment node
    pass


def test_multiple_functions(serve_instance):
    # TODO: (jiaodong) Support function deployment node
    pass


def test_mix_class_and_function(serve_instance):
    # TODO: (jiaodong) Support function deployment node
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
