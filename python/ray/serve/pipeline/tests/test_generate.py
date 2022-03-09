import pytest
import requests

import ray
from ray import serve
from ray.serve.handle import RayServeSyncHandle
from ray.experimental.dag import InputNode
from ray.serve.pipeline.api import build
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    get_pipeline_input_node,
    get_ingress_deployment,
)
from ray.serve.pipeline.tests.test_modules import (
    Model,
    Combine,
    NESTED_HANDLE_KEY,
    combine,
    request_to_data_int,
)
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode


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


def test_no_inline_class_or_func(serve_instance):
    # This function is defined inline given it lives in the test function
    @ray.remote
    def inline_func(val):
        return val

    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        ray_dag = inline_func._bind(dag_input)

    assert ray.get(ray_dag.execute(1)) == 1
    with pytest.raises(
        AssertionError,
        match="Function used in DAG should not be in-line defined",
    ):
        _ = build(ray_dag)

    # This class is defined inline given it lives in the test function
    @ray.remote
    class InlineClass:
        def __init__(self, val):
            self.val = val

        def get(self, input):
            return self.val + input

    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        node = InlineClass._bind(1)
        ray_dag = node.get._bind(dag_input)

    with pytest.raises(
        AssertionError,
        match="Class used in DAG should not be in-line defined",
    ):
        _ = build(ray_dag)


def test_simple_single_class(serve_instance):
    # Assert converting both arg and kwarg
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        model = Model._bind(2, ratio=0.3)
        ray_dag = model.forward._bind(dag_input)

    serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    ingress_deployment = get_ingress_deployment(serve_root_dag, dag_input)
    assert len(deployments) == 1
    deployments[0].deploy()
    ingress_deployment.deploy()
    _validate_consistent_python_output(
        deployments[0], ray_dag, "Model", input=1, output=0.6
    )

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "0.6"


def test_single_class_with_valid_ray_options(serve_instance):
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        model = Model.options(num_cpus=1, memory=1000)._bind(2, ratio=0.3)
        ray_dag = model.forward._bind(dag_input)

    serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
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
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        model = Model.options(name="my_deployment")._bind(2, ratio=0.3)
        ray_dag = model.forward._bind(dag_input)

    serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    with pytest.raises(
        ValueError, match="Specifying name in ray_actor_options is not allowed"
    ):
        deployments[0].deploy()


@pytest.mark.parametrize("use_build_public_api", [False, True])
def test_multi_instantiation_class_deployment_in_init_args(
    serve_instance, use_build_public_api
):
    """
    Test we can pass deployments as init_arg or init_kwarg, instantiated
    multiple times for the same class, and we can still correctly replace
    args with deployment handle and parse correct deployment instances.
    """
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m1 = Model._bind(2)
        m2 = Model._bind(3)
        combine = Combine._bind(m1, m2=m2)
        ray_dag = combine.__call__._bind(dag_input)
        print(f"Ray DAG: \n{ray_dag}")
    if not use_build_public_api:
        serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
        print(f"Serve DAG: \n{serve_root_dag}")
        deployments = extract_deployments_from_serve_dag(serve_root_dag)
        assert len(deployments) == 3
        for deployment in deployments:
            deployment.deploy()

        ingress_deployment = get_ingress_deployment(serve_root_dag, dag_input)
        ingress_deployment.deploy()

        _validate_consistent_python_output(
            deployments[2], ray_dag, "Combine", input=1, output=5
        )
    else:
        deployments = build(ray_dag)
        for deployment in deployments:
            deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "5"


def test_shared_deployment_handle(serve_instance):
    """
    Test we can re-use the same deployment handle multiple times or in
    multiple places, without incorrectly parsing duplicated deployments.
    """
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m = Model._bind(2)
        combine = Combine._bind(m, m2=m)
        ray_dag = combine.__call__._bind(dag_input)
        print(f"Ray DAG: \n{ray_dag}")

    serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    print(f"Serve DAG: \n{serve_root_dag}")
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 2
    for deployment in deployments:
        deployment.deploy()

    ingress_deployment = get_ingress_deployment(serve_root_dag, dag_input)
    ingress_deployment.deploy()

    _validate_consistent_python_output(
        deployments[1], ray_dag, "Combine", input=1, output=4
    )
    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "4"


@pytest.mark.parametrize("use_build_public_api", [False, True])
def test_multi_instantiation_class_nested_deployment_arg(
    serve_instance, use_build_public_api
):
    """
    Test we can pass deployments with **nested** init_arg or init_kwarg,
    instantiated multiple times for the same class, and we can still correctly
    replace args with deployment handle and parse correct deployment instances.
    """
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m1 = Model._bind(2)
        m2 = Model._bind(3)
        combine = Combine._bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        ray_dag = combine.__call__._bind(dag_input)
        print(f"Ray DAG: \n{ray_dag}")

    if not use_build_public_api:
        serve_root_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
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

        ingress_deployment = get_ingress_deployment(serve_root_dag, dag_input)
        ingress_deployment.deploy()

        _validate_consistent_python_output(
            deployments[2], ray_dag, "Combine", input=1, output=5
        )
    else:
        deployments = build(ray_dag)
        for deployment in deployments:
            deployment.deploy()

    for _ in range(5):
        resp = requests.get("http://127.0.0.1:8000/ingress", data="1")
        assert resp.text == "5"


def test_get_pipeline_input_node():
    # 1) No PipelineInputNode found
    ray_dag = combine._bind(1, 2)
    serve_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    with pytest.raises(
        AssertionError, match="There should be one and only one PipelineInputNode"
    ):
        get_pipeline_input_node(serve_dag)

    # 2) More than one PipelineInputNode found
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        a = combine._bind(dag_input[0], dag_input[1])
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input_2:
        b = combine._bind(dag_input_2[0], dag_input_2[1])
        ray_dag = combine._bind(a, b)
    serve_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    with pytest.raises(
        AssertionError, match="There should be one and only one PipelineInputNode"
    ):
        get_pipeline_input_node(serve_dag)

    # 3) User forgot to change InputNode to PipelineInputNode
    with InputNode() as dag_input:
        ray_dag = combine._bind(dag_input[0], dag_input[1])
    serve_dag = ray_dag.apply_recursive(transform_ray_dag_to_serve_dag)
    with pytest.raises(
        ValueError, match="Please change Ray DAG InputNode to PipelineInputNode"
    ):
        get_pipeline_input_node(serve_dag)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
