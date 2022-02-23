import pytest
from typing import TypeVar
import requests

import ray
from ray import serve
from ray.experimental.dag import InputNode
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
)
from ray.serve.pipeline.tests.test_modules import Model

RayHandleLike = TypeVar("RayHandleLike")


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

    serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], ray_dag, "Model", input=1, output=0.6
    )


def test_single_class_with_ray_options(serve_instance):
    model = Model.options(num_cpus=1, memory=1000, max_concurrency=50)._bind(
        2, ratio=0.3
    )
    ray_dag = model.forward._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], ray_dag, deployments[0].name, input=1, output=0.6
    )

    deployment = serve.get_deployment(deployments[0].name)
    assert deployment.ray_actor_options == {
        "num_cpus": 1,
        "memory": 1000,
        "max_concurrency": 50,
        "runtime_env": {},
    }


def test_single_class_with_deployment_options(serve_instance):
    """Test user provided name in .options() overrides class name as
    deployment name
    """
    # TODO: (jiaodong) Support this
    model = Model.options(name="my_deployment")._bind(2, ratio=0.3)
    ray_dag = model.forward._bind(InputNode())

    serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], ray_dag, "my_deployment", input=1, output=0.6
    )


def test_multiple_instantiation_class(serve_instance):
    """
    Test a multiple class methods can all be used as entrypoints in a dag.
    """
    pass


def test_no_duplicated_deployment_name():
    """Test to ensure we don't allow user providing to deployments with
    same name in .options()
    """
    pass


def test_multi_classes(serve_instance):
    """
    Test a multiple class methods can all be used as entrypoints in a dag.
    """

    @ray.remote
    class Model1:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: int):
            return self.weight * input

    @ray.remote
    class Model2:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: int):
            return self.weight + input

    @ray.remote
    class Combine:
        def __init__(self, m1: "RayHandleLike", m2: "RayHandleLike"):
            self.m1 = m1
            self.m2 = m2

        def __call__(self, req):
            r1_ref = self.m1.forward.remote(req)
            r2_ref = self.m2.forward.remote(req)
            return sum(ray.get([r1_ref, r2_ref]))

    m1 = Model1._bind(2)
    m2 = Model2._bind(3)
    combine = Combine._bind(m1, m2)
    ray_dag = combine.__call__._bind(InputNode())
    print(ray_dag)

    serve_root_dag = ray_dag._apply_recursive(
        lambda node: transform_ray_dag_to_serve_dag(node)
    )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    assert len(deployments) == 3
    for deployment in deployments:
        deployment.deploy()

    _validate_consistent_output(
        deployments[2], ray_dag, "Combine", input=1, output=6
    )


# def test_simple_function(serve_instance):
#     pass


# def test_multiple_functions(serve_instance):
#     pass


# def test_mix_class_and_function(serve_instance):
#     pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
