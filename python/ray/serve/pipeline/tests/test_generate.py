import pytest
from typing import TypeVar

import ray
from ray import serve
import ray.experimental.dag as ray_dag
from ray.serve.pipeline.generate import generate_deployments_from_ray_dag

RayHandleLike = TypeVar("RayHandleLike")


@ray.remote
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        return self.ratio * self.weight * input

    def __call__(self, request):
        return self.forward(request)


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
    dag = model.forward._bind(ray_dag.DAG_ENTRY_POINT)

    deployments = generate_deployments_from_ray_dag(dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], dag, "Model", input=1, output=0.6
    )


def test_single_class_with_ray_options(serve_instance):
    model = Model.options(
        num_cpus=1, memory=1000, object_store_memory=1000, max_concurrency=50
    )._bind(2, ratio=0.3)
    dag = model.forward._bind(ray_dag.DAG_ENTRY_POINT)

    deployments = generate_deployments_from_ray_dag(dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], dag, "Model", input=1, output=0.6
    )

    deployment = serve.get_deployment("Model")
    assert deployment.ray_actor_options == {
        "num_cpus": 1,
        "memory": 1000,
        "object_store_memory": 1000,
        "max_concurrency": 50
    }


def test_single_class_with_deployment_options(serve_instance):
    """Test user provided name in .options() overrides class name as
    deployment name
    """
    model = Model.options(name="my_deployment")._bind(2, ratio=0.3)
    dag = model.forward._bind(ray_dag.DAG_ENTRY_POINT)

    deployments = generate_deployments_from_ray_dag(dag)
    assert len(deployments) == 1
    deployments[0].deploy()
    _validate_consistent_output(
        deployments[0], dag, "my_deployment", input=1, output=0.6
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
    pass


#     @ray.remote
#     class Model:
#         def __init__(self, weight: int):
#             self.weight = weight

#         def forward(self, input: int):
#             return self.weight * input

#     @ray.remote
#     def combine(m1: "RayHandleLike", m2: "RayHandleLike"):
#         return m1 + m2

#     m1 = Model._bind(2)
#     m2 = Model._bind(3)

#     m1_output = m1.forward._bind(ray_dag.DAG_ENTRY_POINT)
#     m2_output = m2.forward._bind(ray_dag.DAG_ENTRY_POINT)

#     dag = combine._bind(m1_output, m2_output)
#     print(dag)


def test_multiple_class_method_entrypoints(serve_instance):
    pass


def test_simple_function(serve_instance):
    pass


def test_multiple_functions(serve_instance):
    pass


def test_mix_class_and_function(serve_instance):
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
