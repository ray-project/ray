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
