import sys

import pytest

from ray import serve
from ray.serve._private.deployment_graph_build import build as pipeline_build
from ray.serve._private.deployment_graph_build import (
    get_and_validate_ingress_deployment,
)


@serve.deployment
class Model:
    def __init__(self, val):
        self.val = val

    def forward(self, input):
        return self.val + input


@serve.deployment
def func_deployment():
    return "hello"


@serve.deployment
def combine(input_1, input_2):
    return input_1 + input_2


@serve.deployment
class Driver:
    def __init__(self, dag):
        self.dag = dag

    async def __call__(self, inp):
        return await self.dag.remote(inp)


def test_http_user_bring_own_driver_route_prefix(serve_instance):
    m1 = Model.bind(1)
    serve_dag = Driver.options(route_prefix="/hello").bind(m1)

    deployments = pipeline_build(serve_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/hello"
    for deployment in deployments[:-1]:
        assert deployment.route_prefix is None


def test_http_no_non_ingress_deployment_route_prefix(serve_instance):
    m1 = Model.options(route_prefix="/should-fail").bind(1)
    serve_dag = Driver.bind(m1)

    with pytest.raises(
        ValueError,
        match="Route prefix is only configurable on the ingress deployment",
    ):
        _ = pipeline_build(serve_dag)


def test_http_we_provide_default_route_prefix_cls(serve_instance):
    """Ensure the default ingress deployment route is '/' instead of driver
    class name
    """

    m1 = Model.bind(1)
    serve_dag = Driver.bind(m1)

    deployments = pipeline_build(serve_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/"
    for deployment in deployments[:-1]:
        assert deployment.route_prefix is None


def test_http_we_provide_default_route_prefix_func(serve_instance):
    """Ensure the default ingress deployment route is '/' instead of driver
    function name
    """
    func_dag = func_deployment.bind()
    deployments = pipeline_build(func_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/"


def test_http_non_default_route_prefix_on_non_root_node(serve_instance):
    m1 = Model.bind(1)
    serve_dag = Driver.bind(m1)

    deployments = pipeline_build(serve_dag)
    non_root_deployment = deployments[0].options(route_prefix="/")
    deployments[0] = non_root_deployment

    with pytest.raises(
        ValueError,
        match=(
            "Only one deployment in an Serve Application or DAG can have "
            "non-None route prefix"
        ),
    ):
        _ = get_and_validate_ingress_deployment(deployments)


def test_http_reconfigure_non_default_route_prefix_on_root(serve_instance):
    m1 = Model.bind(1)
    serve_dag = Driver.bind(m1)

    deployments = pipeline_build(serve_dag)
    non_root_deployment = deployments[-1].options(route_prefix="/yoo")
    deployments[-1] = non_root_deployment
    _ = get_and_validate_ingress_deployment(deployments)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
