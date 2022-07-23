import pytest
import sys

from ray.serve.deployment_graph_build import get_and_validate_ingress_deployment
from ray.serve.deployment_graph_build import build as pipeline_build
from ray.serve.dag import InputNode
from ray import serve


@serve.deployment
class Model:
    def __init__(self, val):
        self.val = val

    def forward(self, input):
        return self.val + input


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
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[0])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.options(route_prefix="/hello").bind(combine_output)

    deployments = pipeline_build(serve_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/hello"
    for deployment in deployments[:-1]:
        assert deployment.route_prefix is None


def test_http_no_non_ingress_deployment_rout_prefix(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.options(route_prefix="/should-fail").bind(1)
        m2 = Model.bind(1)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[0])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    with pytest.raises(
        ValueError,
        match="Route prefix is only configurable on the ingress deployment",
    ):
        _ = pipeline_build(serve_dag)


def test_http_we_provide_default_route_prefix_cls(serve_instance):
    """Ensure the default ingress deployment route is '/' instead of driver
    class name
    """
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(1)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[0])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/"
    for deployment in deployments[:-1]:
        assert deployment.route_prefix is None


def test_http_we_provide_default_route_prefix_func(serve_instance):
    """Ensure the default ingress deployment route is '/' instead of driver
    function name
    """
    func_dag = combine.bind(1, 2)
    deployments = pipeline_build(func_dag)
    ingress_deployment = get_and_validate_ingress_deployment(deployments)
    assert ingress_deployment.route_prefix == "/"


def test_http_only_one_ingress_deployment(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(1)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[0])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

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


def test_http_non_default_route_prefix_on_non_root_node(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(1)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[0])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag)
    non_root_deployment = deployments[0].options(route_prefix="/yoo")
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
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag)
    non_root_deployment = deployments[-1].options(route_prefix="/yoo")
    deployments[-1] = non_root_deployment
    _ = get_and_validate_ingress_deployment(deployments)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
