import pytest

from ray.serve.pipeline.api import get_and_validate_exposed_deployment
from ray.serve.pipeline.api import build as pipeline_build
from ray.experimental.dag.input_node import InputNode
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


def test_http_only_one_exposed_deployment(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag, default_route_prefix="/hii")
    non_root_deployment = deployments[0].options(route_prefix="/hii")
    deployments[0] = non_root_deployment

    with pytest.raises(
        ValueError,
        match=(
            "Only one deployment in an Serve Application or DAG can have "
            "non-None route prefix"
        ),
    ):
        _ = get_and_validate_exposed_deployment(
            deployments, default_route_prefix="/hii"
        )


def test_http_non_default_route_prefix_on_non_root_node(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag, default_route_prefix="/hii")
    non_root_deployment = deployments[0].options(route_prefix="/yoo")
    deployments[0] = non_root_deployment


    with pytest.raises(
        ValueError,
        match=("Exposed deployment should not have route prefix other than")
    ):
        _ = get_and_validate_exposed_deployment(
            deployments, default_route_prefix="/hii"
        )


def test_http_non_default_rout_prefix_on_root(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output)
        serve_dag = Driver.bind(combine_output)

    deployments = pipeline_build(serve_dag, default_route_prefix="/hii")
    non_root_deployment = deployments[-1].options(route_prefix="/yoo")
    deployments[-1] = non_root_deployment

    with pytest.raises(
        ValueError,
        match=("Exposed deployment should not have route prefix other than")
    ):
        _ = get_and_validate_exposed_deployment(
            deployments, default_route_prefix="/hii"
        )
