import os
import pytest
import sys
import tempfile

import ray
from ray import serve
from ray.dag import DAGNode
from ray.dag.utils import _DAGNodeNameGenerator
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.deployment_graph import InputNode
from ray.serve.drivers import DAGDriver
import starlette.requests

from ray.serve._private.deployment_graph_build import transform_ray_dag_to_serve_dag

NESTED_HANDLE_KEY = "nested_handle"


@serve.deployment
class ClassHello:
    def __init__(self):
        pass

    def hello(self):
        return "hello"


@serve.deployment
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        return self.ratio * self.weight * input

    def __call__(self, request):
        input_data = request
        return self.ratio * self.weight * input_data


@serve.deployment
class Combine:
    def __init__(
        self,
        m1,
        m2=None,
        m2_nested: bool = False,
    ):
        self.m1 = m1
        self.m2 = m2.get(NESTED_HANDLE_KEY) if m2_nested else m2

    def __call__(self, req):
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        return sum(ray.get([r1_ref, r2_ref]))


@serve.deployment
class Counter:
    def __init__(self, val):
        self.val = val

    def get(self):
        return self.val

    def inc(self, inc):
        self.val += inc


@serve.deployment
def fn_hello():
    return "hello"


@serve.deployment
def combine(m1_output, m2_output, kwargs_output=0):
    return m1_output + m2_output + kwargs_output


def class_factory():
    class MyInlineClass:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    return MyInlineClass


@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def forward(self, inp: int) -> int:
        print(f"Adder got {inp}")
        return inp + self.increment

    __call__ = forward


@serve.deployment
class NoargDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await (await self.dag.remote())


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


def ray_dag_to_serve_dag(dag: DAGNode):
    with _DAGNodeNameGenerator() as deployment_name_generator:
        serve_dag = dag.apply_recursive(
            lambda node: transform_ray_dag_to_serve_dag(node, deployment_name_generator)
        )
    return serve_dag


def test_serve_pipeline_single_func_no_input_plot():
    dag = fn_hello.bind()
    serve_dag = NoargDriver.bind(dag)
    serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "fn_hello -> NoargDriver" in to_string


def test_serve_pipeline_single_func_deployment_dag_plot():
    with InputNode() as dag_input:
        dag = combine.bind(dag_input[0], dag_input[1], kwargs_output=1)
        serve_dag = DAGDriver.bind(dag, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "INPUT_NODE -> INPUT_ATTRIBUTE_NODE" in to_string
    assert "INPUT_NODE -> INPUT_ATTRIBUTE_NODE_1" in to_string
    assert "INPUT_ATTRIBUTE_NODE -> combine" in to_string
    assert "INPUT_ATTRIBUTE_NODE_1 -> combine" in to_string
    assert "combine -> DAGDriver" in to_string


def test_serve_pipeline_chained_function_plot():
    @serve.deployment
    def func_1(input):
        return input

    @serve.deployment
    def func_2(input):
        return input * 2

    with InputNode() as dag_input:
        output_1 = func_1.bind(dag_input)
        output_2 = func_2.bind(dag_input)
        serve_dag = combine.bind(output_1, output_2)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "INPUT_NODE -> func_1" in to_string
    assert "INPUT_NODE -> func_2" in to_string
    assert "func_1 -> combine" in to_string
    assert "func_2 -> combine" in to_string


def test_serve_pipeline_class_with_class_method_plot():
    with InputNode() as dag_input:
        model = Model.bind(2, ratio=0.3)
        dag = model.forward.bind(dag_input)
        serve_dag = DAGDriver.bind(dag, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "Model -> forward" in to_string
    assert "INPUT_NODE -> forward" in to_string
    assert "forward -> DAGDriver" in to_string


def test_serve_pipeline_func_class_with_class_method_plot():
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output, kwargs_output=dag_input[2])
        serve_dag = DAGDriver.bind(combine_output, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "INPUT_NODE -> INPUT_ATTRIBUTE_NODE" in to_string
    assert "INPUT_NODE -> INPUT_ATTRIBUTE_NODE_1" in to_string
    assert "INPUT_NODE -> INPUT_ATTRIBUTE_NODE_2" in to_string
    assert "Model -> forward" in to_string
    assert "INPUT_ATTRIBUTE_NODE -> forward" in to_string
    assert "Model_1 -> forward_1" in to_string
    assert "INPUT_ATTRIBUTE_NODE_1 -> forward_1" in to_string
    assert "INPUT_ATTRIBUTE_NODE_2 -> combine" in to_string
    assert "forward -> combine" in to_string
    assert "forward_1 -> combine" in to_string
    assert "combine -> DAGDriver" in to_string


def test_serve_pipeline_multi_instantiation_class_deployment_in_init_args_plot():
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2=m2)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(combine_output, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "Model -> Combine" in to_string
    assert "Model_1 -> Combine" in to_string
    assert "Combine -> __call__" in to_string
    assert "INPUT_NODE -> __call__" in to_string
    assert "__call__ -> DAGDriver" in to_string


def test_serve_pipeline_test_shared_deployment_handle_plot():
    with InputNode() as dag_input:
        m = Model.bind(2)
        combine = Combine.bind(m, m2=m)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(combine_output, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "Model -> Combine" in to_string
    assert "Combine -> __call__" in to_string
    assert "INPUT_NODE -> __call__" in to_string
    assert "__call__ -> DAGDriver" in to_string


def test_serve_pipeline_multi_instantiation_class_nested_deployment_arg_dag_plot():
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(output, input_schema=json_resolver)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "Model -> Combine" in to_string
    assert "Model_1 -> Combine" in to_string
    assert "Combine -> __call__" in to_string
    assert "INPUT_NODE -> __call__" in to_string
    assert "__call__ -> DAGDriver" in to_string


def test_serve_pipeline_class_factory_plot():
    with InputNode() as _:
        instance = serve.deployment(class_factory()).bind(3)
        output = instance.get.bind()
        serve_dag = NoargDriver.bind(output)
        serve_dag = ray_dag_to_serve_dag(serve_dag)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.dag.plot(serve_dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.dag.vis_utils._dag_to_dot(serve_dag)
    to_string = graph.to_string()
    assert "MyInlineClass -> get" in to_string
    assert "get -> NoargDriver" in to_string


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
