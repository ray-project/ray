import sys
import pytest

from ray.serve.api import _get_deployments_from_node
from ray.serve.handle import PipelineHandle
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode
from ray.experimental.dag import InputNode

import ray
from ray import serve
from typing import TypeVar

RayHandleLike = TypeVar("RayHandleLike")
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
        m1: "RayHandleLike",
        m2: "RayHandleLike" = None,
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
class Driver:
    def __init__(self, dag: PipelineHandle):
        self.dag = dag

    async def __call__(self, inp: int) -> int:
        print(f"Driver got {inp}")
        return await self.dag.remote(inp)

@serve.deployment
class NoargDriver:
    def __init__(self, dag: PipelineHandle):
        self.dag = dag

    async def __call__(self):
        return await self.dag.remote()


def test_single_func_deployment_dag(serve_instance):
    with InputNode() as dag_input:
        combine_func = combine.bind()
        serve_dag = combine_func.__call__.bind(
            dag_input[0], dag_input[1], kwargs_output=1
        )
    print(serve_dag)

    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote([1, 2])) == 4


def test_simple_class_with_class_method(serve_instance):
    with InputNode() as dag_input:
        model = Model.bind(2, ratio=0.3)
        serve_dag = model.forward.bind(dag_input)

    print(serve_dag)
    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote(1)) == 0.6


def test_func_class_with_class_method(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        serve_dag = combine.bind().__call__.bind(
            m1_output, m2_output, kwargs_output=dag_input[2]
        )

    print(serve_dag)
    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote([1, 2, 3])) == 8


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2=m2)
        serve_dag = combine.__call__.bind(dag_input)
    print(serve_dag)
    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote(1)) == 5


def test_shared_deployment_handle(serve_instance):
    with InputNode() as dag_input:
        m = Model.bind(2)
        combine = Combine.bind(m, m2=m)
        serve_dag = combine.__call__.bind(dag_input)
    print(serve_dag)
    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote(1)) == 4


def test_multi_instantiation_class_nested_deployment_arg_dag(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        serve_dag = combine.__call__.bind(dag_input)
    print(serve_dag)
    handle = serve.run(Driver.bind(serve_dag))
    assert ray.get(handle.remote(1)) == 5

def test_class_factory(serve_instance):
    with InputNode() as _:
        instance = ray.remote(class_factory()).bind(3)
        serve_dag = instance.get.bind()
    print(serve_dag)
    handle = serve.run(NoargDriver.bind(serve_dag))
    assert ray.get(handle.remote()) == 3


def test_single_node_deploy_success(serve_instance):
    m1 = Adder.bind(1)
    handle = serve.run(m1)
    assert ray.get(handle.remote(41)) == 42


def test_single_node_driver_sucess(serve_instance):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with PipelineInputNode() as input_node:
        out = m1.forward.bind(input_node)
        out = m2.forward.bind(out)
    driver = Driver.bind(out)
    handle = serve.run(driver)
    assert ray.get(handle.remote(39)) == 42


def test_options_and_names(serve_instance):

    m1 = Adder.bind(1)
    m1_built = _get_deployments_from_node(m1)[-1]
    assert m1_built.name == "Adder"

    m1 = Adder.options(name="Adder2").bind(1)
    m1_built = _get_deployments_from_node(m1)[-1]
    assert m1_built.name == "Adder2"

    m1 = Adder.options(num_replicas=2).bind(1)
    m1_built = _get_deployments_from_node(m1)[-1]
    assert m1_built.num_replicas == 2


@ray.remote
def combine_task(*args):
    return sum(args)


@pytest.mark.skip("TODO")
def test_mixing_task(serve_instance):
    # Can't mix ray.remote tasks with deployment in a dag right now.
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with PipelineInputNode() as input_node:
        out = combine_task.bind(
            m1.forward.bind(input_node), m2.forward.bind(input_node)
        )
    driver = Driver.bind(out)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 5


@serve.deployment
class TakeHandle:
    def __init__(self, handle) -> None:
        self.handle = handle

    def __call__(self, inp):
        return ray.get(self.handle.remote(inp))


def test_passing_handle(serve_instance):
    child = Adder.bind(1)
    parent = TakeHandle.bind(child)
    driver = Driver.bind(parent)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
