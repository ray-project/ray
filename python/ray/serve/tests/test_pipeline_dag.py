import pytest
import os
import sys
from typing import TypeVar, Union

import numpy as np
import requests

import ray
from ray import serve
from ray.serve.application import Application
from ray.serve.api import build as build_app
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.deployment_graph_build import build as pipeline_build
from ray.serve.deployment_graph import ClassNode, InputNode
from ray.serve.drivers import DAGDriver
import starlette.requests


RayHandleLike = TypeVar("RayHandleLike")
NESTED_HANDLE_KEY = "nested_handle"


def maybe_build(node: ClassNode, use_build: bool) -> Union[Application, ClassNode]:
    if use_build:
        return build_app(node)
    else:
        return node


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
class NoargDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await self.dag.remote()


# TODO(Shreyas): Enable use_build once serve.build() PR is out.
@pytest.mark.parametrize("use_build", [False])
def test_single_func_no_input(serve_instance, use_build):
    dag = fn_hello.bind()
    serve_dag = NoargDriver.bind(dag)

    handle = serve.run(maybe_build(serve_dag, use_build))
    assert ray.get(handle.remote()) == "hello"
    assert requests.get("http://127.0.0.1:8000/").text == "hello"


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


@pytest.mark.parametrize("use_build", [False, True])
def test_single_func_deployment_dag(serve_instance, use_build):
    with InputNode() as dag_input:
        dag = combine.bind(dag_input[0], dag_input[1], kwargs_output=1)
        serve_dag = DAGDriver.bind(dag, http_adapter=json_resolver)
    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote([1, 2])) == 4
    assert requests.post("http://127.0.0.1:8000/", json=[1, 2]).json() == 4


@pytest.mark.parametrize("use_build", [False, True])
def test_chained_function(serve_instance, use_build):
    @serve.deployment
    def func_1(input):
        return input

    @serve.deployment
    def func_2(input):
        return input * 2

    @serve.deployment
    def func_3(input):
        return input * 3

    with InputNode() as dag_input:
        output_1 = func_1.bind(dag_input)
        output_2 = func_2.bind(dag_input)
        output_3 = func_3.bind(output_2)
        ray_dag = combine.bind(output_1, output_2, kwargs_output=output_3)
    with pytest.raises(ValueError, match="Please provide a driver class"):
        _ = serve.run(ray_dag)

    serve_dag = DAGDriver.bind(ray_dag, http_adapter=json_resolver)

    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote(2)) == 18  # 2 + 2*2 + (2*2) * 3
    assert requests.post("http://127.0.0.1:8000/", json=2).json() == 18


@pytest.mark.parametrize("use_build", [False, True])
def test_simple_class_with_class_method(serve_instance, use_build):
    with InputNode() as dag_input:
        model = Model.bind(2, ratio=0.3)
        dag = model.forward.bind(dag_input)
        serve_dag = DAGDriver.bind(dag, http_adapter=json_resolver)
    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote(1)) == 0.6
    assert requests.post("http://127.0.0.1:8000/", json=1).json() == 0.6


@pytest.mark.parametrize("use_build", [False, True])
def test_func_class_with_class_method(serve_instance, use_build):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output, kwargs_output=dag_input[2])
        serve_dag = DAGDriver.bind(combine_output, http_adapter=json_resolver)

    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote([1, 2, 3])) == 8
    assert requests.post("http://127.0.0.1:8000/", json=[1, 2, 3]).json() == 8


@pytest.mark.parametrize("use_build", [False, True])
def test_multi_instantiation_class_deployment_in_init_args(serve_instance, use_build):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2=m2)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(combine_output, http_adapter=json_resolver)

    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote(1)) == 5
    assert requests.post("http://127.0.0.1:8000/", json=1).json() == 5


@pytest.mark.parametrize("use_build", [False, True])
def test_shared_deployment_handle(serve_instance, use_build):
    with InputNode() as dag_input:
        m = Model.bind(2)
        combine = Combine.bind(m, m2=m)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(combine_output, http_adapter=json_resolver)

    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote(1)) == 4
    assert requests.post("http://127.0.0.1:8000/", json=1).json() == 4


@pytest.mark.parametrize("use_build", [False, True])
def test_multi_instantiation_class_nested_deployment_arg_dag(serve_instance, use_build):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        output = combine.__call__.bind(dag_input)
        serve_dag = DAGDriver.bind(output, http_adapter=json_resolver)

    handle = serve.run(serve_dag)
    assert ray.get(handle.predict.remote(1)) == 5
    assert requests.post("http://127.0.0.1:8000/", json=1).json() == 5


def test_class_factory(serve_instance):
    with InputNode() as _:
        instance = serve.deployment(class_factory()).bind(3)
        output = instance.get.bind()
        serve_dag = NoargDriver.bind(output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote()) == 3
    assert requests.get("http://127.0.0.1:8000/").text == "3"


@serve.deployment
class Echo:
    def __init__(self, s: str):
        self._s = s

    def __call__(self, *args):
        return self._s


# TODO(Shreyas): Enable use_build once serve.build() PR is out.
@pytest.mark.parametrize("use_build", [False])
def test_single_node_deploy_success(serve_instance, use_build):
    m1 = Adder.bind(1)
    handle = serve.run(maybe_build(m1, use_build))
    assert ray.get(handle.remote(41)) == 42


@pytest.mark.parametrize("use_build", [False, True])
def test_single_node_driver_sucess(serve_instance, use_build):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with InputNode() as input_node:
        out = m1.forward.bind(input_node)
        out = m2.forward.bind(out)
    driver = DAGDriver.bind(out, http_adapter=json_resolver)
    handle = serve.run(driver)
    assert ray.get(handle.predict.remote(39)) == 42
    assert requests.post("http://127.0.0.1:8000/", json=39).json() == 42


def test_options_and_names(serve_instance):

    m1 = Adder.bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.name == "Adder"

    m1 = Adder.options(name="Adder2").bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.name == "Adder2"

    m1 = Adder.options(num_replicas=2).bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.num_replicas == 2


@serve.deployment
class TakeHandle:
    def __init__(self, handle) -> None:
        self.handle = handle

    def __call__(self, inp):
        return ray.get(self.handle.remote(inp))


@pytest.mark.parametrize("use_build", [False, True])
def test_passing_handle(serve_instance, use_build):
    child = Adder.bind(1)
    parent = TakeHandle.bind(child)
    driver = DAGDriver.bind(parent, http_adapter=json_resolver)
    handle = serve.run(driver)
    assert ray.get(handle.predict.remote(1)) == 2
    assert requests.post("http://127.0.0.1:8000/", json=1).json() == 2


@serve.deployment
class DictParent:
    def __init__(self, d):
        self._d = d

    async def __call__(self, key):
        return await self._d[key].remote()


# TODO(Shreyas): Enable use_build once serve.build() PR is out.
@pytest.mark.parametrize("use_build", [False])
def test_passing_handle_in_obj(serve_instance, use_build):
    child1 = Echo.bind("ed")
    child2 = Echo.bind("simon")
    parent = maybe_build(
        DictParent.bind({"child1": child1, "child2": child2}), use_build
    )

    handle = serve.run(parent)
    assert ray.get(handle.remote("child1")) == "ed"
    assert ray.get(handle.remote("child2")) == "simon"


@serve.deployment
class Child:
    def __call__(self, *args):
        return os.getpid()


@serve.deployment
class Parent:
    def __init__(self, child):
        self._child = child

    def __call__(self, *args):
        return ray.get(self._child.remote())


@serve.deployment
class GrandParent:
    def __init__(self, child, parent):
        self._child = child
        self._parent = parent

    def __call__(self, *args):
        # Check that the grandparent and parent are talking to the same child.
        assert ray.get(self._child.remote()) == ray.get(self._parent.remote())
        return "ok"


# TODO(Shreyas): Enable use_build once serve.build() PR is out.
@pytest.mark.parametrize("use_build", [False])
def test_pass_handle_to_multiple(serve_instance, use_build):
    child = Child.bind()
    parent = Parent.bind(child)
    grandparent = maybe_build(GrandParent.bind(child, parent), use_build)

    handle = serve.run(grandparent)
    assert ray.get(handle.remote()) == "ok"


def test_run_non_json_serializable_args(serve_instance):
    # Test that we can capture and bind non-json-serializable arguments.
    arr1 = np.zeros(100)
    arr2 = np.zeros(200)
    arr3 = np.zeros(300)

    @serve.deployment
    class A:
        def __init__(self, arr1, *, arr2):
            self.arr1 = arr1
            self.arr2 = arr2
            self.arr3 = arr3

        def __call__(self, *args):
            return self.arr1, self.arr2, self.arr3

    handle = serve.run(A.bind(arr1, arr2=arr2))
    ret1, ret2, ret3 = ray.get(handle.remote())
    assert all(
        [
            np.array_equal(ret1, arr1),
            np.array_equal(ret2, arr2),
            np.array_equal(ret3, arr3),
        ]
    )


@serve.deployment
def func():
    return 1


def test_single_functional_node_base_case(serve_instance):
    # Base case should work
    handle = serve.run(func.bind())
    assert ray.get(handle.remote()) == 1
    assert requests.get("http://127.0.0.1:8000/").text == "1"


def test_unsupported_bind():
    @serve.deployment
    class Actor:
        def ping(self):
            return "hello"

    with pytest.raises(AttributeError, match=r"\.bind\(\) cannot be used again on"):
        _ = Actor.bind().bind()

    with pytest.raises(AttributeError, match=r"\.bind\(\) cannot be used again on"):
        _ = Actor.bind().ping.bind().bind()

    with pytest.raises(
        AttributeError,
        match=r"\.remote\(\) cannot be used on ClassMethodNodes",
    ):
        actor = Actor.bind()
        _ = actor.ping.remote()


def test_unsupported_remote():
    @serve.deployment
    class Actor:
        def ping(self):
            return "hello"

    with pytest.raises(AttributeError, match=r"\'Actor\' has no attribute \'remote\'"):
        _ = Actor.bind().remote()

    @serve.deployment
    def func():
        return 1

    with pytest.raises(AttributeError, match=r"\.remote\(\) cannot be used on"):
        _ = func.bind().remote()


def test_suprious_call(serve_instance):
    # https://github.com/ray-project/ray/issues/24116

    @serve.deployment
    class CallTracker:
        def __init__(self):
            self.records = []

        def __call__(self, inp):
            self.records.append("__call__")

        def predict(self, inp):
            self.records.append("predict")

        def get(self):
            return self.records

    tracker = CallTracker.bind()
    with InputNode() as inp:
        dag = DAGDriver.bind(tracker.predict.bind(inp))
    handle = serve.run(dag)
    ray.get(handle.predict.remote(1))

    call_tracker = CallTracker.get_handle()
    assert ray.get(call_tracker.get.remote()) == ["predict"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
