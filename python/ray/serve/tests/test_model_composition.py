import array
import asyncio
import os
import sys
from typing import Dict, Union

import httpx
import pytest
import starlette.requests

from ray import serve
from ray.serve._private.test_utils import get_application_url
from ray.serve.handle import DeploymentHandle

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
        m1: DeploymentHandle,
        m2: Union[DeploymentHandle, Dict[str, DeploymentHandle]],
    ):
        self.m1 = m1
        if isinstance(m2, dict):
            self.m2 = m2.get(NESTED_HANDLE_KEY)
        else:
            self.m2 = m2

    async def predict(self, req):
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        return sum(await asyncio.gather(r1_ref, r2_ref))

    async def __call__(self, request: starlette.requests.Request):
        req = await request.json()
        return await self.predict(req)


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

        def __call__(self):
            return self.get()

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
    def __init__(self, h: DeploymentHandle):
        self._h = h

    async def __call__(self):
        return await self._h.remote()


def test_single_func_no_input(serve_instance):
    dag = fn_hello.bind()
    serve_dag = NoargDriver.bind(dag)

    handle = serve.run(serve_dag)
    url = get_application_url()
    assert handle.remote().result() == "hello"
    assert httpx.get(url).text == "hello"


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    m1 = Model.bind(2)
    m2 = Model.bind(3)
    serve_dag = Combine.bind(m1, m2=m2)

    handle = serve.run(serve_dag)
    url = get_application_url()
    assert handle.predict.remote(1).result() == 5
    assert httpx.post(url, json=1).json() == 5


def test_shared_deployment_handle(serve_instance):
    m = Model.bind(2)
    serve_dag = Combine.bind(m, m2=m)

    handle = serve.run(serve_dag)
    url = get_application_url()
    assert handle.predict.remote(1).result() == 4
    assert httpx.post(url, json=1).json() == 4


def test_multi_instantiation_class_nested_deployment_arg_dag(serve_instance):
    m1 = Model.bind(2)
    m2 = Model.bind(3)
    serve_dag = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2})

    handle = serve.run(serve_dag)
    url = get_application_url()
    assert handle.predict.remote(1).result() == 5
    assert httpx.post(url, json=1).json() == 5


def test_class_factory(serve_instance):
    serve_dag = serve.deployment(class_factory()).bind(3)
    handle = serve.run(serve_dag)
    url = get_application_url()
    assert handle.get.remote().result() == 3
    assert httpx.get(url).text == "3"


@serve.deployment
class Echo:
    def __init__(self, s: str):
        self._s = s

    def __call__(self, *args):
        return self._s


def test_single_node_deploy_success(serve_instance):
    m1 = Adder.bind(1)
    handle = serve.run(m1)
    assert handle.remote(41).result() == 42


@serve.deployment
class TakeHandle:
    def __init__(self, handle) -> None:
        self.handle = handle

    async def predict(self, inp):
        return await self.handle.remote(inp)

    async def __call__(self, request: starlette.requests.Request):
        inp = await request.json()
        return await self.predict(inp)


def test_passing_handle(serve_instance):
    child = Adder.bind(1)
    parent = TakeHandle.bind(child)
    handle = serve.run(parent)
    url = get_application_url()
    assert handle.predict.remote(1).result() == 2
    assert httpx.post(url, json=1).json() == 2


@serve.deployment
class DictParent:
    def __init__(self, d: Dict[str, DeploymentHandle]):
        self._d = d

    async def __call__(self, key: str):
        return await self._d[key].remote()


def test_passing_handle_in_obj(serve_instance):
    child1 = Echo.bind("ed")
    child2 = Echo.bind("simon")
    parent = DictParent.bind({"child1": child1, "child2": child2})

    handle = serve.run(parent)
    assert handle.remote("child1").result() == "ed"
    assert handle.remote("child2").result() == "simon"


@serve.deployment
class Child:
    def __call__(self, *args):
        return os.getpid()


@serve.deployment
class Parent:
    def __init__(self, child):
        self._child = child

    async def __call__(self, *args):
        return await self._child.remote()


@serve.deployment
class GrandParent:
    def __init__(self, child, parent):
        self._child = child
        self._parent = parent

    async def __call__(self, *args):
        # Check that the grandparent and parent are talking to the same child.
        assert await self._child.remote() == await self._parent.remote()
        return "ok"


def test_pass_handle_to_multiple(serve_instance):
    child = Child.bind()
    parent = Parent.bind(child)
    grandparent = GrandParent.bind(child, parent)

    handle = serve.run(grandparent)
    assert handle.remote().result() == "ok"


def test_run_non_json_serializable_args(serve_instance):
    # Test that we can capture and bind non-json-serializable arguments.
    arr1 = array.array("d", [1.0, 2.0, 3.0])
    arr2 = array.array("d", [2.0, 3.0, 4.0])
    arr3 = array.array("d", [3.0, 4.0, 5.0])

    @serve.deployment
    class A:
        def __init__(self, arr1, *, arr2):
            self.arr1 = arr1
            self.arr2 = arr2
            self.arr3 = arr3

        def __call__(self, *args):
            return self.arr1, self.arr2, self.arr3

    handle = serve.run(A.bind(arr1, arr2=arr2))
    ret1, ret2, ret3 = handle.remote().result()
    assert all(
        [
            ret1 == arr1,
            ret2 == arr2,
            ret3 == arr3,
        ]
    )


@serve.deployment
def func():
    return 1


def test_single_functional_node_base_case(serve_instance):
    # Base case should work
    handle = serve.run(func.bind())
    url = get_application_url()
    assert handle.remote().result() == 1
    assert httpx.get(url).text == "1"


def test_unsupported_remote():
    @serve.deployment
    class Actor:
        def ping(self):
            return "hello"

    with pytest.raises(
        AttributeError, match=r"\'Application\' object has no attribute \'remote\'"
    ):
        _ = Actor.bind().remote()

    @serve.deployment
    def func():
        return 1

    with pytest.raises(
        AttributeError, match=r"\'Application\' object has no attribute \'remote\'"
    ):
        _ = func.bind().remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
