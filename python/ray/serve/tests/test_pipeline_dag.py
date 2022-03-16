import pytest
import os
import sys
from typing import Union

import numpy as np

import ray
from ray import serve
from ray.serve.api import Application, DeploymentNode, _get_deployments_from_node
from ray.serve.handle import PipelineHandle
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode


def maybe_build(
    node: DeploymentNode, use_build: bool
) -> Union[Application, DeploymentNode]:
    return Application.from_dict(serve.build(node).to_dict())


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

    def __call__(self, inp: int) -> int:
        print(f"Driver got {inp}")
        return ray.get(self.dag.remote(inp))


@serve.deployment
class Echo:
    def __init__(self, s: str):
        self._s = s

    def __call__(self, *args):
        return self._s


@ray.remote
def combine(*args):
    return sum(args)


def test_single_node_deploy_success(serve_instance):
    m1 = Adder.bind(1)
    handle = serve.run(m1)
    assert ray.get(handle.remote(41)) == 42


@pytest.mark.parametrize("use_build", [False, True])
def test_single_node_driver_sucess(serve_instance, use_build):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with PipelineInputNode() as input_node:
        out = m1.forward.bind(input_node)
        out = m2.forward.bind(out)

    driver = maybe_build(Driver.bind(out), use_build)
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


@pytest.mark.skip("TODO")
def test_mixing_task(serve_instance):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with PipelineInputNode() as input_node:
        out = combine.bind(m1.forward.bind(input_node), m2.forward.bind(input_node))
    driver = Driver.bind(out)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 5


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
    driver = maybe_build(Driver.bind(parent), use_build)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 2


@serve.deployment
class DictParent:
    def __init__(self, d):
        self._d = d

    async def __call__(self, key):
        return await self._d[key].remote()


@pytest.mark.parametrize("use_build", [False, True])
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


@pytest.mark.parametrize("use_build", [False, True])
def test_pass_handle_to_multiple(serve_instance, use_build):

    child = Child.bind()
    parent = Parent.bind(child)
    grandparent = maybe_build(GrandParent.bind(child, parent), use_build)

    handle = serve.run(grandparent)
    assert ray.get(handle.remote()) == "ok"


def test_non_json_serializable_args(serve_instance):
    # Test that we can capture and bind non-json-serializable arguments.
    arr1 = np.zeros(100)
    arr2 = np.zeros(200)

    @serve.deployment
    class A:
        def __init__(self, arr1):
            self.arr1 = arr1
            self.arr2 = arr2

        def __call__(self, *args):
            return self.arr1, self.arr2

    handle = serve.run(A.bind(arr1))
    ret1, ret2 = ray.get(handle.remote())
    assert np.array_equal(ret1, arr1) and np.array_equal(ret2, arr2)

    # TODO: check that serve.build raises an exception.


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
