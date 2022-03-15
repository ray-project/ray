import sys
import pytest

from ray.serve.api import _get_deployments_from_node
from ray.serve.handle import PipelineHandle
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode
import ray
from ray import serve


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


@ray.remote
def combine(*args):
    return sum(args)


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


def test_passing_handle(serve_instance):
    child = Adder.bind(1)
    parent = TakeHandle.bind(child)
    driver = Driver.bind(parent)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
