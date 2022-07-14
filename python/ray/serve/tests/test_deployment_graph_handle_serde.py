import sys

import pytest
from ray.serve.dag import InputNode
from ray.serve.deployment_graph_build import build as pipeline_build

import ray
from ray import serve


@serve.deployment
def func():
    pass


@serve.deployment
class Driver:
    def __init__(self, *args):
        pass

    def __call__(self, *args):
        pass


def test_environment_start():
    """Make sure that in the beginning ray hasn't been started"""
    assert not ray.is_initialized()


def test_func_building():
    dag = func.bind()
    assert len(pipeline_build(dag)) == 1


def test_class_building():
    dag = Driver.bind()
    assert len(pipeline_build(dag)) == 1


def test_dag_building():
    dag = Driver.bind(func.bind())
    assert len(pipeline_build(dag)) == 2


def test_nested_building():
    with InputNode() as inp:
        out = func.bind(inp)
        out = Driver.bind().__call__.bind(out)
        out = func.bind(out)
    dag = Driver.bind(out, func.bind())
    assert len(pipeline_build(dag)) == 5


def test_environment_end():
    """Make sure that in the end ray hasn't been started"""
    assert not ray.is_initialized()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
