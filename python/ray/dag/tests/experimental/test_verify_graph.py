# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import start_nccl_mock
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, num_gpus=1)
class MockedWorker:
    def __init__(self):
        pass

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def echo(self, value):
        return value


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_invalid_graph_1_actor(ray_start_regular):
    """
    The first a.echo writes to the second a.echo via the NCCL channel. However,
    the NCCL channel only supports synchronous communication and an actor can
    only execute one task at a time, so the graph is deadlocked.
    """
    a = MockedWorker.remote()

    ray.get(a.start_mock.remote())

    with InputNode() as inp:
        dag = a.echo.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.echo.bind(dag)

    with pytest.raises(AssertionError, match="The graph is not a DAG"):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_invalid_graph_2_actors_1(ray_start_regular):
    """
    The first a.echo writes to the second b.echo via the NCCL channel, and the
    first b.echo writes to the second a.echo via the NCCL channel. However, the
    NCCL channel only supports synchronous communication, so the graph is deadlocked.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.echo.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.echo.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode([
            a.echo.bind(branch2),
            b.echo.bind(branch1),
        ])

    with pytest.raises(AssertionError, match="The graph is not a DAG"):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_invalid_graph_2_actors_2(ray_start_regular):
    """
    The first a.echo writes to the second a.echo via the NCCL channel, and the
    first b.echo writes to the second b.echo via the NCCL channel. However, the
    NCCL channel only supports synchronous communication and an actor can only
    execute one task at a time, so the graph is deadlocked.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.echo.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.echo.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode([
            a.echo.bind(branch1),
            b.echo.bind(branch2),
        ])

    with pytest.raises(AssertionError, match="The graph is not a DAG"):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_invalid_graph_3_actors(ray_start_regular):
    """
    The first a.echo writes to the second b.echo via the NCCL channel, the
    first b.echo writes to the second c.echo via the NCCL channel, and the
    first c.echo writes to the second a.echo via the NCCL channel.
    """

    a = MockedWorker.remote()
    b = MockedWorker.remote()
    c = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote(), c.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.echo.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.echo.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        branch3 = c.echo.bind(inp)
        branch3.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode([
            a.echo.bind(branch3),
            b.echo.bind(branch1),
            c.echo.bind(branch2),
        ])

    with pytest.raises(AssertionError, match="The graph is not a DAG"):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_valid_graph_2_actors_1(ray_start_regular):
    """
    Driver -> a.echo -> b.echo -> a.echo -> b.echo -> a.echo -> b.echo -> Driver

    All communication between `a` and `b` is done via the NCCL channel.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        dag = a.echo.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.echo.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.echo.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.echo.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.echo.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.echo.bind(dag)

    compiled_dag = dag.experimental_compile()
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
