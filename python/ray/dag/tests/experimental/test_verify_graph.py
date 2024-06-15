# coding: utf-8
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import start_nccl_mock
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode

INVALID_GRAPH = "Detect a deadlock in the graph. If this is a false positive, "
"please disable the graph verification by setting the environment "
"variable RAY_ADAG_ENABLE_VERIFY_GRAPH to 0."


@ray.remote(num_cpus=0, num_gpus=1)
class MockedWorker:
    def __init__(self):
        pass

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def no_op(self, value):
        return value

    def no_op_two(self, value1, value2):
        return value1, value2


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_invalid_graph_1_actor(ray_start_regular):
    """
    The first a.no_op writes to the second a.no_op via the NCCL channel. However,
    the NCCL channel only supports synchronous communication and an actor can
    only execute one task at a time, so the graph is deadlocked.
    """
    a = MockedWorker.remote()

    ray.get(a.start_mock.remote())

    with InputNode() as inp:
        dag = a.no_op.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.no_op.bind(dag)

    with pytest.raises(AssertionError, match=INVALID_GRAPH):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_invalid_graph_2_actors_1(ray_start_regular):
    """
    The first a.no_op writes to the second b.no_op via the NCCL channel, and the
    first b.no_op writes to the second a.no_op via the NCCL channel. However, the
    NCCL channel only supports synchronous communication, so the graph is deadlocked.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.no_op.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.no_op.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode(
            [
                a.no_op.bind(branch2),
                b.no_op.bind(branch1),
            ]
        )

    with pytest.raises(AssertionError, match=INVALID_GRAPH):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_invalid_graph_2_actors_2(ray_start_regular):
    """
    The first a.no_op writes to the second a.no_op via the NCCL channel, and the
    first b.no_op writes to the second b.no_op via the NCCL channel. However, the
    NCCL channel only supports synchronous communication and an actor can only
    execute one task at a time, so the graph is deadlocked.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.no_op.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.no_op.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode(
            [
                a.no_op.bind(branch1),
                b.no_op.bind(branch2),
            ]
        )

    with pytest.raises(AssertionError, match=INVALID_GRAPH):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_invalid_graph_2_actors_3(ray_start_regular):
    """
    The first a.no_op writes to the second a.no_op and the b.no_op via the NCCL channels.
    However, the NCCL channel only supports synchronous communication and an actor can
    only execute one task at a time, so the graph is deadlocked.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        dag = a.no_op.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode(
            [
                a.no_op.bind(dag),
                b.no_op.bind(dag),
            ]
        )

    with pytest.raises(AssertionError, match=INVALID_GRAPH):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_invalid_graph_3_actors(ray_start_regular):
    """
    The first a.no_op writes to the second b.no_op via the NCCL channel, the
    first b.no_op writes to the second c.no_op via the NCCL channel, and the
    first c.no_op writes to the second a.no_op via the NCCL channel.
    """

    a = MockedWorker.remote()
    b = MockedWorker.remote()
    c = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote(), c.start_mock.remote()])

    with InputNode() as inp:
        branch1 = a.no_op.bind(inp)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = b.no_op.bind(inp)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        branch3 = c.no_op.bind(inp)
        branch3.with_type_hint(TorchTensorType(transport="nccl"))
        dag = MultiOutputNode(
            [
                a.no_op.bind(branch3),
                b.no_op.bind(branch1),
                c.no_op.bind(branch2),
            ]
        )

    with pytest.raises(AssertionError, match=INVALID_GRAPH):
        dag.experimental_compile()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_valid_graph_2_actors(ray_start_regular):
    """
    Driver -> a.no_op -> b.no_op -> a.no_op -> b.no_op -> a.no_op -> b.no_op -> Driver

    All communication between `a` and `b` is done via the NCCL channel.
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote()])

    with InputNode() as inp:
        dag = a.no_op.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.no_op.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.no_op.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.no_op.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.no_op.bind(dag)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = b.no_op.bind(dag)

    compiled_dag = dag.experimental_compile()
    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 3}], indirect=True)
def test_valid_graph_3_actors(ray_start_regular):
    """
    Driver -> a.no_op -> b.no_op -> a.no_op_two -> Driver
                      |          |
                      -> c.no_op -
    """
    a = MockedWorker.remote()
    b = MockedWorker.remote()
    c = MockedWorker.remote()

    ray.get([a.start_mock.remote(), b.start_mock.remote(), c.start_mock.remote()])

    with InputNode() as inp:
        dag = a.no_op.bind(inp)
        dag.with_type_hint(TorchTensorType(transport="nccl"))
        branch1 = b.no_op.bind(dag)
        branch1.with_type_hint(TorchTensorType(transport="nccl"))
        branch2 = c.no_op.bind(dag)
        branch2.with_type_hint(TorchTensorType(transport="nccl"))
        dag = a.no_op_two.bind(branch1, branch2)

    compiled_dag = dag.experimental_compile()
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
