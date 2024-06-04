# coding: utf-8
import logging
import os
import sys
import torch

import pytest

import ray
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel.conftest import (
    Barrier,
    start_nccl_mock,
)
from ray.tests.conftest import *  # noqa
from ray.dag import InputNode

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, num_gpus=1)
class MockedWorker:
    def __init__(self):
        self.chan = None

    def start_mock(self):
        """
        Patch methods that require CUDA.
        """
        start_nccl_mock()

    def send(self, shape, dtype, value: int, send_as_dict=False):
        if send_as_dict:
            return self.send_dict([(value, shape, dtype, value)])

        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        if isinstance(tensor, dict):
            assert len(tensor) == 1
            tensor = tensor.values()[0]

        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def send_dict(self, entries):
        results = {}
        for key, value, shape, dtype in entries:
            results[key] = torch.ones(shape, dtype=dtype) * value
        return results

    def recv_dict(self, tensor_dict):
        results = []
        for key in sorted(tensor_dict.keys()):
            tensor = tensor_dict[key]
            results.append((key, tensor[0].item(), tensor.shape, tensor.dtype))
        return results


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p(ray_start_cluster):
    """
    Test simple sender -> receiver pattern. Check that receiver receives
    correct results.
    """
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    # Create a barrier in both directions because we don't know which rank will
    # get assigned to sender and receiver.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(
            inp.shape, inp.dtype, inp[0], send_as_dict=inp.send_as_dict
        )
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(
            i, shape=shape, dtype=dtype, send_as_dict=False
        )
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    # Sending tensors of different shape also works.
    for i in range(3):
        output_channel = compiled_dag.execute(
            i, shape=(20,), dtype=dtype, send_as_dict=False
        )
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, (20,), dtype)
        output_channel.end_read()

    # Sending tensors inside a dictionary also works.
    for i in range(3):
        output_channel = compiled_dag.execute(
            i, shape=shape, dtype=dtype, send_as_dict=True
        )
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    ray.kill(barrier1)
    ray.kill(barrier2)
    compiled_dag.teardown()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p_static_shape(ray_start_cluster):
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    # Create a barrier in both directions because we don't know which rank will
    # get assigned to sender and receiver.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", static_shape=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(i, shape=shape, dtype=dtype)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    ray.kill(barrier1)
    ray.kill(barrier2)
    compiled_dag.teardown()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p_static_shape_error(ray_start_cluster):
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    # Create a barrier in both directions because we don't know which rank will
    # get assigned to sender and receiver.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", static_shape=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        output_channel = compiled_dag.execute(i, shape=shape, dtype=dtype)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()

    # Sending wrong shape errors.
    output_channel = compiled_dag.execute(i, shape=(20,), dtype=dtype)
    with pytest.raises(OSError, match="Channel closed"):
        result = output_channel.begin_read()

    # Sending correct shape still errors because the DAG has already been torn
    # down after the previous error.
    with pytest.raises(OSError, match="Channel closed"):
        output_channel = compiled_dag.execute(i, shape=shape, dtype=dtype)

    ray.kill(barrier1)
    ray.kill(barrier2)
    compiled_dag.teardown()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p_static_non_tensor_data(ray_start_cluster):
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    # Create a barrier in both directions because we don't know which rank will
    # get assigned to sender and receiver.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send_dict.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(transport="nccl", static_non_tensor_data=True)
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    shape = 10
    dtype = torch.float16
    # Test sending a dictionary that always contains keys inserted in the same
    # order, but with different-sized and different-valued tensors.
    # same keys.
    for i in range(3):
        tensor_shape = (shape * (i + 1),)
        spec = [
            ("a", i, tensor_shape, dtype),
            ("b", i, tensor_shape, dtype),
            ("c", i, tensor_shape, dtype),
        ]
        output_channel = compiled_dag.execute(spec)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == spec
        output_channel.end_read()

    ray.kill(barrier1)
    ray.kill(barrier2)
    compiled_dag.teardown()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 2,
            "num_gpus": 2,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_p2p_static_non_tensor_data_error(ray_start_cluster):
    # Barrier name should be barrier-{sender rank}-{receiver rank}.
    # Create a barrier in both directions because we don't know which rank will
    # get assigned to sender and receiver.
    barrier1 = Barrier.options(name="barrier-0-1").remote()  # noqa
    barrier2 = Barrier.options(name="barrier-1-0").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send_dict.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(transport="nccl", static_non_tensor_data=True)
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    shape = 10
    dtype = torch.float16
    # Test sending a dictionary that always contains keys inserted in the same
    # order, but with different-sized and different-valued tensors.
    # same keys.
    for i in range(3):
        tensor_shape = (shape * (i + 1),)
        spec = [
            ("a", i, tensor_shape, dtype),
            ("b", i, tensor_shape, dtype),
            ("c", i, tensor_shape, dtype),
        ]
        output_channel = compiled_dag.execute(spec)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == spec
        output_channel.end_read()

    # Test sending a dictionary with a different number of entries.
    spec = [
        ("a", 1, shape, dtype),
        ("b", 1, shape, dtype),
    ]
    output_channel = compiled_dag.execute(spec)
    with pytest.raises(OSError, match="Channel closed"):
        result = output_channel.begin_read()

    # Sending correct shape still errors because the DAG has already been torn
    # down after the previous error.
    spec.append(("c", 1, shape, dtype))
    with pytest.raises(OSError, match="Channel closed"):
        output_channel = compiled_dag.execute(spec)

    # TODO(swang): Ideally we would also check that sending a different value,
    # e.g., changing the keys of the dictionary or sending a list instead of a
    # dictionary, would also fail, but currently we don't have a reliable way
    # to do this. Checking the serialized output produces many false positives.

    ray.kill(barrier1)
    ray.kill(barrier2)
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
