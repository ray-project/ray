# coding: utf-8
import os
import sys
import torch

import pytest

import ray
import ray.cluster_utils
from ray.exceptions import RayChannelError, RayTaskError
from ray.experimental.channel.conftest import (
    Barrier,
    start_nccl_mock,
)
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import wait_for_condition
from ray.dag import InputNode


def error_logged(capsys, msg):
    out, err = capsys.readouterr()
    # Write captured back to stdout, stderr for easier test debugging.
    sys.stdout.write(out)
    sys.stderr.write(err)
    return msg in err


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
            return self.send_dict([(value, value, shape, dtype)])

        return torch.ones(shape, dtype=dtype) * value

    def recv(self, tensor):
        if isinstance(tensor, dict):
            assert len(tensor) == 1
            tensor = list(tensor.values())[0]

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
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0], inp.send_as_dict)
        dag = dag.with_tensor_transport(transport="nccl")
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype, send_as_dict=False)
        assert ray.get(ref) == (i, shape, dtype)

    # Sending tensors of different shape also works.
    for i in range(3):
        ref = compiled_dag.execute(i, shape=(20,), dtype=dtype, send_as_dict=False)
        assert ray.get(ref) == (i, (20,), dtype)

    # Sending tensors inside a dictionary also works.
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype, send_as_dict=True)
        assert ray.get(ref) == (i, shape, dtype)

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
@pytest.mark.parametrize("send_as_dict", [True, False])
def test_p2p_static_shape(ray_start_cluster, send_as_dict):
    """
    Test simple send -> recv pattern with
    _static_shape=True. If sender always sends tensors of
    the same shape, then it works.
    """
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0], send_as_dict=send_as_dict)
        dag = dag.with_tensor_transport(transport="nccl", _static_shape=True)
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)


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
@pytest.mark.parametrize("send_as_dict", [True, False])
def test_p2p_static_shape_error(capsys, ray_start_cluster, send_as_dict):
    """
    Test that when static_shape=True, an error is thrown when a tensor with a
    different shape or dtype is found.
    """
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0], send_as_dict=send_as_dict)
        dag = dag.with_tensor_transport(transport="nccl", _static_shape=True)
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

    # Sending wrong shape errors.
    ref = compiled_dag.execute(i, shape=(20,), dtype=dtype)
    with pytest.raises(RayTaskError):
        ray.get(ref)

    # Sending correct shape still errors because the DAG has already been torn
    # down after the previous error.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)

    wait_for_condition(
        lambda: error_logged(
            capsys,
            "ValueError: Expected torch.Tensors with shapes and dtypes: "
            "[(shape=torch.Size([10]), dtype=torch.float16)], found: "
            "[(shape=torch.Size([20]), dtype=torch.float16)]",
        )
    )


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
def test_p2p_direct_return(ray_start_cluster):
    """
    Test simple sender -> receiver pattern with _direct_return=True
    """
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_as_dict)
        dag = dag.with_tensor_transport(
            transport="nccl",
            _direct_return=True,
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    dtype = torch.float16
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=i, send_as_dict=False
        )
        assert ray.get(ref) == (i, shape, dtype)


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
def test_p2p_direct_return_error(capsys, ray_start_cluster):
    """
    Test simple sender -> receiver pattern with
    _direct_return=True. Test that error is thrown when
    actor task does not return a tensor directly.
    """
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_as_dict)
        dag = dag.with_tensor_transport(
            transport="nccl",
            _direct_return=True,
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    dtype = torch.float16
    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=i, send_as_dict=False
        )
        assert ray.get(ref) == (i, shape, dtype)

    # Error is thrown if we do not send a tensor.
    ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_as_dict=True)
    with pytest.raises(RayTaskError):
        ray.get(ref)

    # Currently the receiver cannot catch the exception so the DAG cannot be
    # used again.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=1, send_as_dict=False
        )

    wait_for_condition(
        lambda: error_logged(
            capsys,
            "Task annotated with _direct_return=True must "
            "return a CUDA torch.Tensor",
        )
    )


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
@pytest.mark.parametrize("check_static_shape", [True, False])
def test_p2p_static_shape_and_direct_return(
    capsys, ray_start_cluster, check_static_shape
):
    """
    Test simple sender -> receiver pattern with both _static_shape=True and
    _direct_return=True. Check errors are thrown if tensors with wrong shape
    are passed (check_static_shape=True) OR if non-tensor value is returned
    (check_static_shape=False).
    """
    # Barrier name should be barrier-{lower rank}-{higher rank}.
    barrier = Barrier.options(name="barrier-0-1").remote()  # noqa

    sender = MockedWorker.remote()
    receiver = MockedWorker.remote()

    ray.get([sender.start_mock.remote(), receiver.start_mock.remote()])

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_as_dict)
        dag = dag.with_tensor_transport(
            transport="nccl",
            _static_shape=True,
            _direct_return=True,
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    shape = (10,)
    dtype = torch.float16
    for i in range(3):
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=i, send_as_dict=False
        )
        assert ray.get(ref) == (i, shape, dtype)

    if check_static_shape:
        # Error is thrown if we send the wrong shape.
        ref = compiled_dag.execute(
            shape=(20,), dtype=dtype, value=1, send_as_dict=False
        )
    else:
        # Error is thrown if we do not send a tensor.
        ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_as_dict=True)

    with pytest.raises(RayTaskError):
        ray.get(ref)

    # Currently the receiver cannot catch either kind of
    # exception so the DAG cannot be used again.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(
            shape=shape, dtype=dtype, value=1, send_as_dict=False
        )

    if check_static_shape:
        msg = (
            "ValueError: Expected torch.Tensors with shapes and dtypes: "
            "[(shape=torch.Size([10]), dtype=torch.float16)], found: "
            "[(shape=torch.Size([20]), dtype=torch.float16)]"
        )
    else:
        msg = (
            "Task annotated with _direct_return=True must " "return a CUDA torch.Tensor"
        )
    wait_for_condition(lambda: error_logged(capsys, msg))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
