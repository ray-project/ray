# coding: utf-8
import logging
import os
import sys
import torch
import time

import pytest

from ray.exceptions import RayChannelError
import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode
from ray.tests.conftest import *  # noqa

from ray.experimental.channel.torch_tensor_type import (
    TorchTensorType,
)


logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int, send_tensor=True):
        if not send_tensor:
            return 1
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def send_or_raise(self, shape, dtype, value: int, raise_exception=False):
        if raise_exception:
            raise RuntimeError()
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def send_dict_with_tuple_args(self, args):
        shape, dtype, value = args
        return {
            i: torch.ones(shape, dtype=dtype, device=self.device) * i
            for i in range(value)
        }

    def send_with_tuple_args(self, args):
        # Hack because InputNode can currently only contain one arg.
        shape, dtype, value = args
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def recv(self, tensor):
        # Check that tensor got loaded to the correct device.
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)

    def recv_dict(self, tensor_dict):
        vals = {}
        for i, tensor in tensor_dict.items():
            vals[i] = self.recv(tensor)
        return vals

    def ping(self):
        return


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_p2p(ray_start_regular):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(shape, dtype, _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()

    # Passing tensors of a similar or smaller shape is okay.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType((20,), dtype, _direct_return=True))
        dag = receiver.recv.bind(dag)
    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)
    compiled_dag.teardown()

    # Passing a torch.tensor inside of other data is okay even if
    # _direct_return=True, if `transport` is not set.
    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(
                _shape=shape,
                _dtype=dtype,
                _direct_return=True,
            )
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute((shape, dtype, 1))
    ray.get(ref)
    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_as_dag_input(ray_start_regular):
    if USE_GPU:
        assert sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 0

    actor_cls = TorchTensorWorker
    if USE_GPU:
        actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Test torch.Tensor as input.
    with InputNode() as inp:
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        torch_inp = inp.with_type_hint(
            TorchTensorType(shape, dtype, _direct_return=True)
        )
        dag = receiver.recv.bind(torch_inp)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(torch.ones(shape, dtype=dtype) * i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    # Passing tensors of a similar or smaller shape is okay.
    for i in range(3):
        ref = compiled_dag.execute(torch.ones((20,), dtype=dtype) * i)
        result = ray.get(ref)
        assert result == (i, (20,), dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(
            TorchTensorType(shape, dtype, transport="nccl", _direct_return=True)
        )
        dag = receiver.recv.bind(dag)

    # Test normal execution.
    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()

    # Test that InputNode cannot cannot participate in the NCCL group.
    with InputNode() as inp:
        torch_inp = inp.with_type_hint(TorchTensorType(shape, dtype, transport="nccl"))
        dag = receiver.recv.bind(torch_inp)
    with pytest.raises(
        ValueError,
        match=(
            r"DAG inputs cannot be transferred "
            "via NCCL because the driver cannot participate in the NCCL group"
        ),
    ):
        compiled_dag = dag.experimental_compile()

    # Test that actors can be reused for a valid DAG.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = dag.with_type_hint(TorchTensorType(shape, dtype, transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == (i, shape, dtype)
    compiled_dag.teardown()

    # TODO(swang): Check that actors are still alive. Currently this fails due
    # to a ref counting assertion error.
    # ray.get(sender.ping.remote())
    # ray.get(receiver.ping.remote())


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_dynamic(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_with_tuple_args.bind(inp)
        # TODO(swang): Test that we are using the minimum number of
        # channels/messages when _direct_return=True.
        dag = dag.with_type_hint(TorchTensorType(transport="nccl", _direct_return=True))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(3):
        i += 1
        shape = (i * 10,)
        dtype = torch.float16
        args = (shape, dtype, i)
        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_wrong_shape(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    dtype = torch.float16

    # Passing tensors of the wrong shape will error.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value)
        dag = dag.with_type_hint(
            TorchTensorType(
                (20,),
                dtype,
                transport="nccl",
            )
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(shape=(20,), dtype=dtype, value=1)
    ray.get(ref) == (1, (20,), dtype)

    ref = compiled_dag.execute(shape=(10,), dtype=dtype, value=1)

    with pytest.raises(RayChannelError):
        ray.get(ref)

    # For tensors where the shape is declared to be static, the DAG will be
    # torn down after any task throws an application-level exception, such as
    # when the task returns torch.Tensors of the wrong shape or dtype. Check
    # that we can no longer submit to the DAG.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(shape=(20,), dtype=dtype, value=1)

    compiled_dag.teardown()

    # TODO(swang): This currently requires time.sleep to avoid some issue with
    # following tests.
    time.sleep(3)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_nested(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is statically
    declared.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(
            TorchTensorType(_shape=shape, _dtype=dtype, transport="nccl")
        )
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        args = (shape, dtype, 1)

        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        expected_result = {0: (0, shape, dtype)}
        assert result == expected_result

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_nested_dynamic(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is
    dynamically declared, and there may be multiple tensors.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_dict_with_tuple_args.bind(inp)
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv_dict.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1

        shape = (10 * i,)
        dtype = torch.float16
        args = (shape, dtype, i)

        ref = compiled_dag.execute(args)
        result = ray.get(ref)
        expected_result = {j: (j, shape, dtype) for j in range(i)}
        assert result == expected_result

    compiled_dag.teardown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_nccl_direct_return_error(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_cpus=0, num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    shape = (10,)
    dtype = torch.float16

    # Passing a non-tensor value when _direct_return=True and tranport="nccl"
    # fails.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp.value, inp.send_tensor)
        dag = dag.with_type_hint(
            TorchTensorType(
                transport=TorchTensorType.NCCL,
                _direct_return=True,
            )
        )
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=True)
    assert ray.get(ref) == (1, shape, dtype)

    ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=False)
    with pytest.raises(RayChannelError):
        ray.get(ref)

    # For direct_return=True tensors, the DAG will be torn down after any task
    # throws an application-level exception, such as when the task returns
    # something other than a torch.Tensor. Check that we can no longer submit
    # to the DAG.
    with pytest.raises(RayChannelError):
        ref = compiled_dag.execute(shape=shape, dtype=dtype, value=1, send_tensor=True)

    compiled_dag.teardown()

    # TODO(swang): This currently requires time.sleep to avoid some issue with
    # following tests.
    time.sleep(3)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_torch_tensor_exceptions(ray_start_regular):
    """
    Test nested torch.Tensor passed via NCCL. Its shape and dtype is
    dynamically declared, and there may be multiple tensors.
    """
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    assert (
        sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
    ), "This test requires at least 2 GPUs"

    actor_cls = TorchTensorWorker.options(num_gpus=1)

    sender = actor_cls.remote()
    receiver = actor_cls.remote()

    with InputNode() as inp:
        dag = sender.send_or_raise.bind(
            inp.shape, inp.dtype, inp.value, inp.raise_exception
        )
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        i += 1

        shape = (10 * i,)
        dtype = torch.float16

        ref = compiled_dag.execute(
            shape=shape,
            dtype=dtype,
            value=i,
            raise_exception=False,
        )
        result = ray.get(ref)
        assert result == (i, shape, dtype)

    # Application level exceptions are thrown to the end ray.get
    ref = compiled_dag.execute(
        shape=shape,
        dtype=dtype,
        value=i,
        raise_exception=True,
    )
    with pytest.raises(RuntimeError):
        ray.get(ref)

    # If using dynamic shape or dtype is used and direct_return=False, then the
    # DAG should still be usable after application-level exceptions.
    ref = compiled_dag.execute(
        shape=shape,
        dtype=dtype,
        value=i,
        raise_exception=False,
    )
    result = ray.get(ref)
    assert result == (i, shape, dtype)

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
