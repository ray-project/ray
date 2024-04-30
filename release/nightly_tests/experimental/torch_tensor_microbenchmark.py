# coding: utf-8
import logging
import os
import sys
import torch
import pickle
import io
import cupy
import numpy as np
import time

import pytest

import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode
from ray.tests.conftest import *  # noqa
from ray.util.collective.collective_group import nccl_util

from ray.dag.experimental.types import TorchTensorType
from ray._private.ray_microbenchmark_helpers import timeit, asyncio_timeit

# from ray.experimental.torch_serializer import TorchTensor


logger = logging.getLogger(__name__)


SHAPE = (500_000_000,)
DTYPE = torch.float16

NUM_ITERS = 10


@ray.remote
class TorchIpcWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int):
        t = torch.ones(shape, dtype=dtype, device=self.device) * value
        h = cupy.cuda.runtime.ipcGetMemHandle(t.data_ptr())
        return h

    def recv(self, device_ptr, num_bytes, shape, dtype):
        h = cupy.cuda.runtime.ipcOpenMemHandle(device_ptr)
        m = cupy.cuda.UnownedMemory(h, num_bytes, None)
        m_ptr = cupy.cuda.MemoryPointer(m, 0)
        tensor = torch.tensor(cupy.ndarray(shape, dtype, m_ptr), device=self.device)
        assert tensor.device == self.device
        if self.device.type == "cuda":
            torch.cuda.synchronize()
        return (tensor[0].item(), tensor.shape, tensor.dtype)


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int, use_tensor):
        t = torch.ones(shape, dtype=dtype, device=self.device) * value
        if use_tensor:
            return t

        return t.numpy()

    def recv(self, tensor):
        if isinstance(tensor, np.ndarray):
            tensor = torch.tensor(tensor)

        assert tensor.device == self.device
        if self.device.type == "cuda":
            torch.cuda.synchronize()
        return (tensor[0].item(), tensor.shape, tensor.dtype)


@ray.remote(num_gpus=1)
class NcclWorker:
    def __init__(self, world_size, rank, comm_id):
        from ray.air._internal import torch_utils
        import cupy.cuda.nccl

        self.device = torch_utils.get_devices()[0]
        self.world_size = world_size
        self.rank = rank

        torch.distributed.init_process_group(
            backend="nccl", world_size=world_size, rank=rank
        )

    def _send(self, buf, num_el, rank):
        torch.distributed.send(buf, rank)

    def _recv(self, buf, num_el, rank):
        torch.distributed.recv(buf, rank)

    def do_send_recv(self, shape, dtype):
        other_rank = (self.rank + 1) % self.world_size

        def _run():

            if self.rank == 0:
                i = np.random.randint(100)
                input_buffer = torch.ones(shape, dtype=dtype, device=self.device) * i
                self._send(input_buffer, input_buffer.numel(), other_rank)
            else:
                input_buffer = torch.zeros(shape, dtype=dtype, device=self.device)
                self._recv(input_buffer, input_buffer.numel(), other_rank)

            torch.cuda.synchronize()

        timeit("exec_nccl_gpu", _run)


def exec_ray_dag(
    label, sender, receiver, use_tensor=True, use_nccl=False, use_adag=True
):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(SHAPE, DTYPE, inp, use_tensor)

        if use_adag and use_tensor:
            dag = dag.with_type_hint(
                TorchTensorType(SHAPE, DTYPE, transport="nccl" if use_nccl else None)
            )

        dag = receiver.recv.bind(dag)

    if use_adag:
        dag = dag.experimental_compile(buffer_size_bytes=int(SHAPE[0] * 3))

        def _run():
            i = np.random.randint(100)
            output_channel = dag.execute(i)
            # TODO(swang): Replace with fake ObjectRef.
            result = output_channel.begin_read()
            assert result == (i, SHAPE, DTYPE)
            output_channel.end_read()

    else:

        def _run():
            i = np.random.randint(100)
            result = ray.get(dag.execute(i))
            assert result == (i, SHAPE, DTYPE)

    timeit(label, _run)

    if use_adag:
        dag.teardown()


def exec_ray_dag_ipc(label, sender, receiver, use_tensor=True, use_nccl=False):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(SHAPE, DTYPE, inp)
        dag = receiver.recv.bind(
            dag,
            SHAPE[0] * DTYPE.itemsize,
            SHAPE,
            nccl_util.TORCH_NUMPY_DTYPE_MAP[DTYPE],
        )

    compiled_dag = dag.experimental_compile(buffer_size_bytes=int(SHAPE[0] * 3))

    def _run():
        i = np.random.randint(100)
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        # assert result == (i, SHAPE, DTYPE), (result, (i, SHAPE, DTYPE))
        output_channel.end_read()
        i += 1

    timeit(label, _run)
    compiled_dag.teardown()


def _exec_torch_cpu_cpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    t2 = t.to(copy=True)
    assert (t2[0].item(), t2.shape, t2.dtype) == (i, SHAPE, DTYPE)


def _exec_torch_gpu():
    i = np.random.randint(100)
    device_from = torch.device("cuda:1")
    device_to = torch.device("cuda:0")

    t = torch.ones(SHAPE, dtype=DTYPE, device=device_from) * i
    t2 = t.to(device_to)
    torch.cuda.synchronize(device_to)
    assert (t2[0].item(), t2.shape, t2.dtype) == (i, SHAPE, DTYPE)


def exec_nccl_gpu():
    import cupy.cuda.nccl

    comm_id = cupy.cuda.nccl.get_unique_id()
    workers = [NcclWorker.remote(2, i, comm_id) for i in range(2)]
    tasks = [worker.do_send_recv.remote(SHAPE, DTYPE) for worker in workers]
    ray.wait(tasks, num_returns=1)

    # Workaround for Ray bug in reusing GPUs too quickly.
    for worker in workers:
        ray.kill(worker)
    time.sleep(1)


def _exec_torch_gpu_cpu_gpu():
    i = np.random.randint(100)
    device_from = torch.device("cuda:0")
    device_to = torch.device("cuda:1")
    t = torch.ones(SHAPE, dtype=DTYPE, device=device_from) * i
    t = t.to("cpu")
    t2 = t.to(device_to)
    torch.cuda.synchronize(device_to)
    assert (t2[0].item(), t2.shape, t2.dtype) == (i, SHAPE, DTYPE)


def _exec_pickle_cpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    byte_stream = io.BytesIO()
    pickle.dump(t, byte_stream)
    byte_stream.seek(0)
    pickle.load(byte_stream)


def _exec_pickle_gpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE, device="cuda") * i
    byte_stream = io.BytesIO()
    pickle.dump(t, byte_stream)
    byte_stream.seek(0)
    pickle.load(byte_stream)


def _exec_ray_put_cpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    ray.get(ray.put(t))


def _exec_ray_put_np():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    torch.as_tensor(ray.get(ray.put(t.numpy())))


def _exec_ray_put_np_copy():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    torch.tensor(ray.get(ray.put(t.numpy())))


def _exec_ray_put_gpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE, device="cuda") * i
    ray.get(ray.put(t))


def exec_ray_dag_cpu():
    sender = TorchTensorWorker.options().remote()
    receiver = TorchTensorWorker.options().remote()
    exec_ray_dag("exec_ray_dag_cpu", sender, receiver)


def exec_ray_core_dag_cpu():
    time.sleep(1)
    sender = TorchTensorWorker.remote()
    receiver = TorchTensorWorker.remote()
    exec_ray_dag("exec_ray_core_dag_cpu", sender, receiver, use_adag=False)


def exec_ray_dag_np():
    sender = TorchTensorWorker.options().remote()
    receiver = TorchTensorWorker.options().remote()
    exec_ray_dag("exec_ray_dag_np", sender, receiver, use_tensor=False)


def exec_ray_dag_gpu_ipc_gpu():
    time.sleep(1)
    sender = TorchIpcWorker.options(num_gpus=1).remote()
    receiver = TorchIpcWorker.options(num_gpus=1).remote()
    exec_ray_dag_ipc("exec_ray_dag_gpu_ipc_gpu", sender, receiver)

    # Workaround for Ray bug in reusing GPUs too quickly.
    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)


def exec_ray_dag_gpu_cpu_gpu():
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    exec_ray_dag("exec_ray_dag_gpu_cpu_gpu", sender, receiver)

    # Workaround for Ray bug in reusing GPUs too quickly.
    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)


def exec_ray_dag_gpu():
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    exec_ray_dag("exec_ray_dag_gpu", sender, receiver, use_nccl=True)

    # Workaround for Ray bug in reusing GPUs too quickly.
    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)


def exec_ray_core_dag_gpu():
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    exec_ray_dag(
        "exec_ray_core_dag_gpu", sender, receiver, use_nccl=True, use_adag=False
    )

    # Workaround for Ray bug in reusing GPUs too quickly.
    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)


def main():
    ray.init(
        runtime_env={
            "env_vars": {
                "CUDA_VISIBLE_DEVICES": "0,1",
                # Needed for torch distributed.
                "MASTER_ADDR": "localhost",
                "MASTER_PORT": "8888",
            }
        }
    )

    timeit("exec_torch_cpu_cpu", _exec_torch_cpu_cpu)
    timeit("exec_torch_gpu", _exec_torch_gpu)
    exec_nccl_gpu()

    timeit("exec_torch_gpu_cpu_gpu", _exec_torch_gpu_cpu_gpu)
    timeit("exec_pickle_cpu", _exec_pickle_cpu)
    timeit("exec_pickle_gpu", _exec_pickle_gpu)
    timeit("exec_ray_put_cpu", _exec_ray_put_cpu)
    timeit("exec_ray_put_np", _exec_ray_put_np)
    timeit("exec_ray_put_np_copy", _exec_ray_put_np_copy)
    timeit("exec_ray_put_gpu", _exec_ray_put_gpu)

    exec_ray_core_dag_cpu()
    exec_ray_dag_cpu()
    exec_ray_dag_np()
    exec_ray_dag_gpu_cpu_gpu()
    exec_ray_core_dag_gpu()
    exec_ray_dag_gpu()
    exec_ray_dag_gpu_ipc_gpu()


if __name__ == "__main__":
    main()
