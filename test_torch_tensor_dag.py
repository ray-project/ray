# coding: utf-8
import logging
import os
import sys
import torch
import pickle
import io

import pytest

import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode
from ray.tests.conftest import *  # noqa

from ray.experimental import TorchTensor


logger = logging.getLogger(__name__)



@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int):
        return torch.ones(shape, dtype=dtype, device=self.device) * value

    def recv(self, tensor):
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)



sender = TorchTensorWorker.options().remote()
receiver = TorchTensorWorker.options().remote()

# 10MiB tensor.
shape = (50_000_000,)
dtype = torch.float16

NUM_ITERS = 10

def timeit(label, fn):
    for i in range(NUM_ITERS):
        start = time.perf_counter()
        fn(i)
        end = time.perf_counter()
        print(label, (end - start) * 1000, "ms")

def exec_ray_dag(label, sender, receiver):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(shape, dtype, inp)
        dag = TorchTensor(dag, shape, dtype)
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile(buffer_size_bytes=1000_000_000)

    def _run(i):
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i, shape, dtype)
        output_channel.end_read()
        i += 1

    timeit(label, _run)
    compiled_dag.teardown()

def _exec_torch_cpu_cpu(i):
    t = torch.ones(shape, dtype=dtype) * i
    t2 = t.to(copy=True)

def _exec_torch_gpu_gpu(i):
    device_from = torch.device("cuda:0")
    device_to = torch.device("cuda:1")
    t = torch.ones(shape, dtype=dtype, device=device_from) * i
    t2 = t.to(device_to)
    torch.cuda.synchronize(device_to)

def _exec_torch_gpu_cpu_gpu(i):
    device_from = torch.device("cuda:0")
    device_to = torch.device("cuda:1")
    t = torch.ones(shape, dtype=dtype, device=device_from) * i
    t = t.to("cpu")
    t2 = t.to(device_to)

def _exec_pickle_cpu(i):
    t = torch.ones(shape, dtype=dtype) * i
    byte_stream = io.BytesIO()
    pickle.dump(t, byte_stream)
    byte_stream.seek(0)
    pickle.load(byte_stream)

def _exec_pickle_gpu(i):
    t = torch.ones(shape, dtype=dtype, device="cuda") * i
    byte_stream = io.BytesIO()
    pickle.dump(t, byte_stream)
    byte_stream.seek(0)
    pickle.load(byte_stream)

def _exec_ray_put_cpu(i):
    t = torch.ones(shape, dtype=dtype) * i
    ray.get(ray.put(t))

def _exec_ray_put_gpu(i):
    t = torch.ones(shape, dtype=dtype, device="cuda") * i
    ray.get(ray.put(t))

timeit("exec_torch_cpu_cpu", _exec_torch_cpu_cpu)
timeit("exec_torch_gpu_gpu", _exec_torch_gpu_gpu)
timeit("exec_torch_gpu_cpu_gpu", _exec_torch_gpu_cpu_gpu)
timeit("exec_pickle_cpu", _exec_pickle_cpu)
timeit("exec_pickle_gpu", _exec_pickle_gpu)
timeit("exec_ray_put_cpu", _exec_ray_put_cpu)
timeit("exec_ray_put_gpu", _exec_ray_put_gpu)

def exec_ray_dag_cpu():
    sender = TorchTensorWorker.options().remote()
    receiver = TorchTensorWorker.options().remote()
    exec_ray_dag("exec_ray_dag_cpu", sender, receiver)


def exec_ray_dag_gpu():
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    exec_ray_dag("exec_ray_dag_gpu", sender, receiver)


exec_ray_dag_cpu()
exec_ray_dag_gpu()
