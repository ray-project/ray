# coding: utf-8
import logging
import torch
import ray.cloudpickle as pickle
import io
import cupy
import numpy as np
import time

import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode
from ray.util.collective.collective_group import nccl_util

from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray._private.ray_microbenchmark_helpers import timeit


logger = logging.getLogger(__name__)


SHAPE = None
DTYPE = torch.float16

NUM_ITERS = 10


@ray.remote
class TorchIpcWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int):
        t = torch.ones(shape, dtype=dtype, device=self.device) * value
        if self.device.type == "cuda":
            # NOTE(swang): This is needed because the IPC can get sent before
            # the value has been written to memory. But somehow the read value
            # is still the wrong one?
            torch.cuda.synchronize()
        h = cupy.cuda.runtime.ipcGetMemHandle(t.data_ptr())
        return h

    def recv(self, device_ptr, num_bytes, shape, dtype):
        h = cupy.cuda.runtime.ipcOpenMemHandle(device_ptr)
        m = cupy.cuda.UnownedMemory(h, num_bytes, None)
        m_ptr = cupy.cuda.MemoryPointer(m, 0)
        tensor = torch.tensor(cupy.ndarray(shape, dtype, m_ptr), device=self.device)
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)


@ray.remote
class TorchTensorWorker:
    def __init__(self):
        self.device = torch_utils.get_devices()[0]

    def send(self, shape, dtype, value: int):
        t = torch.ones(shape, dtype=dtype, device=self.device) * value
        return t

    def recv(self, tensor):
        assert tensor.device == self.device
        return (tensor[0].item(), tensor.shape, tensor.dtype)


@ray.remote(num_gpus=1)
class NcclWorker:
    def __init__(self, world_size, rank, comm_id):
        from ray.air._internal import torch_utils

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

        return timeit("exec_nccl_gpu", _run)


def exec_ray_dag(
    label, sender, receiver, use_nccl=False, use_adag=True, dynamic_shape=False
):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(SHAPE, DTYPE, inp)

        if use_adag:
            dag = dag.with_type_hint(
                TorchTensorType(
                    "auto" if dynamic_shape else SHAPE,
                    "auto" if dynamic_shape else DTYPE,
                    transport="nccl" if use_nccl else "auto",
                )
            )

        dag = receiver.recv.bind(dag)

    if use_adag:
        dag = dag.experimental_compile(_buffer_size_bytes=int(SHAPE[0] * 3))

        def _run():
            i = np.random.randint(100)
            ref = dag.execute(i)
            # TODO(swang): Replace with fake ObjectRef.
            result = ray.get(ref)
            assert result == (i, SHAPE, DTYPE)

    else:

        def _run():
            i = np.random.randint(100)
            result = ray.get(dag.execute(i))
            assert result == (i, SHAPE, DTYPE)

    results = timeit(label, _run)

    if use_adag:
        dag.teardown()

    # Workaround for Ray bug in reusing GPUs too quickly.
    # See https://github.com/ray-project/ray/issues/44821.
    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)

    return results


def exec_ray_dag_ipc(label, sender, receiver, use_nccl=False):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(SHAPE, DTYPE, inp)
        dag = receiver.recv.bind(
            dag,
            # torch.float16 has item size of 2 bytes.
            SHAPE[0] * 2,
            SHAPE,
            nccl_util.TORCH_NUMPY_DTYPE_MAP[DTYPE],
        )

    compiled_dag = dag.experimental_compile(_buffer_size_bytes=int(SHAPE[0] * 3))
    # Flag that each run can set if it sees incorrect results.
    ok = [True]

    def _run():
        i = np.random.randint(100)
        output_channel = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.read()
        if result != (i, SHAPE, DTYPE):
            ok[0] = False

    results = timeit(label, _run)

    if not ok[0]:
        logger.warning("IPC DAG returned incorrect result")
    compiled_dag.teardown()

    return results


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
    done_refs, _ = ray.wait(tasks, num_returns=1)

    results = ray.get(done_refs[0])

    # Workaround for Ray bug in reusing GPUs too quickly.
    # See https://github.com/ray-project/ray/issues/44821.
    for worker in workers:
        ray.kill(worker)
    time.sleep(1)

    return results


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


def _exec_ray_put_np_zero_copy():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE) * i
    torch.as_tensor(ray.get(ray.put(t.numpy())))


def _exec_ray_put_gpu():
    i = np.random.randint(100)
    t = torch.ones(SHAPE, dtype=DTYPE, device="cuda") * i
    ray.get(ray.put(t))


def exec_ray_dag_cpu():
    sender = TorchTensorWorker.options().remote()
    receiver = TorchTensorWorker.options().remote()
    return exec_ray_dag("exec_ray_dag_cpu", sender, receiver)


def exec_ray_core_cpu():
    time.sleep(1)
    sender = TorchTensorWorker.remote()
    receiver = TorchTensorWorker.remote()
    return exec_ray_dag("exec_ray_core_cpu", sender, receiver, use_adag=False)


def exec_ray_dag_gpu_ipc_gpu():
    time.sleep(1)
    sender = TorchIpcWorker.options(num_gpus=1).remote()
    receiver = TorchIpcWorker.options(num_gpus=1).remote()
    return exec_ray_dag_ipc("exec_ray_dag_gpu_ipc_gpu", sender, receiver)


def exec_ray_dag_gpu_cpu_gpu():
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    return exec_ray_dag("exec_ray_dag_gpu_cpu_gpu", sender, receiver)


def exec_ray_dag_gpu_nccl(dynamic_shape: bool = False):
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    return exec_ray_dag(
        "exec_ray_dag_gpu_nccl" + ("_dynamic" if dynamic_shape else ""),
        sender,
        receiver,
        use_nccl=True,
        dynamic_shape=dynamic_shape,
    )


def exec_ray_core_gpu():
    time.sleep(1)
    sender = TorchTensorWorker.options(num_gpus=1).remote()
    receiver = TorchTensorWorker.options(num_gpus=1).remote()
    return exec_ray_dag("exec_ray_core_gpu", sender, receiver, use_adag=False)


def main():
    results = []

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

    results += timeit("exec_torch_cpu_cpu", _exec_torch_cpu_cpu)
    results += timeit("exec_torch_gpu", _exec_torch_gpu)
    results += timeit("exec_torch_gpu_cpu_gpu", _exec_torch_gpu_cpu_gpu)
    results += exec_nccl_gpu()

    results += timeit("exec_ray_put_cpu", _exec_ray_put_cpu)
    results += timeit("exec_ray_put_np_zero_copy", _exec_ray_put_np_zero_copy)
    results += timeit("exec_ray_put_gpu", _exec_ray_put_gpu)

    results += exec_ray_core_cpu()
    results += exec_ray_dag_cpu()
    results += exec_ray_core_gpu()
    results += exec_ray_dag_gpu_cpu_gpu()
    results += exec_ray_dag_gpu_nccl(dynamic_shape=True)
    results += exec_ray_dag_gpu_nccl(dynamic_shape=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tensor-size-bytes",
        type=int,
        # 100KB
        default=100_000,
    )
    args = parser.parse_args()

    # Divide by 2 because we're using torch.float16.
    SHAPE = (args.tensor_size_bytes // 2,)

    main()
