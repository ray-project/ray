# coding: utf-8
import logging
import torch
import ray.cloudpickle as pickle
import io
import cupy
import numpy as np
import time
import os
import json
import socket

import ray
from ray.air._internal import torch_utils
import ray.cluster_utils
from ray.dag import InputNode, DAGContext
from ray.util.collective.collective_group import nccl_util

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

    def send(self, shape, dtype, _):
        t = torch.ones(shape, dtype=dtype, device=self.device) * 1
        return t

    def recv(self, tensor):
        # This benchmark tests the overhead of sending a tensor between
        # actors. To minimize the overhead of shared memory transfer,
        # we return only a byte string.
        assert tensor.device == self.device
        return b"x"


@ray.remote(num_gpus=1)
class NcclWorker:
    def __init__(self, rank):
        self.rank = rank

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    def init(self, world_size):
        from ray.air._internal import torch_utils

        self.device = torch_utils.get_devices()[0]
        self.world_size = world_size

        torch.distributed.init_process_group(
            backend="nccl",
            world_size=world_size,
            rank=self.rank,
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
                input_buffer = torch.empty(shape, dtype=dtype, device=self.device)
                self._recv(input_buffer, input_buffer.numel(), other_rank)

            torch.cuda.synchronize()

        return timeit("exec_nccl_gpu", _run)


def exec_ray_dag(
    label,
    sender,
    receiver,
    use_nccl=False,
    use_cgraph=True,
    static_shape=False,
    direct_return=False,
):
    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(SHAPE, DTYPE, inp)

        if use_cgraph:
            dag = dag.with_tensor_transport(
                transport="nccl" if use_nccl else "auto",
                _static_shape=static_shape,
                _direct_return=direct_return,
            )

        dag = receiver.recv.bind(dag)

    if use_cgraph:
        dag = dag.experimental_compile()

        def _run():
            ref = dag.execute(b"x")
            result = ray.get(ref)
            assert result == b"x"

    else:

        def _run():
            result = ray.get(dag.execute(b"x"))
            assert result == b"x"

    results = timeit(label, _run)

    if use_cgraph:
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
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
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


def exec_nccl_gpu(sender_hint, receiver_hint):
    workers = [
        NcclWorker.options(scheduling_strategy=sender_hint).remote(0),
        NcclWorker.options(scheduling_strategy=receiver_hint).remote(1),
    ]

    # node_id = ray.get(workers[0].get_node_id.remote())
    # head_node = [node for node in ray.nodes() if node["NodeID"] == node_id]
    # assert len(head_node) == 1
    # head_node = head_node[0]
    # rank_0_addr = f"{head_node['NodeManagerAddress']}:8888"

    ray.get([worker.init.remote(2) for worker in workers])

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


def exec_ray_dag_cpu(sender_hint, receiver_hint):
    sender = TorchTensorWorker.options(scheduling_strategy=sender_hint).remote()
    receiver = TorchTensorWorker.options(scheduling_strategy=receiver_hint).remote()
    return exec_ray_dag("exec_ray_dag_cpu", sender, receiver)


def exec_ray_core_cpu(sender_hint, receiver_hint):
    time.sleep(1)
    sender = TorchTensorWorker.options(scheduling_strategy=sender_hint).remote()
    receiver = TorchTensorWorker.options(scheduling_strategy=receiver_hint).remote()
    return exec_ray_dag("exec_ray_core_cpu", sender, receiver, use_cgraph=False)


def exec_ray_dag_gpu_ipc_gpu():
    time.sleep(1)
    sender = TorchIpcWorker.options(num_gpus=1).remote()
    receiver = TorchIpcWorker.options(num_gpus=1).remote()
    return exec_ray_dag_ipc("exec_ray_dag_gpu_ipc_gpu", sender, receiver)


def exec_ray_dag_gpu_cpu_gpu(sender_hint, receiver_hint):
    time.sleep(1)
    sender = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=sender_hint
    ).remote()
    receiver = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=receiver_hint
    ).remote()
    return exec_ray_dag("exec_ray_dag_gpu_cpu_gpu", sender, receiver)


def exec_ray_dag_gpu_nccl(
    sender_hint,
    receiver_hint,
    static_shape: bool = False,
    direct_return: bool = False,
):
    time.sleep(1)
    sender = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=sender_hint
    ).remote()
    receiver = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=receiver_hint
    ).remote()
    return exec_ray_dag(
        "exec_ray_dag_gpu_nccl"
        + ("_static_shape" if static_shape else "")
        + ("_direct_return" if direct_return else ""),
        sender,
        receiver,
        use_nccl=True,
        static_shape=static_shape,
        direct_return=direct_return,
    )


def exec_ray_core_gpu(sender_hint, receiver_hint):
    time.sleep(1)
    sender = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=sender_hint
    ).remote()
    receiver = TorchTensorWorker.options(
        num_gpus=1, scheduling_strategy=receiver_hint
    ).remote()
    return exec_ray_dag("exec_ray_core_gpu", sender, receiver, use_cgraph=False)


def main(distributed):
    results = []

    ray.init(
        runtime_env={
            "env_vars": {
                "CUDA_VISIBLE_DEVICES": "0,1",
                # Needed for torch distributed.
                "MASTER_ADDR": socket.gethostbyname(socket.gethostname()),
                "MASTER_PORT": "8888",
            }
        }
    )

    # NCCL takes a while to warm up on multi node so increase the default
    # timeout.
    ctx = DAGContext.get_current()
    ctx.get_timeout = 120

    sender_hint, receiver_hint = None, None
    if distributed:
        local_node_id = ray.get_runtime_context().get_node_id()
        node_ids = [node["NodeID"] for node in ray.nodes()]
        remote_node_ids = [node_id for node_id in node_ids if node_id != local_node_id]
        assert remote_node_ids
        remote_node_id = remote_node_ids[0]

        # Pin sender on local node and receiver on the other node for consistent
        # results.
        sender_hint = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            local_node_id, soft=False
        )
        receiver_hint = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            remote_node_id, soft=False
        )

    if not distributed:
        results += timeit("exec_torch_cpu_cpu", _exec_torch_cpu_cpu)
        results += timeit("exec_torch_gpu", _exec_torch_gpu)
        results += timeit("exec_torch_gpu_cpu_gpu", _exec_torch_gpu_cpu_gpu)

    results += exec_nccl_gpu(sender_hint, receiver_hint)

    if not distributed:
        results += timeit("exec_ray_put_cpu", _exec_ray_put_cpu)
        results += timeit("exec_ray_put_np_zero_copy", _exec_ray_put_np_zero_copy)
        results += timeit("exec_ray_put_gpu", _exec_ray_put_gpu)

    results += exec_ray_core_cpu(sender_hint, receiver_hint)
    results += exec_ray_dag_cpu(sender_hint, receiver_hint)
    results += exec_ray_core_gpu(sender_hint, receiver_hint)
    results += exec_ray_dag_gpu_cpu_gpu(sender_hint, receiver_hint)
    results += exec_ray_dag_gpu_nccl(
        sender_hint, receiver_hint, static_shape=True, direct_return=True
    )
    results += exec_ray_dag_gpu_nccl(
        sender_hint, receiver_hint, static_shape=False, direct_return=True
    )
    results += exec_ray_dag_gpu_nccl(
        sender_hint, receiver_hint, static_shape=True, direct_return=False
    )
    results += exec_ray_dag_gpu_nccl(
        sender_hint, receiver_hint, static_shape=False, direct_return=False
    )

    return results


def to_dict_key(key: str):
    for r in [" ", ":", "-"]:
        key = key.replace(r, "_")
    for r in ["(", ")"]:
        key = key.replace(r, "")
    return key


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tensor-size-bytes",
        type=int,
        # 100KB
        default=100_000,
    )
    parser.add_argument(
        "--distributed",
        action="store_true",
        help="Whether this is running on more than one node",
    )
    args = parser.parse_args()

    # Divide by 2 because we're using torch.float16.
    SHAPE = (args.tensor_size_bytes // 2,)

    results = main(args.distributed)

    result_dict = {
        f"{to_dict_key(v[0])}": (v[1], v[2]) for v in results if v is not None
    }

    perf_metrics = [
        {
            "perf_metric_name": to_dict_key(v[0]),
            "perf_metric_value": v[1],
            "perf_metric_type": "THROUGHPUT",
        }
        for v in results
        if v is not None
    ]
    result_dict["perf_metrics"] = perf_metrics

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/microbenchmark_gpu.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)
