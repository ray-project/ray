# coding: utf-8
import argparse
import time

from typing import List, Tuple, Type

import socket
import numpy as np
import torch
import ray
from ray.experimental.collective import create_collective_group


DTYPE = torch.float16
DEFAULT_SIZES: List[int] = [
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1 * 1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
]
BACKENDS = ["gloo", "object", "nccl"]


@ray.remote
class GlooActor:
    def __init__(self) -> None:
        self.device = torch.device("cpu")

    @ray.method(tensor_transport="gloo")
    def send(self, shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor:
        seed = int(np.random.randint(100))
        return torch.ones(shape, dtype=dtype, device=self.device) * seed

    def recv(self, tensor: torch.Tensor) -> torch.Tensor:
        return tensor


@ray.remote
class ObjectStoreActor:
    def __init__(self) -> None:
        self.device = torch.device("cpu")

    def send(self, shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor:
        seed = int(np.random.randint(100))
        return torch.ones(shape, dtype=dtype, device=self.device) * seed

    def recv(self, tensor: torch.Tensor) -> torch.Tensor:
        return tensor


@ray.remote(num_gpus=1, num_cpus=0)
class NCCLActor:
    def __init__(self) -> None:
        self.device = torch.device("cuda")

    @ray.method(tensor_transport="nccl")
    def send(self, shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor:
        seed = int(np.random.randint(100))
        return torch.ones(shape, dtype=dtype, device=self.device) * seed

    def recv(self, tensor: torch.Tensor) -> torch.Tensor:
        return tensor


def _exec_p2p_transfer(
    shape: Tuple[int],
    iters: int,
    backend: str,
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
) -> float:
    actor_cls: Type
    group_backend: str
    if backend == "gloo":
        actor_cls = GlooActor
        group_backend = "torch_gloo"
    elif backend == "object":
        actor_cls = ObjectStoreActor
        group_backend = None
    elif backend == "nccl":
        actor_cls = NCCLActor
        group_backend = "nccl"
    else:
        raise ValueError(f"Unsupported backend: {backend}")
    sender = actor_cls.options(scheduling_strategy=sender_hint).remote()
    receiver = actor_cls.options(scheduling_strategy=receiver_hint).remote()
    if group_backend is not None:
        create_collective_group([sender, receiver], backend=group_backend)

    ray.get(receiver.recv.remote(sender.send.remote(shape, DTYPE)))  # warm‑up

    start = time.perf_counter()
    for _ in range(iters):
        ref = sender.send.remote(shape, DTYPE)
        ref2 = receiver.recv.remote(ref)
        ray.get(ref2)
    elapsed = time.perf_counter() - start

    avg_us = (elapsed / iters) * 1e6

    ray.kill(sender)
    ray.kill(receiver)

    return avg_us


def main() -> None:
    p = argparse.ArgumentParser(description="GPU tensor transfer benchmark")
    p.add_argument("--size", type=int, default=None, help="Tensor size in bytes")
    p.add_argument("--iters", type=int, default=10, help="Iterations per run")
    p.add_argument(
        "--distributed",
        action="store_true",
        help="Whether this is running on more than one node",
    )
    p.add_argument(
        "--backend",
        choices=["gloo", "object", "nccl"],
        default="gloo",
        help="Transport backend to benchmark",
    )
    args = p.parse_args()
    ray.init(
        logging_level="ERROR",
        runtime_env={
            "env_vars": {
                "NCCL_DEBUG": "INFO",
                # "UCX_TLS": "^gdr_copy",
                "RAY_worker_register_timeout_seconds": "120",
                # Needed for torch distributed.
                "MASTER_ADDR": socket.gethostbyname(socket.gethostname()),
                "MASTER_PORT": "8888",
            },
        },
    )

    distributed = args.distributed
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

    # Single‑run path
    size = args.size or DEFAULT_SIZES[-1]  # default to largest size
    backend = args.backend
    shape = (size // 2,)
    lat = _exec_p2p_transfer(shape, args.iters, backend, sender_hint, receiver_hint)

    print(
        f"Backend: {backend} | Tensor size: {size // 1024} KB | "
        f"Iterations: {args.iters} | Avg latency: {lat:.2f} µs",
    )


if __name__ == "__main__":
    main()
