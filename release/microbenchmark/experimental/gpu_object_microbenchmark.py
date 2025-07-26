# coding: utf-8
import argparse
import time
import os
from typing import Tuple

import socket
import numpy as np
import torch
import ray
import json
from ray._private.ray_microbenchmark_helpers import timeit
from ray.experimental.collective import create_collective_group


DTYPE = torch.float16
NUM_ITERS = 5


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
    label: str,
    shape: Tuple[int],
    backend: str,
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    BACKEND_CONFIG = {
        "gloo": (GlooActor, "torch_gloo"),
        "object": (ObjectStoreActor, None),
        "nccl": (NCCLActor, "nccl"),
    }
    if backend not in BACKEND_CONFIG:
        raise ValueError(f"Unsupported backend: {backend}")
    actor_cls, group_backend = BACKEND_CONFIG[backend]
    sender = actor_cls.options(scheduling_strategy=sender_hint).remote()
    receiver = actor_cls.options(scheduling_strategy=receiver_hint).remote()
    if group_backend is not None:
        create_collective_group([sender, receiver], backend=group_backend)

    def _run():
        ref = sender.send.remote(shape, DTYPE)
        ref2 = receiver.recv.remote(ref)
        ray.get(ref2)

    results = timeit(label, _run)

    ray.kill(sender)
    ray.kill(receiver)
    time.sleep(1)

    return results


def _exec_p2p_transfer_object(
    shape: Tuple[int],
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer(
        "exec_p2p_transfer_object", shape, "object", sender_hint, receiver_hint
    )


def _exec_p2p_transfer_gloo(
    shape: Tuple[int],
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer(
        "exec_p2p_transfer_gloo", shape, "gloo", sender_hint, receiver_hint
    )


def _exec_p2p_transfer_nccl(
    shape: Tuple[int],
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer(
        "exec_p2p_transfer_nccl", shape, "nccl", sender_hint, receiver_hint
    )


def to_dict_key(key: str):
    for r in [" ", ":", "-"]:
        key = key.replace(r, "_")
    for r in ["(", ")"]:
        key = key.replace(r, "")
    return key


def main() -> None:
    p = argparse.ArgumentParser(description="GPU tensor transfer benchmark")
    p.add_argument(
        "--tensor-size-bytes",
        type=int,
        default=1_000_000,
    )
    p.add_argument(
        "--distributed",
        action="store_true",
        help="Whether this is running on more than one node",
    )

    args = p.parse_args()
    ray.init(
        logging_level="ERROR",
        runtime_env={
            "env_vars": {
                # "NCCL_DEBUG": "INFO",
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

    size = args.tensor_size_bytes
    shape = (size // 2,)
    results = []
    results += _exec_p2p_transfer_object(shape, sender_hint, receiver_hint)
    results += _exec_p2p_transfer_gloo(shape, sender_hint, receiver_hint)
    results += _exec_p2p_transfer_nccl(shape, sender_hint, receiver_hint)
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
        "TEST_OUTPUT_JSON", "/tmp/microbenchmark_gpu_object.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)


if __name__ == "__main__":
    main()
