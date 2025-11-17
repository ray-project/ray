import argparse
import json
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import torch

import ray
from ray._private.ray_microbenchmark_helpers import timeit
from ray._private.test_utils import (
    kill_actor_and_wait_for_failure,
)
from ray.experimental.collective import create_collective_group

DTYPE = torch.float16
SHAPE = [(1,), (1_000,), (1_000_000,), (100_000_000,)]


@dataclass
class BackendConfig:
    init_actor_kwargs: dict
    send_method_kwargs: dict
    device: torch.device
    collective_group_backend: Optional[str]


BACKEND_CONFIG = {
    "gloo": BackendConfig(
        init_actor_kwargs={},
        send_method_kwargs={"tensor_transport": "gloo"},
        device=torch.device("cpu"),
        collective_group_backend="torch_gloo",
    ),
    "object": BackendConfig(
        init_actor_kwargs={},
        send_method_kwargs={},
        device=torch.device("cpu"),
        collective_group_backend=None,
    ),
    "nccl": BackendConfig(
        init_actor_kwargs={
            "num_gpus": 1,
            "num_cpus": 0,
            "enable_tensor_transport": True,
        },
        send_method_kwargs={"tensor_transport": "nccl"},
        device=torch.device("cuda"),
        collective_group_backend="nccl",
    ),
}


@ray.remote(enable_tensor_transport=True)
class Actor:
    def __init__(
        self,
        shape: Tuple[int],
        dtype: torch.dtype,
        device: torch.device,
    ) -> None:
        self.device = device
        self.dtype = dtype
        self.shape = shape

    def send(self) -> torch.Tensor:
        seed = int(np.random.randint(100))
        return torch.ones(self.shape, dtype=self.dtype, device=self.device) * seed

    def recv(self, tensor: torch.Tensor):
        assert tensor.device.type == self.device.type
        # Return the first element of the tensor to make sure the actor has received the tensor.
        return tensor[0].item()


def _exec_p2p_transfer(
    label: str,
    shape: Tuple[int],
    backend: str,
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    if backend not in BACKEND_CONFIG:
        raise ValueError(f"Unsupported backend: {backend}")
    backend_config = BACKEND_CONFIG[backend]
    device = backend_config.device
    init_actor_kwargs = backend_config.init_actor_kwargs
    send_method_kwargs = backend_config.send_method_kwargs
    collective_group_backend = backend_config.collective_group_backend
    sender = Actor.options(scheduling_strategy=sender_hint, **init_actor_kwargs).remote(
        shape, DTYPE, device
    )
    receiver = Actor.options(
        scheduling_strategy=receiver_hint, **init_actor_kwargs
    ).remote(shape, DTYPE, device)
    if collective_group_backend is not None:
        create_collective_group([sender, receiver], backend=collective_group_backend)

    def _run():
        ref = sender.send.options(**send_method_kwargs).remote()
        ref2 = receiver.recv.remote(ref)
        ray.get(ref2)

    results = timeit(label, _run)

    kill_actor_and_wait_for_failure(sender)
    kill_actor_and_wait_for_failure(receiver)

    return results


def _exec_p2p_transfer_multiple_shapes(
    label: str,
    backend: str,
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    temp_results = []
    for shape in SHAPE:
        temp_results += _exec_p2p_transfer(
            f"{label}_shape_{shape}", shape, backend, sender_hint, receiver_hint
        )
    return temp_results


def _exec_p2p_transfer_object(
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer_multiple_shapes(
        "exec_p2p_transfer_object", "object", sender_hint, receiver_hint
    )


def _exec_p2p_transfer_gloo(
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer_multiple_shapes(
        "exec_p2p_transfer_gloo", "gloo", sender_hint, receiver_hint
    )


def _exec_p2p_transfer_nccl(
    sender_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
    receiver_hint: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy,
):
    return _exec_p2p_transfer_multiple_shapes(
        "exec_p2p_transfer_nccl", "nccl", sender_hint, receiver_hint
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
        "--distributed",
        action="store_true",
        help="Whether this is running on more than one node",
    )

    args = p.parse_args()
    ray.init(logging_level="ERROR")

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

    results = []
    results.extend(_exec_p2p_transfer_object(sender_hint, receiver_hint))
    results.extend(_exec_p2p_transfer_gloo(sender_hint, receiver_hint))
    results.extend(_exec_p2p_transfer_nccl(sender_hint, receiver_hint))
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
