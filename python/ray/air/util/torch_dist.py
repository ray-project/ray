from abc import ABC
from datetime import timedelta
import os
import torch.distributed as dist
from typing import Callable, List, T

import ray
from ray.actor import ActorHandle
from ray.air._internal.util import skip_exceptions, exception_cause
from ray.train._internal.utils import get_address_and_port
from ray.train.constants import DEFAULT_NCCL_SOCKET_IFNAME


class TorchDistributedWorker(ABC):
    """Dfines the interfaces required by the init_torch_dist_process_group().

    This is modeled after RayTrainerWorker, which allows arbitrary functions
    to be executed on a remote DDP worker.
    """

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Executes the input function and returns the output.

        Args:
            func: The function to execute.
            args, kwargs: The arguments to pass into func.
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            skipped = skip_exceptions(e)
            raise skipped from exception_cause(skipped)


def _init_torch_distributed(
    init_method: str,
    backend: str,
    rank: int,
    world_size: int,
    local_rank: int,
    local_world_size: int,
    master_addr: str,
    master_port: str,
    gpu_ids: List[int],
):
    """Initialize torch distributed backend"""
    if init_method == "env":
        os.environ["MASTER_ADDR"] = str(master_addr)
        os.environ["MASTER_PORT"] = str(master_port)
        url = "env://"
    elif init_method == "tcp":
        url = f"tcp://{master_addr}:{master_port}"
    else:
        raise ValueError(
            f"The provided init_method ("
            f"{init_method}) is not supported. Must "
            f"be either 'env' or 'tcp'."
        )

    if backend == "nccl":
        # Same as in Ray Train
        os.environ["NCCL_ASYNC_ERROR_HANDLING"] = "1"
        # All workers on a same node should share the same set of
        # visible GPUs. Otherwise they can't talk among themselves.
        os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(gpu_ids))
        if "NCCL_SOCKET_IFNAME" not in os.environ:
            os.environ["NCCL_SOCKET_IFNAME"] = DEFAULT_NCCL_SOCKET_IFNAME

    dist.init_process_group(
        backend=backend,
        init_method=url,
        rank=rank,
        world_size=world_size,
        timeout=timedelta(seconds=1800),
    )

    os.environ["RANK"] = str(rank)
    os.environ["LOCAL_RANK"] = str(local_rank)
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["LOCAL_WORLD_SIZE"] = str(local_world_size)


def _get_node_and_gpu_ids():
    """Returns the node_id and gpu_ids for this worker."""
    node_id = ray.get_runtime_context().get_node_id()
    gpu_ids = ray.get_gpu_ids()
    return node_id, gpu_ids


def init_torch_dist_process_group(
    workers: List[ActorHandle],
    backend: str = "gloo",
    init_method: str = "env",
):
    """Initialize a torch distributed process group.

    Args:
        workers: A list of TorchDistributedWorker actors.
        backend: The torch distributed backend to use,
            possible choices are "gloo" or "nccl".
        init_method: The initialization method to use,
            possible choices are "env" or "tcp".
    """
    if not dist.is_available():
        raise RuntimeError("Distributed torch is not available.")

    # Build a map from node_id to workers on that node.
    node_and_gpu_ids = ray.get(
        [w.execute.remote(_get_node_and_gpu_ids) for w in workers]
    )
    # All the workers on a specific node.
    node_to_workers = {}
    # All the gpu ids visible to all the workers on a specific node.
    node_to_gpu_ids = {}
    for i, (node_id, gpu_ids) in enumerate(node_and_gpu_ids):
        node_to_workers.setdefault(node_id, []).append(i)
        node_to_gpu_ids.setdefault(node_id, []).extend(gpu_ids)

    # Assume the first worker is the master.
    master_addr, master_port = ray.get(workers[0].execute.remote(get_address_and_port))

    setup_futures = []
    world_size = len(workers)
    for rank, worker in enumerate(workers):
        node_id = node_and_gpu_ids[rank][0]
        setup_futures.append(
            worker.execute.remote(
                _init_torch_distributed,
                init_method=init_method,
                backend=backend,
                rank=rank,
                world_size=world_size,
                local_rank=node_to_workers[node_id].index(i),
                local_world_size=len(node_to_workers[node_id]),
                master_addr=master_addr,
                master_port=master_port,
                gpu_ids=node_to_gpu_ids[node_id],
            )
        )
    ray.get(setup_futures)
