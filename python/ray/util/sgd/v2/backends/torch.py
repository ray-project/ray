from dataclasses import dataclass
import logging
import os

from contextlib import closing
from datetime import timedelta
import socket
from typing import Optional

import ray
from ray.util.sgd.v2.backends.backend import BackendConfig

from python.ray.util.sgd.v2.backends.backend import BackendInterface
from python.ray.util.sgd.v2.worker_group import WorkerGroup

try:
    import torch
    import torch.distributed as dist
except ImportError:
    torch = None
    dist = None

logger = logging.getLogger(__name__)


@dataclass
class TorchConfig(BackendConfig):
    """Configuration for torch process group setup.

    Args:
        backend (str): The backend (nccl, gloo, etc.) to use for training.
            If set to None, nccl will be used for GPU training, else gloo
            will be used.
        init_method (str): The initialization method to use. Either "env"
            for environment variable initialization or "tcp" for TCP
            initialization. Defaults to "env".
        timeout_s (timedelta): Seconds for process group operations to timeout.
    """
    backend: Optional[str] = None
    init_method: str = "env"
    timeout_s: timedelta = timedelta(0, 1800)

    def __post_init__(self):
        if torch is None:
            raise ValueError("`torch` is not installed. "
                             "Please install torch to use this backend.")

    def backend_name(self):
        return "torch"

    def get_backend_cls(self):
        return TorchBackend


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def setup_torch_process_group(backend: str,
                              world_rank: int,
                              world_size: int,
                              init_method: str = None,
                              timeout_s: timedelta = timedelta(0, 1800)):
    """Connects the distributed PyTorch backend.

    Args:
        backend (str): The backend (nccl, gloo, etc.) to use for training.
        world_rank (int): Rank of the current worker.
        world_size (int): Number of workers participating in the job.
        init_method (str): URL specifying how to initialize the process group.
        timeout_s (timedelta): Seconds for process group operations to timeout.
    """
    logger.info(
        f"Setting up process group for: {init_method} [rank={world_rank}, "
        f"world_size={world_size}]")
    logger.debug(f"using {backend}")

    if backend == "nccl" and "NCCL_BLOCKING_WAIT" not in os.environ:
        logger.debug(
            "Setting NCCL_BLOCKING_WAIT for detecting node failure. "
            "To override this behavior, you can set NCCL_BLOCKING_WAIT=0.")
        os.environ["NCCL_BLOCKING_WAIT"] = "1"

    dist.init_process_group(
        backend=backend,
        init_method=init_method,
        rank=world_rank,
        world_size=world_size,
        timeout=timeout_s)


def get_address():
    addr = ray.util.get_node_ip_address()
    port = find_free_port()
    return addr, port


def shutdown_torch():
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


class TorchBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        if len(worker_group) > 1:
            master_addr, master_port = worker_group.execute_single(
                0, get_address)
            if backend_config.init_method == "env":

                def set_env_vars(addr, port):
                    os.environ["MASTER_ADDR"] = addr
                    os.environ["MASTER_PORT"] = port

                worker_group.execute(
                    set_env_vars, addr=master_addr, port=master_port)
                url = "env://"
            elif backend_config.init_method == "tcp":
                url = f"tcp://{master_addr}:{master_port}"
            else:
                raise ValueError(
                    f"The provided init_method ("
                    f"{backend_config.init_method} is not supported.")

            for i in range(len(worker_group)):
                worker_group.execute_single(
                    i,
                    setup_torch_process_group,
                    backend=backend_config.backend,
                    world_rank=i,
                    world_size=len(worker_group),
                    init_method=url,
                    timeout_s=backend_config.timeout_s)

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TorchConfig):
        worker_group.exexute_single(0, dist.destroy_process_group)
        worker_group.execute(shutdown_torch)
