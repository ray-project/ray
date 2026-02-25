import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

import torch
import torch.distributed as dist
from packaging.version import Version

import ray
from ray._common.network_utils import build_address
from ray._private import ray_constants
from ray.air._internal.device_manager import register_custom_torch_dist_backend
from ray.exceptions import GetTimeoutError
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train._internal.utils import get_address_and_port
from ray.train.backend import Backend, BackendConfig
from ray.train.constants import (
    DEFAULT_TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
    TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
)
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


class TorchConfigContextManager:
    def __enter__(self):
        # Set default cuda device
        if torch.cuda.is_available():
            device = ray.train.torch.get_device()
            if device.type == "cuda":
                torch.cuda.set_device(device)

    def __exit__(self, type, value, traceback):
        # Propagate exceptions if any
        return False


@PublicAPI(stability="stable")
@dataclass
class TorchConfig(BackendConfig):
    """Configuration for torch process group setup.

    See https://pytorch.org/docs/stable/distributed.html for more info.

    Args:
        backend: The backend to use for training.
            See ``torch.distributed.init_process_group`` for more info and
            valid values.
            If set to None, nccl will be used if GPUs are requested, else gloo
            will be used.
        init_method: The initialization method to use. Either "env"
            for environment variable initialization or "tcp" for TCP
            initialization. Defaults to "env".
        timeout_s: Seconds for process group operations to timeout.
    """

    backend: Optional[str] = None
    init_method: str = "env"
    timeout_s: int = 1800

    @property
    def backend_cls(self):
        return _TorchBackend

    @property
    def train_func_context(self):
        return TorchConfigContextManager


@dataclass
class TorchftConfig(TorchConfig):
    """Configuration for torchft-based fault-tolerant training with replica groups.

    Each replica group has its own independent TCPStore and process group,
    matching the torchrun model where each torchrun instance is an independent
    replica group.

    Args:
        dp_workers: Number of data-parallel workers per replica group.
            Total workers = num_replica_groups * dp_workers.
        min_replicas: Minimum number of replica groups required for torchft
            lighthouse quorum.
    """

    dp_workers: int = 1
    min_replicas: int = 1

    @property
    def backend_cls(self):
        return _TorchftBackend


def _setup_torch_process_group(
    backend: str,
    world_rank: int,
    world_size: int,
    init_method: str,
    timeout_s: int = 1800,
):
    """Connects the distributed PyTorch backend.

    Args:
        backend: The backend (nccl, gloo, etc.) to use for training.
        world_rank: Rank of the current worker.
        world_size: Number of workers participating in the job.
        init_method: URL specifying how to initialize the process group.
        timeout_s: Seconds for process group operations to timeout.
    """
    if world_rank == 0:
        logger.info(
            f"Setting up process group for: {init_method} [rank={world_rank}, "
            f"world_size={world_size}]"
        )
    else:
        logger.debug(
            f"Setting up process group for: {init_method} [rank={world_rank}, "
            f"world_size={world_size}]"
        )
    logger.debug(f"using {backend}")

    if backend == "nccl":
        # See https://github.com/pytorch/pytorch/blob/c263bd43e8e8502d4726643bc6fd046f0130ac0e/torch/distributed/distributed_c10d.py#L803-L823 # noqa: E501
        # We do not use TORCH_NCCL_BLOCKING_WAIT due to performance overhead.
        if Version(torch.__version__) < Version("2.2.0"):
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR = "NCCL_ASYNC_ERROR_HANDLING"
            TORCH_NCCL_BLOCKING_WAIT_ENV_VAR = "NCCL_BLOCKING_WAIT"
        else:
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR = "TORCH_NCCL_ASYNC_ERROR_HANDLING"
            TORCH_NCCL_BLOCKING_WAIT_ENV_VAR = "TORCH_NCCL_BLOCKING_WAIT"
        if (
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR not in os.environ
            and TORCH_NCCL_BLOCKING_WAIT_ENV_VAR not in os.environ
        ):
            logger.debug(
                f"Setting {TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR}=1 to fail if NCCL collective communication operations are timing out. "  # noqa: E501
                f"To override this behavior, you can set {TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR}=0."  # noqa: E501
            )
            os.environ[TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR] = "1"
    elif backend == "hccl":
        register_custom_torch_dist_backend(backend)

    dist.init_process_group(
        backend=backend,
        init_method=init_method,
        rank=world_rank,
        world_size=world_size,
        timeout=timedelta(seconds=timeout_s),
    )


def _shutdown_torch(destroy_process_group=False):
    from ray.air._internal.torch_utils import get_devices

    devices = get_devices()
    # Check dist.is_initialized() because torchft might already destroy process group.
    if destroy_process_group and dist.is_initialized():
        dist.destroy_process_group()
    if torch.cuda.is_available():
        for device in devices:
            with torch.cuda.device(device):
                torch.cuda.empty_cache()


def _set_torch_distributed_env_vars():
    # Same env vars as in
    # https://pytorch.org/docs/stable/elastic/run.html#environment-variables
    from ray.train.torch import get_device

    context = ray.train.get_context()
    os.environ["LOCAL_RANK"] = str(context.get_local_rank())
    os.environ["RANK"] = str(context.get_world_rank())
    os.environ["LOCAL_WORLD_SIZE"] = str(context.get_local_world_size())
    os.environ["WORLD_SIZE"] = str(context.get_world_size())
    os.environ["NODE_RANK"] = str(context.get_node_rank())

    # Makes sure Hugging Face Accelerate uses the correct device
    device = get_device()
    os.environ["ACCELERATE_TORCH_DEVICE"] = str(device)


class _TorchBackend(Backend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: BaseWorkerGroup, backend_config: TorchConfig):
        if dist.is_available():
            # Set the appropriate training backend.
            if backend_config.backend is None:
                resources = worker_group.get_resources_per_worker()
                num_gpus_per_worker = resources.get("GPU", 0)

                if num_gpus_per_worker > 0:
                    backend = "nccl"
                else:
                    backend = "gloo"
            else:
                backend = backend_config.backend

            master_addr, master_port = worker_group.execute_single(
                0, get_address_and_port
            )
            if backend_config.init_method == "env":

                def set_env_vars(addr, port):
                    os.environ["MASTER_ADDR"] = addr
                    os.environ["MASTER_PORT"] = str(port)

                worker_group.execute(set_env_vars, addr=master_addr, port=master_port)
                url = "env://"
            elif backend_config.init_method == "tcp":
                url = f"tcp://{build_address(master_addr, master_port)}"
            else:
                raise ValueError(
                    f"The provided init_method ("
                    f"{backend_config.init_method}) is not supported. Must "
                    f"be either 'env' or 'tcp'."
                )

            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        _setup_torch_process_group,
                        backend=backend,
                        world_rank=i,
                        world_size=len(worker_group),
                        init_method=url,
                        timeout_s=backend_config.timeout_s,
                    )
                )
            ray.get(setup_futures)
        else:
            raise RuntimeError("Distributed torch is not available.")

    def on_shutdown(self, worker_group: BaseWorkerGroup, backend_config):
        futures = worker_group.execute_async(
            _shutdown_torch,
            destroy_process_group=len(worker_group) > 1,
        )
        timeout_s = ray_constants.env_integer(
            TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
            DEFAULT_TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
        )
        try:
            ray.get(futures, timeout=timeout_s)
        except GetTimeoutError:
            logger.warning(
                f"Torch process group shutdown timed out after {timeout_s} seconds"
            )

    def on_training_start(
        self, worker_group: BaseWorkerGroup, backend_config: BackendConfig
    ):
        worker_group.execute(_set_torch_distributed_env_vars)


def _set_torchft_distributed_env_vars(replica_group_rank, dp_workers):
    """Set torch distributed env vars for torchft replica group workers.

    Unlike the global _set_torch_distributed_env_vars, this sets RANK and
    WORLD_SIZE to the replica-group-local values so that torchft Manager
    uses the correct group-local rank.
    """
    from ray.train.torch import get_device

    context = ray.train.get_context()
    os.environ["LOCAL_RANK"] = str(context.get_local_rank())
    os.environ["RANK"] = str(context.get_world_rank())
    os.environ["LOCAL_WORLD_SIZE"] = str(context.get_local_world_size())
    os.environ["WORLD_SIZE"] = str(context.get_world_size())
    os.environ["NODE_RANK"] = str(context.get_node_rank())

    device = get_device()
    os.environ["ACCELERATE_TORCH_DEVICE"] = str(device)


class _TorchftBackend(Backend):
    """Backend for torchft-based fault-tolerant training with replica groups.

    Creates a separate TCPStore and process group per replica group,
    matching the torchrun model.
    """

    share_cuda_visible_devices: bool = True

    def on_start(
        self,
        worker_group: BaseWorkerGroup,
        backend_config: TorchftConfig,
        workers_subset: Optional[List[int]] = None,
    ):
        if not dist.is_available():
            raise RuntimeError("Distributed torch is not available.")

        # Determine backend (nccl/gloo)
        if backend_config.backend is None:
            resources = worker_group.get_resources_per_worker()
            num_gpus_per_worker = resources.get("GPU", 0)
            backend = "nccl" if num_gpus_per_worker > 0 else "gloo"
        else:
            backend = backend_config.backend

        dp_workers = backend_config.dp_workers
        num_workers = len(worker_group)
        num_groups = num_workers // dp_workers

        if workers_subset is not None:
            # Determine which groups need initialization based on the subset
            group_ids = set()
            for rank in workers_subset:
                group_ids.add(rank // dp_workers)
        else:
            group_ids = set(range(num_groups))

        for group_id in group_ids:
            self._init_replica_group(
                worker_group, backend_config, backend, group_id, dp_workers
            )

    def _init_replica_group(
        self,
        worker_group: BaseWorkerGroup,
        backend_config: TorchftConfig,
        backend: str,
        group_id: int,
        dp_workers: int,
    ):
        """Initialize a single replica group's TCPStore and process group."""
        group_ranks = list(range(group_id * dp_workers, (group_id + 1) * dp_workers))

        # Get master addr/port from group's rank 0
        master_addr, master_port = worker_group.execute_single(
            group_ranks[0], get_address_and_port
        )

        if backend_config.init_method == "env":

            def set_env_vars(addr, port):
                os.environ["MASTER_ADDR"] = addr
                os.environ["MASTER_PORT"] = str(port)

            # Set MASTER_ADDR/MASTER_PORT on all workers in this group
            for rank in group_ranks:
                worker_group.execute_single(
                    rank, set_env_vars, addr=master_addr, port=master_port
                )
            url = "env://"
        elif backend_config.init_method == "tcp":
            url = f"tcp://{build_address(master_addr, master_port)}"
        else:
            raise ValueError(
                f"The provided init_method ("
                f"{backend_config.init_method}) is not supported. Must "
                f"be either 'env' or 'tcp'."
            )

        # Call init_process_group on each worker in the group with
        # group-local ranks (0..dp_workers-1)
        setup_futures = []
        for i, global_rank in enumerate(group_ranks):
            setup_futures.append(
                worker_group.execute_single_async(
                    global_rank,
                    _setup_torch_process_group,
                    backend=backend,
                    world_rank=i,  # group-local rank
                    world_size=dp_workers,
                    init_method=url,
                    timeout_s=backend_config.timeout_s,
                )
            )
        ray.get(setup_futures)

        logger.info(
            f"Initialized replica group {group_id} with {dp_workers} workers "
            f"(global ranks {group_ranks}), master={master_addr}:{master_port}"
        )

    def on_training_start(
        self,
        worker_group: BaseWorkerGroup,
        backend_config: TorchftConfig,
        workers_subset: Optional[List[int]] = None,
    ):
        dp_workers = backend_config.dp_workers

        if workers_subset is not None:
            ranks_to_init = workers_subset
        else:
            ranks_to_init = list(range(len(worker_group)))

        for global_rank in ranks_to_init:
            replica_group_rank = global_rank % dp_workers
            worker_group.execute_single(
                global_rank,
                _set_torchft_distributed_env_vars,
                replica_group_rank=replica_group_rank,
                dp_workers=dp_workers,
            )

    def on_shutdown(self, worker_group: BaseWorkerGroup, backend_config):
        futures = worker_group.execute_async(
            _shutdown_torch,
            destroy_process_group=len(worker_group) > 1,
        )
        timeout_s = ray_constants.env_integer(
            TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
            DEFAULT_TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S,
        )
        try:
            ray.get(futures, timeout=timeout_s)
        except GetTimeoutError:
            logger.warning(
                f"Torch process group shutdown timed out after {timeout_s} seconds"
            )
