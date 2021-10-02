from dataclasses import dataclass
import logging
import os

from datetime import timedelta
from typing import Optional, Dict, Any, Tuple

import ray
from ray.util.sgd.v2.backends.backend import BackendConfig, Backend
from ray.util.sgd.v2.worker_group import WorkerGroup
from ray.util.sgd.v2.utils import get_address_and_port
from ray.util.sgd.v2.session import local_rank, world_size

try:
    import torch
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel
    from torch.utils.data import DistributedSampler, DataLoader


    def _add_dist_sampler(loader: DataLoader) -> DataLoader:
        # Automatically set the DistributedSampler
        data_loader_args = {
            "dataset": loader.dataset,
            "batch_size": loader.batch_size,
            "shuffle": False,
            "num_workers": loader.num_workers,
            "collate_fn": loader.collate_fn,
            "pin_memory": loader.pin_memory,
            "drop_last": loader.drop_last,
            "timeout": loader.timeout,
            "worker_init_fn": loader.worker_init_fn,
            "sampler": DistributedSampler(loader.dataset)
        }
        return DataLoader(**data_loader_args)


    class _WrappedDataLoader(DataLoader):
        def __init__(self, base_dataloader: DataLoader,
                     device: torch.device):
            self.base_dataloader = base_dataloader
            self.device = device

        def __iter__(self):
            for batch in 



except ImportError:
    torch = dist = DistributedDataParallel = DistributedSampler = \
        DataLoader = _Wrapped_DataLoader = None

    def noop():
        pass

    _add_dist_sampler = noop

logger = logging.getLogger(__name__)


@dataclass
class TorchConfig(BackendConfig):
    """Configuration for torch process group setup.

    See https://pytorch.org/docs/stable/distributed.html for more info.

    Args:
        backend (str): The backend to use for training.
            See ``torch.distributed.init_process_group`` for more info and
            valid values.
            If set to None, nccl will be used if GPUs are requested, else gloo
            will be used.
        init_method (str): The initialization method to use. Either "env"
            for environment variable initialization or "tcp" for TCP
            initialization. Defaults to "env".
        timeout_s (int): Seconds for process group operations to timeout.
    """
    backend: Optional[str] = None
    init_method: str = "env"
    timeout_s: int = 1800

    def __post_init__(self):
        if torch is None:
            raise ValueError("`torch` is not installed. "
                             "Please install torch to use this backend.")

    @property
    def backend_cls(self):
        return TorchBackend


def setup_torch_process_group(backend: str,
                              world_rank: int,
                              world_size: int,
                              init_method: str,
                              timeout_s: int = 1800):
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
        timeout=timedelta(seconds=timeout_s))


def shutdown_torch(destroy_process_group=False):
    if destroy_process_group:
        dist.destroy_process_group()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


class TorchBackend(Backend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        if len(worker_group) > 1 and dist.is_available():
            # Set the appropriate training backend.
            if backend_config.backend is None:
                if worker_group.num_gpus_per_worker > 0:
                    backend = "nccl"
                else:
                    backend = "gloo"
            else:
                backend = backend_config.backend

            master_addr, master_port = worker_group.execute_single(
                0, get_address_and_port)
            if backend_config.init_method == "env":

                def set_env_vars(addr, port):
                    os.environ["MASTER_ADDR"] = addr
                    os.environ["MASTER_PORT"] = str(port)

                worker_group.execute(
                    set_env_vars, addr=master_addr, port=master_port)
                url = "env://"
            elif backend_config.init_method == "tcp":
                url = f"tcp://{master_addr}:{master_port}"
            else:
                raise ValueError(
                    f"The provided init_method ("
                    f"{backend_config.init_method}) is not supported. Must "
                    f"be either 'env' or 'tcp'.")

            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        setup_torch_process_group,
                        backend=backend,
                        world_rank=i,
                        world_size=len(worker_group),
                        init_method=url,
                        timeout_s=backend_config.timeout_s))
            ray.get(setup_futures)
        else:
            logger.info("Distributed torch is not being used.")

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TorchConfig):

        worker_group.execute(
            shutdown_torch, destroy_process_group=len(worker_group) > 1)


def prepare(*args, wrap_ddp:bool=True, wrap_dist_sampler:bool=True,
            ddp_kwargs:Dict[str, Any]=None) -> Tuple:
    """Prepares the provided arguments for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or on any device type (CPU, GPU).

    ``torch.nn.Module``:
        - Place on correct device and wrap with DDP if applicable

    ``torch.utils.data.DataLoader``
        - Add ``DistributedSampler`` to ``DataLoader`` if applicable

    Example:
        TODO

    Args:
        args: Model, Data Loaders, etc. to be modified for distributed
            execution.
        wrap_ddp (bool): Whether to wrap models in ``DistributedDataParallel``.
        wrap_dist_sampler (bool): Whether to add ``DistributedSampler`` to
            provided Torch Data Loaders.
        ddp_kwargs (Dict[str, Any]): Args to pass into
            ``DistributedDataParallel`` initialization.
    """

    if torch is None:
        raise ValueError("`torch` is not installed. "
                         "Please install torch to use this function.")

    ddp_kwargs = ddp_kwargs if ddp_kwargs else {}

    if torch.cuda.is_available():
        device = torch.device("cpu")
    else:
        device = torch.device(f"cuda:{local_rank()}")

    outputs = []

    rank = local_rank()

    for obj in args:
        if isinstance(obj, torch.utils.data.DataLoader):
            pass
        elif isinstance(obj, torch.nn.Module):
            logger.info(f"Moving model to device: {device}")
            obj = obj.to(device)
            logger.info("Wrapping provided model in DDP.")
            if wrap_ddp and world_size() > 1:
                if torch.cuda.is_available():
                    obj = DistributedDataParallel(obj, device_ids=[rank],
                                                  output_device=rank,
                                                  **ddp_kwargs)
                else:
                    obj = DistributedDataParallel(obj, **ddp_kwargs)

        else:
            logger.info(f"Received object of unrecognizable type "
                        f"{type(obj)}. Performing no-op on this object.")
        outputs.append(obj)

    return tuple(outputs)





