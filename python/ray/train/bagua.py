from dataclasses import dataclass
import logging
import os

from typing import Optional

import ray
from ray.train.torch import (
    TorchAccelerator,
    TorchBackend,
)

from ray.train.backend import BackendConfig
from ray.train.session import get_accelerator, set_accelerator
from ray.train.worker_group import WorkerGroup
from ray.train.utils import get_address_and_port
from ray.util import PublicAPI

import torch
import torch.distributed as dist
from torch.utils.data import DistributedSampler

import bagua.torch_api
from bagua.torch_api.algorithms import gradient_allreduce

try:
    from torch.profiler import profile
except ImportError:
    profile = None

logger = logging.getLogger(__name__)


def _get_torch_distributed_sampler(dataset, shuffle):
    return DistributedSampler(
        dataset,
        num_replicas=bagua.torch_api.get_world_size(),  # equivalent to ray.train.size()
        rank=bagua.torch_api.get_rank(),  # equivalent to ray.train.world_rank()
        shuffle=shuffle,
    )


class BaguaAccelerator(TorchAccelerator):
    def __init__(self, amp: bool = False):
        super().__init__(amp=amp)

    def prepare_model(
        self,
        model: torch.nn.Module,
        optimizer: torch.optim.Optimizer,
        algorithm: bagua.torch_api.algorithms.Algorithm,
    ) -> torch.nn.Module:

        device = self.get_device()
        model = model.to(device)

        if torch.cuda.is_available():
            torch.cuda.set_device(device)

        if self.amp_is_enabled:
            # move wrap_forward() and model_get_state() to a new function
            model = self._patch_model_forward_and_state(model)

        logger.info("Wrapping provided model in BaguaDDP.")
        # we need the optimizer when preparing the model
        # with_bagua() returns a BaguaModule not torch.nn.Module
        model = model.with_bagua([optimizer], algorithm)

        return model


@PublicAPI(stability="beta")
@dataclass
class BaguaConfig(BackendConfig):

    store: Optional[torch.distributed.Store] = None

    @property
    def backend_cls(self):
        return BaguaBackend


def setup_torch_process_group(store: Optional[torch.distributed.Store] = None):
    torch.cuda.set_device(bagua.torch_api.get_local_rank())
    # init_process_group() is different from ray.train.torch approach
    # https://github.com/BaguaSys/bagua/blob/master/bagua/torch_api/communication.py#L524-L529
    # 1. backend is nccl in Bagua
    # 2. init_method is the default value provided in torch
    # 3. users can only set the store argument
    bagua.torch_api.init_process_group(store)


class BaguaBackend(TorchBackend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: WorkerGroup, backend_config: BaguaConfig):

        if dist.is_available():

            store = backend_config.store

            master_addr, master_port = worker_group.execute_single(
                0, get_address_and_port
            )

            def set_env_vars(addr, port):
                os.environ["MASTER_ADDR"] = addr
                os.environ["MASTER_PORT"] = str(port)

            # Bagua uses ``env://`` as the init method.
            worker_group.execute(set_env_vars, addr=master_addr, port=master_port)

            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        setup_torch_process_group,
                        store=store,
                    )
                )
            ray.get(setup_futures)
        else:
            raise RuntimeError("Distributed torch is not available.")


@PublicAPI(stability="beta")
def get_device() -> torch.device:
    return get_accelerator(BaguaAccelerator).get_device()


@PublicAPI(stability="beta")
def prepare_model(
    model: torch.nn.Module,
    optimizer: torch.optim.Optimizer,
    algorithm: bagua.torch_api.algorithms.Algorithm = None,
) -> torch.nn.Module:
    algorithm = (
        gradient_allreduce.GradientAllReduceAlgorithm()
        if algorithm is None
        else algorithm
    )
    return get_accelerator(BaguaAccelerator).prepare_model(model, optimizer, algorithm)


@PublicAPI(stability="beta")
def prepare_data_loader(
    data_loader: torch.utils.data.DataLoader,
    add_dist_sampler: bool = True,
    move_to_device: bool = True,
    auto_transfer: bool = True,
) -> torch.utils.data.DataLoader:
    return get_accelerator(BaguaAccelerator).prepare_data_loader(
        data_loader,
        add_dist_sampler=add_dist_sampler,
        move_to_device=move_to_device,
        auto_transfer=auto_transfer,
    )


@PublicAPI(stability="beta")
def accelerate(amp: bool = False) -> None:
    try:
        set_accelerator(BaguaAccelerator(amp=amp))
    except RuntimeError:
        raise RuntimeError(
            "An accelerator has already been set. Make sure "
            "`train.torch.accelerate()` is not called multiple times, and is called "
            "before any of the prepare methods."
        )


@PublicAPI(stability="beta")
def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
    return get_accelerator(BaguaAccelerator).prepare_optimizer(optimizer)


@PublicAPI(stability="beta")
def backward(tensor: torch.Tensor) -> None:
    get_accelerator(BaguaAccelerator).backward(tensor)


def enable_reproducibility(seed: int = 0) -> None:
    get_accelerator(BaguaAccelerator).enable_reproducibility(seed)
