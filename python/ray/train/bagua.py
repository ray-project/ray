import tempfile
from dataclasses import dataclass
import functools
import io
import logging
import os
import random
import types

from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import ray
from ray import train
from ray.train.torch import (
    TorchAccelerator,
    TorchBackend,
)
from ray.train.accelerator import Accelerator
from ray.train.backend import BackendConfig, Backend, EncodedData
from ray.train.constants import PYTORCH_PROFILER_KEY
from torch.optim import Optimizer
from ray.train.session import get_accelerator, set_accelerator
from ray.train.worker_group import WorkerGroup
from ray.train.utils import get_address_and_port
from ray.util import PublicAPI

import numpy as np
import torch
from torch.cuda.amp import autocast, GradScaler
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import (
    DistributedSampler,
    DataLoader,
    IterableDataset,
    SequentialSampler,
    RandomSampler,
)

import bagua.torch_api as bagua
from bagua.torch_api.algorithms import gradient_allreduce

try:
    from torch.profiler import profile
except ImportError:
    profile = None

logger = logging.getLogger(__name__)


def _get_torch_distributed_sampler(dataset, shuffle):
    return DistributedSampler(dataset,
                              num_replicas=bagua.get_world_size(),  # equivalent to ray.train.size()
                              rank=bagua.get_rank(),  # equivalent to ray.train.world_rank()
                              shuffle=shuffle)


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

        if torch.cuda.is_available():
            torch.cuda.set_device(device)

        if self.amp_is_enabled:
            model = self._patch_model_forward_and_state(model)

        if train.world_size() > 1:
            logger.info("Wrapping provided model in BaguaDDP.")
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
    torch.cuda.set_device(bagua.get_local_rank())
    bagua.init_process_group(store)


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
                        store = store,
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
    algorithm: bagua.torch_api.algorithms.Algorithm = gradient_allreduce.GradientAllReduceAlgorithm()
) -> torch.nn.Module:
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
