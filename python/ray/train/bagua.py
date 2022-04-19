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
    """
    Bagua Backend Configurations (excluding training script args).
    reference: https://github.com/BaguaSys/bagua/blob/master/bagua/distributed/launch.py#L29-L154 # noqa: E501
    """

    nnodes: int = 1
    node_rank: int = 0
    nproc_per_node: int = 1
    master_addr: str = "127.0.0.1"
    master_port: int = 29500
    bagua_service_port: int = 29501
    set_additional_flag: bool = False
    no_python: bool = False
    logdir: Optional[str] = None
    autotune_level: int = 0
    is_output_autotune_log: bool = False
    report_metrics: bool = False
    autotune_max_samples: int = 60
    autotune_sampling_confidence_time: float = 5.0
    autotune_warmup_time: float = 30.0
    default_bucket_size: int = 10 * 1024 ** 2
    enable_bagua_net: bool = False
    host_list: str = ""
    ssh_port: str = 0
    store: Optional[torch.distributed.Store] = None

    @property
    def backend_cls(self):
        return BaguaBackend


def setup_bagua_process_group(store: Optional[torch.distributed.Store] = None):
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

            import copy

            master_addr, master_port = worker_group.execute_single(
                0, get_address_and_port
            )
            backend_config.master_addr = master_addr
            backend_config.master_port = str(master_port)

            def set_env_vars(world_size, config):
                # reference: https://github.com/BaguaSys/bagua/blob/master/bagua/distributed/launch.py#L183-L229 # noqa: E501
                # Follow the env setup of bagua.distributed.launch.
                current_env = os.environ.copy()
                current_env["MASTER_ADDR"] = config.master_addr
                current_env["MASTER_PORT"] = config.master_port
                current_env["WORLD_SIZE"] = str(world_size)
                current_env["FLASK_ENV"] = "development"

                from bagua.distributed.launch import set_bagua_env

                set_bagua_env(config, current_env)

                if "OMP_NUM_THREADS" not in os.environ and config.nproc_per_node > 1:
                    current_env["OMP_NUM_THREADS"] = str(1)
                    print(
                        "*****************************************\n"
                        "Setting OMP_NUM_THREADS environment variable for each process "
                        "to be {} in default, to avoid your system being overloaded, "
                        "please further tune the variable for optimal performance in "
                        "your application as needed. \n"
                        "*****************************************".format(
                            current_env["OMP_NUM_THREADS"]
                        )
                    )

                if config.logdir:
                    if os.path.exists(config.logdir):
                        if not os.path.isdir(config.logdir):
                            raise ValueError(
                                "argument --logdir must be a path to a directory."
                            )
                    else:
                        os.mkdir(os.path.join(os.getcwd(), config.logdir))

                # Following the current ray.train design, each worker uses only
                # 1 GPU by default. To allow Bagua to use multiple GPUs per worker,
                # override rank configurations for each spawned training process.
                # reference: https://github.com/BaguaSys/bagua/blob/master/bagua/distributed/launch.py#L231-L237 # noqa: E501
                dist_rank = config.nproc_per_node * config.node_rank
                current_env["RANK"] = str(dist_rank)
                current_env["LOCAL_RANK"] = str(0)
                current_env["NODE_RANK"] = str(config.node_rank)
                current_env["LOCAL_WORLD_SIZE"] = str(config.nproc_per_node)

                os.environ.update(current_env)

            dist_world_size = backend_config.nproc_per_node * backend_config.nnodes

            setup_futures = []
            for i in range(len(worker_group)):
                bagua_local_config = copy.deepcopy(backend_config)
                bagua_local_config.node_rank = i
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        set_env_vars,
                        world_size=dist_world_size,
                        config=bagua_local_config,
                    )
                )
            ray.get(setup_futures)

            worker_group.execute(setup_bagua_process_group, store=backend_config.store)
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
