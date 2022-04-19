from dataclasses import dataclass
import logging
import os

from typing import List, Optional

import ray
import ray.train.torch
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

try:
    from torch.profiler import profile
except ImportError:
    profile = None

logger = logging.getLogger(__name__)


def _get_torch_distributed_sampler(dataset, shuffle):
    # Return a torch DistributedSampler with Bagua configurations
    return DistributedSampler(
        dataset,
        num_replicas=bagua.torch_api.get_world_size(),  # equivalent to ray.train.size()
        rank=bagua.torch_api.get_rank(),  # equivalent to ray.train.world_rank()
        shuffle=shuffle,
    )


class BaguaAccelerator(TorchAccelerator):
    """A utility that implements methods to accelerate PyTorch training using Bagua.

    Arguments:
        amp (bool): If true, perform training with automatic mixed precision.
            Otherwise, use full precision.
    """

    def __init__(self, amp: bool = False):
        super().__init__(amp=amp)

    def prepare_model(
        self,
        model: torch.nn.Module,
        optimizers: List[torch.optim.Optimizer],
        algorithm: bagua.torch_api.algorithms.Algorithm,
        process_group: Optional[bagua.torch_api.communication.BaguaProcessGroup] = None,
        do_flatten: bool = True,
    ) -> torch.nn.Module:
        """Prepares a torch model for Bagua distributed execution.

        See https://bagua.readthedocs.io/en/latest/autoapi/bagua/torch_api/index.html#bagua.torch_api.BaguaModule.with_bagua # noqa: E501

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU). Same functionality
        as ``TorchAccelerator.prepare_model()``.

        Args:
            model (torch.nn.Module): A torch model to prepare.
            optimizers (List[torch.optim.Optimizer]): A list of optimizers
                that updates model parameters. It can contain one or more
                torch optimizers.
            algorithm (bagua.torch_api.algorithms.Algorithm): A Bagua
                distributed training algorithm.
            process_group (Optional[bagua.torch_api.communication.BaguaProcessGroup]):
                The process group to be used for distributed data all-reduction.
                If set to None, the default process group created by
                ``bagua.torch_api.init_process_group()`` will be used. Defaults to None.
            do_flatten (bool): Whether to flatten the Bagua buckets.
                The flatten operation will reset data pointer of
                bucket tensors so that they can use faster code paths. Defaults to True.
        """
        device = self.get_device()
        model = model.to(device)

        if torch.cuda.is_available():
            torch.cuda.set_device(device)

        if self.amp_is_enabled:
            model = self._patch_model_forward_and_state(model)

        logger.info("Wrapping provided model in BaguaDDP.")
        model = model.with_bagua(
            optimizers=optimizers,
            algorithm=algorithm,
            process_group=process_group,
            do_flatten=do_flatten,
        )
        return model


@PublicAPI(stability="beta")
@dataclass
class BaguaConfig(BackendConfig):
    """Configuration for Bagua process group setup.

    See https://github.com/BaguaSys/bagua/blob/master/bagua/distributed/launch.py#L29-L154 # noqa: E501

    Args:
        nnodes (int): The number of nodes (workers) used
            for distributed training.
        node_rank (int): The rank of the node (worker).
        nproc_per_node (int): The number of processes to
            launch on each node (worker) for GPU training.
            By default, Ray Train sets it to 1.
        master_addr (str): Master node (worker)'s address.
        master_port (int): Master node (worker)'s free port.
        bagua_service_port (int): The free port for Bagua service.
        logdir (Optional[str]): Path to write training logs.
        autotune_level (bool): Bagua automatic super parameter search level.
            The higher the level, the better the theoretical effect,
            and the longer it takes. Defaults to 0.
        is_output_autotune_log (bool): Whether to log autotune output.
            Defaults to False.
        report_metrics (bool): Whether to report Bagua metrics.
            Defaults to False.
        autotune_max_samples (int): Maximum samples used for Bagua
            performance autotuning. Defaults to 60.
        autotune_sampling_confidence_time (float): Bagua performance
            autotuning sampling confidence time. Defaults to 5.0s.
        autotune_warmup_time (float): Bagua performance autotuning
            warmup time. Defaults to 30.0s.
        default_bucket_size (int): Bagua default bucket size.
            Defaults to 10 * 1024 * 2.
        enable_bagua_net (bool): Whether to enable Bagua-Net for better
            communication performance. Defaults to False.
        host_list (str): Host list.
        ssh_port (int): SSH port.
        store (Optional[torch.distributed.Store]): A key-value store used
            to exchange connection/address information across all workers.
            If None, a TCP-based store will be created. Defaults to None.
    """

    nnodes: int = 1
    node_rank: int = 0
    nproc_per_node: int = 1
    master_addr: str = "127.0.0.1"
    master_port: int = 29500
    bagua_service_port: int = 29501
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
    """Connects the Bagua distributed training backend.

    Args:
        store (Optional[torch.distributed.Store]): A key-value store used
             to exchange connection/address information across all workers.
             If None, a TCP-based store will be created. Defaults to None.
    """
    torch.cuda.set_device(bagua.torch_api.get_local_rank())
    # init_process_group() is different from ray.train.torch approach
    # See https://github.com/BaguaSys/bagua/blob/master/bagua/torch_api/communication.py#L524-L529 # noqa: E501
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
                # Follow the env setup of bagua.distributed.launch.
                # See https://github.com/BaguaSys/bagua/blob/master/bagua/distributed/launch.py#L183-L229 # noqa: E501
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
            raise RuntimeError(
                "Distributed torch is not available. "
                "Cannot set up Bagua training backend."
            )


@PublicAPI(stability="beta")
def get_device() -> torch.device:
    """Gets the correct torch device to use for training."""
    return get_accelerator(BaguaAccelerator).get_device()


@PublicAPI(stability="beta")
def prepare_model(
    model: torch.nn.Module,
    optimizer: torch.optim.Optimizer,
    algorithm: bagua.torch_api.algorithms.Algorithm,
    process_group: Optional[bagua.torch_api.communication.BaguaProcessGroup] = None,
    do_flatten: bool = True,
) -> torch.nn.Module:
    """Prepares a torch model for Bagua distributed execution.

    See https://bagua.readthedocs.io/en/latest/autoapi/bagua/torch_api/index.html#bagua.torch_api.BaguaModule.with_bagua # noqa: E501

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU). Same functionality
    as ``TorchAccelerator.prepare_model()``.

    Args:
        model (torch.nn.Module): A torch model to prepare.
        optimizers (List[torch.optim.Optimizer]): A list of optimizers
            that updates model parameters. It can contain one or more
            torch optimizers.
        algorithm (bagua.torch_api.algorithms.Algorithm): A Bagua
            distributed training algorithm.
        process_group (Optional[bagua.torch_api.communication.BaguaProcessGroup]):
            The process group to be used for distributed data all-reduction.
            If set to None, the default process group created by
            ``bagua.torch_api.init_process_group()`` will be used. Defaults to None.
        do_flatten (bool): Whether to flatten the Bagua buckets.
            The flatten operation will reset data pointer of
            bucket tensors so that they can use faster code paths. Defaults to True.
    """
    return get_accelerator(BaguaAccelerator).prepare_model(model, optimizer, algorithm)


@PublicAPI(stability="beta")
def prepare_data_loader(
    data_loader: torch.utils.data.DataLoader,
    add_dist_sampler: bool = True,
    move_to_device: bool = True,
    auto_transfer: bool = True,
) -> torch.utils.data.DataLoader:
    """Prepares DataLoader for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU). Same functionality
    as ``TorchAccelerator.prepare_data_loader()``.

    Args:
        data_loader (torch.utils.data.DataLoader): The DataLoader to
            prepare.
        add_dist_sampler (bool): Whether to add a DistributedSampler to
            the provided DataLoader.
        move_to_device (bool): If set, automatically move the data
            returned by the data loader to the correct device.
        auto_transfer (bool): If set and device is GPU, another CUDA stream
            is created to automatically copy data from host (CPU) memory
            to device (GPU) memory (the default CUDA stream still runs the
            training procedure). If device is CPU, it will be disabled
            regardless of the setting. This configuration will be ignored
            if ``move_to_device`` is False.
    """
    return get_accelerator(BaguaAccelerator).prepare_data_loader(
        data_loader,
        add_dist_sampler=add_dist_sampler,
        move_to_device=move_to_device,
        auto_transfer=auto_transfer,
    )


@PublicAPI(stability="beta")
def accelerate(amp: bool = False) -> None:
    """Enables training optimizations.

    Arguments:
        amp (bool): If True, perform training with automatic mixed precision.
            Otherwise, use full precision.

    .. warning:: ``train.bagua.accelerate`` cannot be called more than once, and it
       must be called before any other ``train.torch`` utility function.
    """
    try:
        set_accelerator(BaguaAccelerator(amp=amp))
    except RuntimeError:
        raise RuntimeError(
            "An accelerator has already been set. Make sure "
            "`train.bagua.accelerate()` is not called multiple times, and is called "
            "before any of the prepare methods."
        )


@PublicAPI(stability="beta")
def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
    """Wraps optimizer to support automatic mixed precision.

    Args:
        optimizer (torch.optim.Optimizer): The DataLoader to prepare.

    Returns:
        A wrapped optimizer.
    """
    return get_accelerator(BaguaAccelerator).prepare_optimizer(optimizer)


@PublicAPI(stability="beta")
def backward(tensor: torch.Tensor) -> None:
    """Computes the gradient of the specified tensor w.r.t. graph leaves.

    Args:
        tensor (torch.Tensor): Tensor of which the derivative will be computed.
    """
    get_accelerator(BaguaAccelerator).backward(tensor)


def enable_reproducibility(seed: int = 0) -> None:
    """Limits sources of nondeterministic behavior.

    This function:
        * Seeds PyTorch, Python, and NumPy.
        * Disables CUDA convolution benchmarking.
        * Configures PyTorch to use determinstic algorithms.
        * Seeds workers spawned for multi-process data loading.

    Args:
        seed (int): The number to seed libraries and data workers with.

    .. warning:: ``train.bagua.enable_reproducibility()`` can't guarantee
        completely reproducible results across executions. To learn more, read
        the `PyTorch notes on randomness
        <https://pytorch.org/docs/stable/notes/randomness.html>`_.
    """
    get_accelerator(BaguaAccelerator).enable_reproducibility(seed)
