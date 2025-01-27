import logging
import os
import random
from typing import Any, Callable, Dict, Optional, Union

import numpy as np
import torch
from packaging.version import Version
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import (
    DataLoader,
    DistributedSampler,
    IterableDataset,
    RandomSampler,
    SequentialSampler,
)

import ray.train.torch
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.train.torch.train_loop_utils import _WrappedDataLoader
from ray.util.annotations import Deprecated, PublicAPI

if Version(torch.__version__) < Version("1.11.0"):
    FullyShardedDataParallel = None
else:
    from torch.distributed.fsdp import FullyShardedDataParallel

logger = logging.getLogger(__name__)


_TORCH_AMP_DEPRECATION_MESSAGE = (
    "The `accelerate`, `backward`, and `prepare_optimizer` utility methods "
    "in the `ray.train.torch` module are deprecated and will be removed in a "
    "future release. "
    "Please use the native PyTorch mixed precision API directly, or "
    "a library such as Lightning or HuggingFace Transformers/Accelerate. "
    "See this issue for more context: "
    "https://github.com/ray-project/ray/issues/49454"
)


def prepare_model(
    model: torch.nn.Module,
    move_to_device: Union[bool, torch.device] = True,
    parallel_strategy: Optional[str] = "ddp",
    parallel_strategy_kwargs: Optional[Dict[str, Any]] = None,
) -> torch.nn.Module:
    """Prepares the model for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    Args:
        model (torch.nn.Module): A torch model to prepare.
        move_to_device: Either a boolean indiciating whether to move
            the model to the correct device or an actual device to
            move the model to. If set to False, the model needs
            to manually be moved to the correct device.
        parallel_strategy ("ddp", "fsdp", or None): Whether to wrap models
            in ``DistributedDataParallel``, ``FullyShardedDataParallel``,
            or neither.
        parallel_strategy_kwargs (Dict[str, Any]): Args to pass into
            ``DistributedDataParallel`` or ``FullyShardedDataParallel``
            initialization if ``parallel_strategy`` is set to "ddp"
            or "fsdp", respectively.
    """
    if parallel_strategy == "fsdp" and FullyShardedDataParallel is None:
        raise ImportError(
            "FullyShardedDataParallel requires torch>=1.11.0. "
            "Run `pip install 'torch>=1.11.0'` to use FullyShardedDataParallel."
        )

    record_extra_usage_tag(TagKey.TRAIN_TORCH_PREPARE_MODEL, "1")

    parallel_strategy_kwargs = parallel_strategy_kwargs or {}

    rank = ray.train.get_context().get_local_rank()

    if isinstance(move_to_device, torch.device):
        device = move_to_device
    else:
        device = ray.train.torch.get_device()
        if isinstance(device, list):
            device = device[0]

    if torch.cuda.is_available():
        torch.cuda.set_device(device)

    if move_to_device:
        if rank == 0:
            logger.info(f"Moving model to device: {device}")
        else:
            logger.debug(f"Moving model to device: {device}")
        model = model.to(device)

    world_size = ray.train.get_context().get_world_size()

    if parallel_strategy and world_size > 1:
        if parallel_strategy == "ddp":
            DataParallel = DistributedDataParallel
            if torch.cuda.is_available():
                parallel_strategy_kwargs = {
                    "device_ids": [device],
                    "output_device": device,
                    **parallel_strategy_kwargs,
                }
        else:
            if not torch.cuda.is_available():
                raise RuntimeError(
                    "FSDP is only available with GPU-enabled "
                    "training. Set "
                    "`use_gpu=True` in your Trainer to train with "
                    "GPUs."
                )
            DataParallel = FullyShardedDataParallel
        if rank == 0:
            logger.info(f"Wrapping provided model in {DataParallel.__name__}.")
        else:
            logger.debug(f"Wrapping provided model in {DataParallel.__name__}.")
        model = DataParallel(model, **parallel_strategy_kwargs)

    return model


@PublicAPI(stability="stable")
def prepare_data_loader(
    data_loader: torch.utils.data.DataLoader,
    add_dist_sampler: bool = True,
    move_to_device: bool = True,
    auto_transfer: bool = True,
) -> torch.utils.data.DataLoader:
    """Prepares :class:`~torch.utils.data.DataLoader` for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    .. note::

        This method adds a `DistributedSampler` to the `DataLoader` if the
        number of training workers is greater than 1. If shuffling is
        enabled on the original `DataLoader`, then `shuffle=True` will also
        be passed into the `DistributedSampler` constructor. `shuffle=False`
        on the original `DataLoader` also means that shuffling is disabled
        on the sampler.

        With more than 1 worker, calling the `DistributedSampler.set_epoch` method
        at the beginning of each epoch before creating the DataLoader iterator
        is necessary to make shuffling work properly across multiple epochs.
        Otherwise, the same ordering will be always used.
        See: https://pytorch.org/docs/stable/data.html#torch.utils.data.distributed.DistributedSampler  # noqa: E501

    Example:

    .. testcode:
        :skipif: True

        import torch

        import ray.train.torch

        train_dataloader = torch.utils.data.DataLoader(
            ..., batch_size=..., shuffle=True
        )
        train_dataloader = ray.train.torch.prepare_data_loader(train_loader)

        for epoch in range(10):
            if ray.train.get_context().get_world_size() > 1:
                # Required for the distributed sampler to shuffle properly across epochs
                train_dataloader.sampler.set_epoch(epoch)

            for X, y in train_loader:
                # No need to move data to GPU, this is done by `prepare_data_loader`!
                # X, y = X.to("cuda"), y.to("cuda")
                ...

    Args:
        data_loader (torch.utils.data.DataLoader): The DataLoader to
            prepare.
        add_dist_sampler: Whether to add a DistributedSampler to
            the provided DataLoader.
        move_to_device: If set, automatically move the data
            returned by the data loader to the correct device.
        auto_transfer: If set and device is GPU, another CUDA stream
            is created to automatically copy data from host (CPU) memory
            to device (GPU) memory (the default CUDA stream still runs the
            training procedure). If device is CPU, it will be disabled
            regardless of the setting. This configuration will be ignored
            if ``move_to_device`` is False.
    """
    record_extra_usage_tag(TagKey.TRAIN_TORCH_PREPARE_DATALOADER, "1")

    world_size = ray.train.get_context().get_world_size()
    world_rank = ray.train.get_context().get_world_rank()

    # Only add Distributed Sampler if the following conditions hold:
    # 1. More than one training worker is being used.
    # 2. A DistributedSampler has not already been added by the user.
    # 3. The dataset is not an IterableDataset. Samplers do not worker with
    # IterableDatasets.
    if (
        world_size > 1
        and not isinstance(data_loader.sampler, DistributedSampler)
        and not (
            hasattr(data_loader, "dataset")
            and isinstance(data_loader.dataset, IterableDataset)
        )
        and add_dist_sampler
    ):

        def with_sampler(loader):
            # Automatically set the DistributedSampler

            # If you're using a sampler, the DataLoader shuffle flag must be set to
            # False. Shuffling is instead determined by the shuffle argument passed
            # to the DistributedSampler constructor.

            # If no sampler is passed to the DataLoader constructor, Torch
            # constructs a default sampler. The default sampler is a RandomSampler
            # if shuffling is enabled and a SequentialSampler otherwise. DataLoader
            # does not have a shuffle attribute, so we instead identify whether
            # shuffling is enabled by checking the default sampler type.
            shuffle = not isinstance(loader.sampler, SequentialSampler)
            worker_init_fn: Optional[Callable[[int], None]] = loader.worker_init_fn
            generator: Optional[torch.Generator] = loader.generator

            using_default_sampler = isinstance(
                loader.sampler, (SequentialSampler, RandomSampler)
            )
            if not using_default_sampler and world_rank == 0:
                logger.warning(
                    f"The {loader.sampler.__class__.__name__} will be overwritten "
                    "with a DistributedSampler. You can disable this by setting "
                    "`with_sampler` to False in `prepare_data_loader`."
                )

            data_loader_args = {
                "dataset": loader.dataset,
                "batch_size": loader.batch_size,
                "shuffle": False,
                "num_workers": loader.num_workers,
                "collate_fn": loader.collate_fn,
                "pin_memory": loader.pin_memory,
                "drop_last": loader.drop_last,
                "timeout": loader.timeout,
                "worker_init_fn": worker_init_fn,
                "generator": generator,
                "sampler": DistributedSampler(loader.dataset, shuffle=shuffle),
            }
            return DataLoader(**data_loader_args)

        data_loader = with_sampler(data_loader)

    if move_to_device:
        device = ray.train.torch.get_device()
        data_loader = _WrappedDataLoader(data_loader, device, auto_transfer)

    return data_loader


@Deprecated
def accelerate(amp: bool = False) -> None:
    raise DeprecationWarning(_TORCH_AMP_DEPRECATION_MESSAGE)


@Deprecated
def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
    raise DeprecationWarning(_TORCH_AMP_DEPRECATION_MESSAGE)


@Deprecated
def backward(tensor: torch.Tensor) -> None:
    raise DeprecationWarning(_TORCH_AMP_DEPRECATION_MESSAGE)


@PublicAPI(stability="stable")
def enable_reproducibility(seed: int = 0) -> None:
    """Limits sources of nondeterministic behavior.

    This function:

        * Seeds PyTorch, Python, and NumPy.
        * Disables CUDA convolution benchmarking.
        * Configures PyTorch to use determinstic algorithms.
        * Seeds workers spawned for multi-process data loading.

    Args:
        seed: The number to seed libraries and data workers with.

    .. warning:: ``train.torch.enable_reproducibility()`` can't guarantee
        completely reproducible results across executions. To learn more, read
        the `PyTorch notes on randomness
        <https://pytorch.org/docs/stable/notes/randomness.html>`_.
    """
    torch.manual_seed(seed)
    random.seed(seed)
    np.random.seed(seed)

    torch.use_deterministic_algorithms(True)
    torch.backends.cudnn.benchmark = False

    # If you want to use deterministic algorithms with CUDA, then you need to set
    # the CUBLAS_WORKSPACE_CONFIG environment variable; otherwise, Torch errors.
    # See https://docs.nvidia.com/cuda/cublas/index.html#cublasApi_reproducibility.
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
