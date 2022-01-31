import logging
from typing import Any, Dict, Optional

import torch
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import (
    DistributedSampler,
    DataLoader,
    IterableDataset,
    SequentialSampler,
)

from ray import train
from ray.util import PublicAPI
from ray.train.torch import get_device

from .base import Accelerator

logger = logging.getLogger(__name__)


class TorchAccelerator(Accelerator):
    def prepare_model(
        model: torch.nn.Module,
        move_to_device: bool = True,
        wrap_ddp: bool = True,
        ddp_kwargs: Optional[Dict[str, Any]] = None,
    ) -> torch.nn.Module:
        """Prepares the model for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            model (torch.nn.Module): A torch model to prepare.
            move_to_device (bool): Whether to move the model to the correct
                device. If set to False, the model needs to manually be moved
                to the correct device.
            wrap_ddp (bool): Whether to wrap models in
                ``DistributedDataParallel``.
            ddp_kwargs (Dict[str, Any]): Args to pass into
                ``DistributedDataParallel`` initialization if ``wrap_ddp`` is
                set to True.
        """
        ddp_kwargs = ddp_kwargs or {}

        rank = train.local_rank()

        device = get_device()

        if torch.cuda.is_available():
            torch.cuda.set_device(device)

        if move_to_device:
            logger.info(f"Moving model to device: {device}")
            model = model.to(device)
        if wrap_ddp and train.world_size() > 1:
            logger.info("Wrapping provided model in DDP.")
            if torch.cuda.is_available():
                model = DistributedDataParallel(
                    model, device_ids=[rank], output_device=rank, **ddp_kwargs
                )
            else:
                model = DistributedDataParallel(model, **ddp_kwargs)

        return model

    def prepare_data_loader(
        data_loader: torch.utils.data.DataLoader,
        add_dist_sampler: bool = True,
        move_to_device: bool = True,
    ) -> torch.utils.data.DataLoader:
        """
        Prepares DataLoader for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            data_loader (torch.utils.data.DataLoader): The DataLoader to
                prepare.
            add_dist_sampler (bool): Whether to add a DistributedSampler to
                the provided DataLoader.
            move_to_device (bool): If set, automatically move the data
                returned by the data loader to the correct device.
        """

        # Only add Distributed Sampler if the following conditions hold:
        # 1. More than one training worker is being used.
        # 2. A DistributedSampler has not already been added by the user.
        # 3. The dataset is not an IterableDataset. Samplers do not worker with
        # IterableDatasets.
        if (
            train.world_size() > 1
            and not isinstance(data_loader.sampler, DistributedSampler)
            and not (
                hasattr(data_loader, "dataset")
                and isinstance(data_loader.dataset, IterableDataset)
            )
            and add_dist_sampler
        ):

            def with_sampler(loader):
                # Automatically set the DistributedSampler

                # If using a sampler, the shuffle attribute in the
                # DataLoader must be set to False.
                # Instead the shuffling is determined by the shuffle attribute
                # in the DistributedSampler.
                # We identify if shuffling is enabled in the passed in
                # DataLoader by seeing if the sampler for the DataLoader is a
                # SequentialSampler.
                shuffle = not isinstance(loader.sampler, SequentialSampler)

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
                    "sampler": DistributedSampler(loader.dataset, shuffle=shuffle),
                }
                return DataLoader(**data_loader_args)

            data_loader = with_sampler(data_loader)

        if move_to_device:
            device = get_device()
            data_loader = _WrappedDataLoader(data_loader, device)

        return data_loader

    def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
        """Wraps optimizer to support automatic mixed precision.

        Args:
            optimizer (torch.optim.Optimizer): The DataLoader to prepare.

        Returns:
            A wrapped optimizer.
        """
        raise NotImplementedError

    def backward(tensor: torch.Tensor) -> None:
        """Computes the gradient of the specified tensor w.r.t. graph leaves.

        Args:
            tensor (torch.Tensor): Tensor of which the derivative will be computed.
        """
        raise NotImplementedError


class _WrappedDataLoader(DataLoader):
    def __init__(self, base_dataloader: DataLoader, device: torch.device):

        self.__dict__.update(getattr(base_dataloader, "__dict__", {}))
        self.dataloader = base_dataloader
        self.device = device

    def _move_to_device(self, item):
        def try_move_device(i):
            try:
                i = i.to(self.device)
            except AttributeError:
                logger.debug(f"Item {i} cannot be moved to device " f"{self.device}.")
            return i

        return tuple(try_move_device(i) for i in item)

    def __len__(self):
        return len(self.dataloader)

    def __iter__(self):
        iterator = iter(self.dataloader)

        for item in iterator:
            yield self._move_to_device(item)
