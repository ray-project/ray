from typing import Dict, Iterator, Tuple
import logging
from abc import ABC, abstractmethod
import multiprocessing

import torch
from torch.utils.data import IterableDataset

import ray
import ray.train

from constants import DatasetKey
from config import BenchmarkConfig, TorchConfig
from dataloader_factory import BaseDataLoaderFactory
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


class TorchDataLoaderFactory(BaseDataLoaderFactory, ABC):
    """Factory for creating PyTorch DataLoaders."""

    @staticmethod
    def worker_init_fn(worker_id: int):
        """Initialize each worker with proper CUDA settings and seed.

        Args:
            worker_id: The ID of the worker being initialized
        """
        # Set worker-specific seed for reproducibility
        worker_seed = torch.initial_seed() % 2**32
        torch.manual_seed(worker_seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed(worker_seed)
            torch.cuda.manual_seed_all(worker_seed)

        logger.info(f"Initialized worker {worker_id} with seed {worker_seed}")

    def __init__(
        self,
        benchmark_config: BenchmarkConfig,
    ):
        """Initialize the factory.

        Args:
            benchmark_config: Configuration for the benchmark
        """
        super().__init__(benchmark_config)

        dataloader_config = self.get_dataloader_config()
        assert isinstance(dataloader_config, TorchConfig), type(dataloader_config)

        # Get worker configuration
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 1
        self.num_torch_workers = dataloader_config.num_torch_workers
        self.num_ray_workers = benchmark_config.num_workers

        # Log configuration without worker rank since context may not be initialized
        logger.info(
            f"Configuration: {self.num_ray_workers * self.num_torch_workers} total workers "
            f"({self.num_ray_workers} Ray × {self.num_torch_workers} Torch) "
            f"across {num_gpus} GPUs"
        )

    def _get_device(self) -> torch.device:
        """Get the device for the current worker using Ray Train's device management."""
        try:
            device = ray.train.torch.get_device()
        except RuntimeError:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"Worker {worker_rank}: Using device: {device}")
        return device

    @abstractmethod
    def create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a safe iterator that handles device transfer and error handling.

        Args:
            dataloader: The PyTorch DataLoader to iterate over
            device: The device to move tensors to

        Returns:
            An iterator that yields batches moved to the specified device
        """
        pass

    @abstractmethod
    def get_iterable_datasets(self) -> Dict[str, IterableDataset]:
        """Get the train and validation datasets.

        Returns:
            A dictionary containing the train and validation datasets.
        """
        pass

    def _create_multiprocessing_context(self):
        # Importing libs in torch dataloader worker subprocesses is very slow.
        # Preload some modules to speed up subprocess forking.
        ctx = multiprocessing.get_context("forkserver")
        modules = ["torch", "torchvision", "pandas", "numpy", "boto3", "fsspec"]
        ctx.set_forkserver_preload(modules)
        return ctx

    def _create_dataloader(self, dataset_key: DatasetKey, batch_size: int):
        worker_rank = ray.train.get_context().get_world_rank()
        dataloader_config = self.get_dataloader_config()

        # Create dataset and dataloader
        ds = self.get_iterable_datasets()[dataset_key]

        device = self._get_device()

        # Adjust worker settings for 0 workers case
        num_workers = max(0, self.num_torch_workers)
        persistent_workers = num_workers > 0
        pin_memory = dataloader_config.torch_pin_memory

        if dataloader_config.torch_prefetch_factor >= 0:
            prefetch_factor = dataloader_config.torch_prefetch_factor
        else:
            prefetch_factor = None

        timeout = (
            dataloader_config.torch_dataloader_timeout_seconds if num_workers > 0 else 0
        )

        logger.info(
            f"Worker {worker_rank}: Creating train DataLoader with "
            f"num_workers={num_workers}, pin_memory={pin_memory}, "
            f"persistent_workers={persistent_workers}, prefetch_factor={prefetch_factor}, "
            f"timeout={timeout}, batch_size={batch_size}"
        )

        multiprocessing_args = {}
        if num_workers > 0:
            multiprocessing_args = dict(
                multiprocessing_context=self._create_multiprocessing_context(),
                worker_init_fn=self.worker_init_fn,
                persistent_workers=persistent_workers,
            )
        dataloader = torch.utils.data.DataLoader(
            dataset=ds,
            batch_size=batch_size,
            num_workers=num_workers,
            pin_memory=pin_memory,
            prefetch_factor=prefetch_factor,
            timeout=timeout,
            drop_last=False,
            **multiprocessing_args,
        )
        # Add a DistributedSampler to the dataloader if possible (map-style datasets)
        dataloader = ray.train.torch.prepare_data_loader(
            dataloader, move_to_device=False
        )

        return self.create_batch_iterator(dataloader, device)

    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for training data.

        Returns:
            An iterator that yields (image, label) tensors for training
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"Worker {worker_rank}: Creating train dataloader")

        return self._create_dataloader(
            DatasetKey.TRAIN, self.get_dataloader_config().train_batch_size
        )

    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for validation data.

        Returns:
            An iterator that yields (image, label) tensors for validation
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"Worker {worker_rank}: Creating validation dataloader")

        return self._create_dataloader(
            DatasetKey.VALID, self.get_dataloader_config().validation_batch_size
        )
