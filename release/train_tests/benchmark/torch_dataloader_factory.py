from typing import Iterator, Optional, Tuple
import time
import logging
import multiprocessing

import torch

import ray.train
import ray

from config import BenchmarkConfig, TorchConfig
from dataloader_factory import BaseDataLoaderFactory
from image_classification.torch_parquet_image_iterable_dataset import (
    S3Reader,
    S3ParquetImageIterableDataset,
)

logger = logging.getLogger(__name__)

# Set multiprocessing start method to 'spawn' for CUDA compatibility
if torch.cuda.is_available():
    try:
        multiprocessing.set_start_method("spawn", force=True)
        logger.info(
            "[DataLoader] Set multiprocessing start method to 'spawn' for CUDA compatibility"
        )
    except RuntimeError:
        logger.info("[DataLoader] Multiprocessing start method already set")


class TorchDataLoaderFactory(BaseDataLoaderFactory, S3Reader):
    """Factory for creating PyTorch DataLoaders that read from S3 parquet files.

    This factory:
    1. Creates DataLoaders that read Parquet files from S3
    2. Distributes files among Ray workers using round-robin allocation
    3. Handles device transfer and error handling for batches
    4. Supports row limits per worker for controlled data processing
    """

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

        logger.info(
            f"[DataLoader] Initialized worker {worker_id} with seed {worker_seed}"
        )

    def __init__(
        self,
        benchmark_config: BenchmarkConfig,
        train_url: str,
        val_url: str,
        limit_validation_rows: Optional[int] = None,
    ):
        """Initialize the factory.

        Args:
            benchmark_config: Configuration for the benchmark
            train_url: S3 URL for training data
            val_url: S3 URL for validation data
            limit_validation_rows: Optional limit on total validation rows to process
        """
        super().__init__(benchmark_config)

        dataloader_config = self.get_dataloader_config()
        assert isinstance(dataloader_config, TorchConfig), type(dataloader_config)

        self.train_url = train_url
        self.val_url = val_url

        # Get worker configuration
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 1
        self.num_torch_workers = dataloader_config.num_torch_workers
        self.num_ray_workers = benchmark_config.num_workers

        # Calculate total workers and row limits
        total_workers = self.num_ray_workers * self.num_torch_workers
        if limit_validation_rows is not None:
            if total_workers > 0:
                self.limit_validation_rows_per_worker = max(
                    1, limit_validation_rows // total_workers
                )
            else:
                # When no workers, apply limit directly to the dataset
                self.limit_validation_rows_per_worker = limit_validation_rows
        else:
            self.limit_validation_rows_per_worker = None

        # Log configuration
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[DataLoader] Worker {worker_rank}: Configuration: {total_workers} total workers "
            f"({self.num_ray_workers} Ray × {self.num_torch_workers} Torch) "
            f"across {num_gpus} GPUs"
        )
        if limit_validation_rows is not None:
            if total_workers > 0 and limit_validation_rows < total_workers:
                logger.warning(
                    f"[DataLoader] Worker {worker_rank}: Warning: limit_validation_rows ({limit_validation_rows}) is less than "
                    f"total_workers ({total_workers}). Each worker will process at least 1 row."
                )
            logger.info(
                f"[DataLoader] Worker {worker_rank}: Validation rows per worker: {self.limit_validation_rows_per_worker}"
            )

    def _get_device(self) -> torch.device:
        """Get the device for the current worker using Ray Train's device management."""
        device = ray.train.torch.get_device()
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[DataLoader] Worker {worker_rank}: Using Ray Train device: {device}"
        )
        return device

    def _create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a safe iterator that handles device transfer and error handling.

        Args:
            dataloader: The PyTorch DataLoader to iterate over
            device: The device to move tensors to

        Returns:
            An iterator that yields batches moved to the specified device
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank}: Starting batch iteration")

        try:
            last_batch_time = time.time()
            for batch_idx, batch in enumerate(dataloader):
                try:
                    # Check for delays between batches
                    current_time = time.time()
                    time_since_last_batch = current_time - last_batch_time
                    if time_since_last_batch > 10:
                        logger.warning(
                            f"[DataLoader] Worker {worker_rank}: "
                            f"Long delay ({time_since_last_batch:.2f}s) between batches {batch_idx-1} and {batch_idx}"
                        )

                    # Move batch to device
                    images, labels = batch
                    logger.info(
                        f"[DataLoader] Worker {worker_rank}: Processing batch {batch_idx} "
                        f"(shape: {images.shape}, time since last: {time_since_last_batch:.2f}s)"
                    )

                    transfer_start = time.time()
                    dataloader_config = self.get_dataloader_config()
                    images = images.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    labels = labels.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    transfer_time = time.time() - transfer_start

                    if transfer_time > 5:
                        logger.warning(
                            f"[DataLoader] Worker {worker_rank}: "
                            f"Slow device transfer ({transfer_time:.2f}s) for batch {batch_idx}"
                        )

                    logger.info(
                        f"[DataLoader] Worker {worker_rank}: Completed device transfer "
                        f"for batch {batch_idx} in {transfer_time:.2f}s"
                    )

                    last_batch_time = time.time()
                    yield images, labels

                except Exception as e:
                    logger.error(
                        f"[DataLoader] Worker {worker_rank}: Error processing batch {batch_idx}: {str(e)}",
                        exc_info=True,
                    )
                    raise

        except Exception as e:
            logger.error(
                f"[DataLoader] Worker {worker_rank}: Error in batch iterator: {str(e)}",
                exc_info=True,
            )
            raise

    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for training data.

        Returns:
            An iterator that yields (image, label) tensors for training
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank}: Creating train dataloader")

        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        # Create dataset and dataloader
        train_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.train_url),
            random_transforms=True,
        )

        # Adjust worker settings for 0 workers case
        num_workers = max(0, self.num_torch_workers)
        persistent_workers = num_workers > 0
        pin_memory = (
            dataloader_config.torch_pin_memory and torch.cuda.is_available()
        )  # Use config setting

        # Only set prefetch_factor and timeout when using workers
        prefetch_factor = (
            dataloader_config.prefetch_batches if num_workers > 0 else None
        )
        timeout = (
            dataloader_config.torch_dataloader_timeout_seconds if num_workers > 0 else 0
        )

        logger.info(
            f"[DataLoader] Worker {worker_rank}: Creating train DataLoader with "
            f"num_workers={num_workers}, pin_memory={pin_memory}, "
            f"persistent_workers={persistent_workers}, prefetch_factor={prefetch_factor}, "
            f"timeout={timeout}"
        )

        dataloader = torch.utils.data.DataLoader(
            dataset=train_ds,
            batch_size=dataloader_config.train_batch_size,
            num_workers=num_workers,
            pin_memory=pin_memory,
            persistent_workers=persistent_workers,
            prefetch_factor=prefetch_factor,
            timeout=timeout,
            drop_last=True,
            worker_init_fn=self.worker_init_fn if num_workers > 0 else None,
        )

        return self._create_batch_iterator(dataloader, device)

    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for validation data.

        Returns:
            An iterator that yields (image, label) tensors for validation
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[DataLoader] Worker {worker_rank}: Creating validation dataloader"
        )

        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        # Create dataset and dataloader with row limits
        val_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.val_url),
            random_transforms=False,
            limit_rows_per_worker=self.limit_validation_rows_per_worker,
        )

        # Adjust worker settings for 0 workers case
        num_workers = max(0, self.num_torch_workers)
        persistent_workers = num_workers > 0
        pin_memory = (
            dataloader_config.torch_pin_memory and torch.cuda.is_available()
        )  # Use config setting

        # Only set prefetch_factor and timeout when using workers
        prefetch_factor = (
            dataloader_config.prefetch_batches if num_workers > 0 else None
        )
        timeout = (
            dataloader_config.torch_dataloader_timeout_seconds if num_workers > 0 else 0
        )

        logger.info(
            f"[DataLoader] Worker {worker_rank}: Creating validation DataLoader with "
            f"num_workers={num_workers}, pin_memory={pin_memory}, "
            f"persistent_workers={persistent_workers}, prefetch_factor={prefetch_factor}, "
            f"timeout={timeout}, limit_rows_per_worker={self.limit_validation_rows_per_worker}"
        )

        dataloader = torch.utils.data.DataLoader(
            dataset=val_ds,
            batch_size=dataloader_config.validation_batch_size,
            num_workers=num_workers,
            pin_memory=pin_memory,
            persistent_workers=persistent_workers,
            prefetch_factor=prefetch_factor,
            timeout=timeout,
            drop_last=False,
            worker_init_fn=self.worker_init_fn if num_workers > 0 else None,
        )

        return self._create_batch_iterator(dataloader, device)
