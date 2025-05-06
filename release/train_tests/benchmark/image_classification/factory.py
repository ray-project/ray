# Standard library imports
import logging
import time
from typing import Any, Dict, Tuple, Iterator, Generator, Optional

# Third-party imports
import torch
import ray
import ray.data
import ray.train

# Local imports
from config import BenchmarkConfig
from dataloader_factory import BaseDataLoaderFactory
from torch_dataloader_factory import TorchDataLoaderFactory
from ray_dataloader_factory import RayDataLoaderFactory
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


def mock_dataloader(
    num_batches: int = 64, batch_size: int = 32
) -> Generator[Tuple[torch.Tensor, torch.Tensor], None, None]:
    """Generate mock image and label tensors for testing.

    Args:
        num_batches: Number of batches to generate
        batch_size: Number of samples per batch

    Yields:
        Tuple of (image_tensor, label_tensor) for each batch
    """
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationTorchDataLoaderFactory(TorchDataLoaderFactory):
    """Factory for creating PyTorch DataLoaders for image classification tasks.

    Features:
    - Distributed file reading with round-robin worker distribution
    - Device transfer and error handling for data batches
    - Configurable row limits per worker for controlled processing
    - Performance monitoring and logging
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)

    def _calculate_rows_per_worker(
        self, total_rows: Optional[int], num_workers: int
    ) -> Optional[int]:
        """Calculate rows per worker for balanced data distribution.

        Args:
            total_rows: Total rows to process across all workers
            num_workers: Total workers (Ray workers × Torch workers)

        Returns:
            Rows per worker or None if no limit. Each worker gets at least 1 row.
        """
        if total_rows is None:
            return None

        if num_workers == 0:
            return total_rows

        return max(1, total_rows // num_workers)

    def _get_worker_row_limits(self) -> Tuple[Optional[int], Optional[int]]:
        """Calculate row limits per worker for training and validation.

        Returns:
            Tuple of (training_rows_per_worker, validation_rows_per_worker)
        """
        dataloader_config = self.get_dataloader_config()
        num_workers = max(1, dataloader_config.num_torch_workers)
        total_workers = self.benchmark_config.num_workers * num_workers

        limit_training_rows_per_worker = self._calculate_rows_per_worker(
            self.benchmark_config.limit_training_rows, total_workers
        )

        limit_validation_rows_per_worker = self._calculate_rows_per_worker(
            self.benchmark_config.limit_validation_rows, total_workers
        )

        return limit_training_rows_per_worker, limit_validation_rows_per_worker

    def _get_total_row_limits(self) -> Tuple[Optional[int], Optional[int]]:
        """Get total row limits for training and validation.

        Returns:
            Tuple of (total_training_rows, total_validation_rows)
        """
        total_training_rows = (
            self.benchmark_config.limit_training_rows
            if self.benchmark_config.limit_training_rows is not None
            else None
        )
        total_validation_rows = (
            self.benchmark_config.limit_validation_rows
            if self.benchmark_config.limit_validation_rows is not None
            else None
        )

        return total_training_rows, total_validation_rows

    def create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create iterator with device transfer and error handling.

        Args:
            dataloader: PyTorch DataLoader to iterate over
            device: Target device for tensor transfer

        Returns:
            Iterator yielding (image_tensor, label_tensor) on target device
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"Worker {worker_rank}: Starting batch iteration")

        try:
            last_batch_time = time.time()
            for batch_idx, batch in enumerate(dataloader):
                try:
                    # Monitor batch processing delays
                    current_time = time.time()
                    time_since_last_batch = current_time - last_batch_time
                    if time_since_last_batch > 10:
                        logger.warning(
                            f"Worker {worker_rank}: Long delay ({time_since_last_batch:.2f}s) "
                            f"between batches {batch_idx-1} and {batch_idx}"
                        )

                    # Process and transfer batch to device
                    images, labels = batch
                    logger.info(
                        f"Worker {worker_rank}: Processing batch {batch_idx} (shape: {images.shape}, "
                        f"time since last: {time_since_last_batch:.2f}s)"
                    )

                    # Transfer tensors to target device
                    transfer_start = time.time()
                    dataloader_config = self.get_dataloader_config()
                    images = images.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    labels = labels.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    transfer_time = time.time() - transfer_start

                    # Monitor device transfer performance
                    if transfer_time > 5:
                        logger.warning(
                            f"Worker {worker_rank}: Slow device transfer ({transfer_time:.2f}s) "
                            f"for batch {batch_idx}"
                        )

                    logger.info(
                        f"Worker {worker_rank}: Completed device transfer for batch {batch_idx} in "
                        f"{transfer_time:.2f}s"
                    )

                    last_batch_time = time.time()
                    yield images, labels

                except Exception as e:
                    logger.error(
                        f"Worker {worker_rank}: Error processing batch {batch_idx}: {str(e)}",
                        exc_info=True,
                    )
                    raise

        except Exception as e:
            logger.error(
                f"Worker {worker_rank}: Error in batch iterator: {str(e)}",
                exc_info=True,
            )
            raise


class ImageClassificationRayDataLoaderFactory(RayDataLoaderFactory):
    """Factory for creating Ray DataLoader for image classification tasks.

    Features:
    - Distributed file reading with round-robin worker distribution
    - Device transfer and error handling for data batches
    - Configurable row limits per worker for controlled processing
    - Performance monitoring and logging
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)

    def collate_fn(self, batch: Dict[str, Any]) -> Tuple[torch.Tensor, torch.Tensor]:
        """Convert Ray data batch to PyTorch tensors on the appropriate device.

        Args:
            batch: Dictionary with 'image' and 'label' numpy arrays

        Returns:
            Tuple of (image_tensor, label_tensor) on the target device
        """
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        device = ray.train.torch.get_device()
        batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

        return batch["image"], batch["label"]


class ImageClassificationMockDataLoaderFactory(BaseDataLoaderFactory):
    """Factory for creating mock dataloaders for testing.

    Provides mock implementations of training and validation dataloaders
    that generate random image and label tensors.
    """

    def get_train_dataloader(
        self,
    ) -> Generator[Tuple[torch.Tensor, torch.Tensor], None, None]:
        """Get mock training dataloader.

        Returns:
            Generator yielding (image_tensor, label_tensor) batches
        """
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=1024, batch_size=dataloader_config.train_batch_size
        )

    def get_val_dataloader(
        self,
    ) -> Generator[Tuple[torch.Tensor, torch.Tensor], None, None]:
        """Get mock validation dataloader.

        Returns:
            Generator yielding (image_tensor, label_tensor) batches
        """
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=512, batch_size=dataloader_config.validation_batch_size
        )
