# Standard library imports
import logging
import time
from typing import Dict, Tuple, Iterator, Generator, Optional, Union

# Third-party imports
import torch
import torchvision
import pyarrow
import ray
import ray.data
import ray.train
from ray.data.collate_fn import ArrowBatchCollateFn, CollateFn

# Local imports
from benchmark_factory import BenchmarkFactory
from config import BenchmarkConfig, DataloaderType, ImageClassificationConfig
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
            self.get_dataloader_config().limit_training_rows, total_workers
        )

        limit_validation_rows_per_worker = self._calculate_rows_per_worker(
            self.get_dataloader_config().limit_validation_rows, total_workers
        )

        return limit_training_rows_per_worker, limit_validation_rows_per_worker

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


class CustomArrowCollateFn(ArrowBatchCollateFn):
    """Custom collate function for converting Arrow batches to PyTorch tensors."""

    def __init__(
        self,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        pin_memory: bool = False,
    ):
        """Initialize the collate function.

        Args:
            dtypes: Optional torch dtype(s) for the tensors
            device: Optional device to place tensors on
        """
        self.dtypes = dtypes
        self.device = device
        self.pin_memory = pin_memory

    def __call__(self, batch: "pyarrow.Table") -> Tuple[torch.Tensor, torch.Tensor]:
        """Convert an Arrow batch to PyTorch tensors.

        Args:
            batch: PyArrow Table to convert

        Returns:
            Tuple of (image_tensor, label_tensor)
        """
        from ray.air._internal.torch_utils import (
            arrow_batch_to_tensors,
        )

        tensors = arrow_batch_to_tensors(
            batch,
            dtypes=self.dtypes,
            combine_chunks=self.device.type == "cpu",
            pin_memory=self.pin_memory,
        )
        return tensors["image"], tensors["label"]


class ImageClassificationRayDataLoaderFactory(RayDataLoaderFactory):
    """Factory for creating Ray DataLoader for image classification tasks."""

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)

    def _get_collate_fn(self) -> Optional[CollateFn]:
        return CustomArrowCollateFn(
            device=ray.train.torch.get_device(),
            pin_memory=self.get_dataloader_config().ray_data_pin_memory,
        )


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


def get_imagenet_data_dirs(task_config: ImageClassificationConfig) -> Dict[str, str]:
    """Returns a dict with the root imagenet dataset directories for train/val/test,
    corresponding to the data format and local/s3 dataset location."""
    from image_classification.imagenet import IMAGENET_LOCALFS_SPLIT_DIRS
    from image_classification.jpeg.imagenet import (
        IMAGENET_JPEG_SPLIT_S3_DIRS,
    )
    from image_classification.parquet.imagenet import (
        IMAGENET_PARQUET_SPLIT_S3_DIRS,
    )

    data_format = task_config.image_classification_data_format

    if task_config.image_classification_local_dataset:
        return IMAGENET_LOCALFS_SPLIT_DIRS

    if data_format == ImageClassificationConfig.ImageFormat.JPEG:
        return IMAGENET_JPEG_SPLIT_S3_DIRS
    elif data_format == ImageClassificationConfig.ImageFormat.PARQUET:
        return IMAGENET_PARQUET_SPLIT_S3_DIRS
    else:
        raise ValueError(f"Unknown data format: {data_format}")


class ImageClassificationFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        dataloader_type = self.benchmark_config.dataloader_type
        task_config = self.benchmark_config.task_config
        assert isinstance(task_config, ImageClassificationConfig)

        data_dirs = get_imagenet_data_dirs(task_config)

        data_format = task_config.image_classification_data_format

        if dataloader_type == DataloaderType.MOCK:
            return ImageClassificationMockDataLoaderFactory(self.benchmark_config)

        elif dataloader_type == DataloaderType.RAY_DATA:
            if data_format == ImageClassificationConfig.ImageFormat.JPEG:
                from image_classification.jpeg.factory import (
                    ImageClassificationJpegRayDataLoaderFactory,
                )

                return ImageClassificationJpegRayDataLoaderFactory(
                    self.benchmark_config, data_dirs
                )
            elif data_format == ImageClassificationConfig.ImageFormat.PARQUET:
                from image_classification.parquet.factory import (
                    ImageClassificationParquetRayDataLoaderFactory,
                )

                return ImageClassificationParquetRayDataLoaderFactory(
                    self.benchmark_config, data_dirs
                )

        elif dataloader_type == DataloaderType.TORCH:
            if data_format == ImageClassificationConfig.ImageFormat.JPEG:
                from image_classification.jpeg.factory import (
                    ImageClassificationJpegTorchDataLoaderFactory,
                )

                return ImageClassificationJpegTorchDataLoaderFactory(
                    self.benchmark_config, data_dirs
                )
            elif data_format == ImageClassificationConfig.ImageFormat.PARQUET:
                from image_classification.parquet.factory import (
                    ImageClassificationParquetTorchDataLoaderFactory,
                )

                return ImageClassificationParquetTorchDataLoaderFactory(
                    self.benchmark_config, data_dirs
                )

        raise ValueError(
            f"Invalid dataloader configuration: {dataloader_type}\n"
            f"{task_config}\n{self.benchmark_config.dataloader_config}"
        )

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
