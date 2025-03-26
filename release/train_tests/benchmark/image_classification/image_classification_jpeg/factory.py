# Standard library imports
import logging
import time
from typing import Any, Dict, Iterator, Tuple, Optional

# Third-party imports
import torch
import torchvision
from torch.utils.data import IterableDataset
import pyarrow

# Ray imports
import ray.train
from ray.data.datasource.partitioning import Partitioning

# Local imports
from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import BaseDataLoaderFactory
from ray_dataloader_factory import RayDataLoaderFactory
from torch_dataloader_factory import TorchDataLoaderFactory
from image_classification.factory import ImageClassificationMockDataLoaderFactory
from s3_reader import AWS_REGION
from .imagenet import get_preprocess_map_fn, IMAGENET_JPEG_SPLIT_S3_DIRS
from .torch_jpeg_image_iterable_dataset import S3JpegReader, S3JpegImageIterableDataset

logger = logging.getLogger(__name__)


class ImageClassificationJpegRayDataLoaderFactory(RayDataLoaderFactory):
    """Factory for creating Ray DataLoader for JPEG image classification.

    This factory:
    1. Sets up S3 filesystem with boto credentials
    2. Creates Ray datasets for training and validation
    3. Handles resource allocation for concurrent validation
    4. Provides collation function for PyTorch tensors
    """

    def get_s3fs_with_boto_creds(
        self, connection_timeout: int = 60, request_timeout: int = 60
    ) -> "pyarrow.fs.S3FileSystem":
        """Create S3 filesystem with boto credentials.

        Args:
            connection_timeout: Timeout for establishing connection in seconds
            request_timeout: Timeout for requests in seconds

        Returns:
            Configured S3FileSystem instance
        """
        import boto3
        from pyarrow import fs

        credentials = boto3.Session().get_credentials()

        s3fs = fs.S3FileSystem(
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            session_token=credentials.token,
            region=AWS_REGION,
            connect_timeout=connection_timeout,
            request_timeout=request_timeout,
        )
        return s3fs

    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        """Get Ray datasets for training and validation.

        This method:
        1. Sets up S3 filesystem connection
        2. Creates training dataset with random transforms
        3. Creates validation dataset without transforms
        4. Configures resource allocation for concurrent validation

        Returns:
            Dict with "train" and "val" Dataset objects

        Note:
            Currently using training data for validation due to partitioning structure
        """
        # Use a file pattern to read images more efficiently
        s3fs = self.get_s3fs_with_boto_creds()

        # Set up training dataset with partitioning by class
        train_pattern = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        train_partitioning = Partitioning(
            "dir", base_dir=train_pattern, field_names=["class"]
        )
        train_ds = (
            ray.data.read_images(
                train_pattern,
                mode="RGB",
                include_paths=True,
                partitioning=train_partitioning,
                filesystem=s3fs,
            )
            .limit(self.benchmark_config.limit_training_rows)
            .map(get_preprocess_map_fn(random_transforms=True))
        )

        # Set up validation dataset (currently using training data due to partitioning)
        val_pattern = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        val_partitioning = Partitioning(
            "dir", base_dir=val_pattern, field_names=["class"]
        )
        val_ds = (
            ray.data.read_images(
                val_pattern,
                mode="RGB",
                include_paths=True,
                partitioning=val_partitioning,
                filesystem=s3fs,
            )
            .limit(self.benchmark_config.limit_validation_rows)
            .map(get_preprocess_map_fn(random_transforms=False))
        )

        # Configure resource allocation for concurrent validation
        if self.benchmark_config.validate_every_n_steps > 0:
            cpus_to_exclude = 16
            train_ds.context.execution_options.exclude_resources = (
                train_ds.context.execution_options.exclude_resources.add(
                    ray.data.ExecutionResources(cpu=cpus_to_exclude)
                )
            )
            val_ds.context.execution_options.resource_limits = (
                ray.data.ExecutionResources(cpu=cpus_to_exclude)
            )
            logger.info(
                f"[ImageClassificationJpegRayDataLoaderFactory] Reserving {cpus_to_exclude} CPUs "
                "for validation that happens concurrently with training every "
                f"{self.benchmark_config.validate_every_n_steps} steps"
            )

        return {"train": train_ds, "val": val_ds}

    def collate_fn(self, batch: Dict[str, Any]) -> Tuple[torch.Tensor, torch.Tensor]:
        """Collate batch of data into PyTorch tensors.

        Args:
            batch: Dictionary containing image and label data

        Returns:
            Tuple of (image_tensor, label_tensor) on the correct device
        """
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        device = ray.train.torch.get_device()
        batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

        return batch["image"], batch["label"]


class ImageClassificationJpegTorchDataLoaderFactory(
    TorchDataLoaderFactory, S3JpegReader
):
    """Factory for creating PyTorch DataLoaders for image classification tasks.

    This factory:
    1. Creates DataLoaders that read Jpeg files from S3
    2. Distributes files among Ray workers using round-robin allocation
    3. Handles device transfer and error handling for batches
    4. Supports row limits per worker for controlled data processing
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)
        S3JpegReader.__init__(self)  # Initialize S3JpegReader to set up _s3_client
        self.train_url = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        self.val_url = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]

    def calculate_rows_per_worker(
        self, total_rows: Optional[int], num_workers: int
    ) -> Optional[int]:
        """Calculate how many rows each worker should process.

        Args:
            total_rows: Total number of rows to process across all workers.
            num_workers: Total number of workers (Ray workers × Torch workers)

        Returns:
            Number of rows each worker should process, or None if no limit.
            If total_rows is less than num_workers, each worker will process at least 1 row.
        """
        if total_rows is None:
            return None

        if num_workers == 0:
            return total_rows

        return max(1, total_rows // num_workers)

    def get_iterable_datasets(self) -> Dict[str, IterableDataset]:
        """Get the train and validation datasets.

        Returns:
            A dictionary containing the train and validation datasets.
        """
        # Calculate row limits per worker for validation
        dataloader_config = self.get_dataloader_config()
        num_workers = max(1, dataloader_config.num_torch_workers)
        total_workers = self.benchmark_config.num_workers * num_workers

        limit_training_rows_per_worker = self.calculate_rows_per_worker(
            self.benchmark_config.limit_training_rows, total_workers
        )

        limit_validation_rows_per_worker = self.calculate_rows_per_worker(
            self.benchmark_config.limit_validation_rows, total_workers
        )

        # Calculate total rows to process
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

        # Get file URLs for training and validation
        train_file_urls = self._get_file_urls(self.train_url)
        train_ds = S3JpegImageIterableDataset(
            file_urls=train_file_urls,
            random_transforms=True,
            limit_rows_per_worker=limit_training_rows_per_worker,
        )

        # Report training dataset configuration
        ray.train.report(
            {
                "train_dataset": {
                    "file_urls": train_file_urls,
                    "random_transforms": True,
                    "limit_rows_per_worker": limit_training_rows_per_worker,
                    "total_rows": total_training_rows,
                    "worker_rank": ray.train.get_context().get_world_rank(),
                }
            }
        )

        # TODO: IMAGENET_JPEG_SPLIT_S3_DIRS["val"] does not have partitioning as "train" does. So we use "train" for validation.
        val_file_urls = train_file_urls
        val_ds = S3JpegImageIterableDataset(
            file_urls=val_file_urls,
            random_transforms=False,
            limit_rows_per_worker=limit_validation_rows_per_worker,
        )

        # Report validation dataset configuration
        ray.train.report(
            {
                "validation_dataset": {
                    "file_urls": val_file_urls,
                    "random_transforms": False,
                    "limit_rows_per_worker": limit_validation_rows_per_worker,
                    "total_rows": total_validation_rows,
                    "worker_rank": ray.train.get_context().get_world_rank(),
                }
            }
        )

        return {"train": train_ds, "val": val_ds}

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
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
            "Starting batch iteration"
        )

        try:
            last_batch_time = time.time()
            for batch_idx, batch in enumerate(dataloader):
                try:
                    # Check for delays between batches
                    current_time = time.time()
                    time_since_last_batch = current_time - last_batch_time
                    if time_since_last_batch > 10:
                        logger.warning(
                            f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                            f"Long delay ({time_since_last_batch:.2f}s) between batches "
                            f"{batch_idx-1} and {batch_idx}"
                        )

                    # Move batch to device
                    images, labels = batch
                    logger.info(
                        f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                        f"Processing batch {batch_idx} (shape: {images.shape}, "
                        f"time since last: {time_since_last_batch:.2f}s)"
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
                            f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                            f"Slow device transfer ({transfer_time:.2f}s) for batch {batch_idx}"
                        )

                    logger.info(
                        f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                        f"Completed device transfer for batch {batch_idx} in {transfer_time:.2f}s"
                    )

                    last_batch_time = time.time()
                    yield images, labels

                except Exception as e:
                    logger.error(
                        f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                        f"Error processing batch {batch_idx}: {str(e)}",
                        exc_info=True,
                    )
                    raise

        except Exception as e:
            logger.error(
                f"[ImageClassificationJpegTorchDataLoaderFactory] Worker {worker_rank}: "
                f"Error in batch iterator: {str(e)}",
                exc_info=True,
            )
            raise


class ImageClassificationJpegFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        data_factory_cls = {
            DataloaderType.MOCK: ImageClassificationMockDataLoaderFactory,
            DataloaderType.RAY_DATA: ImageClassificationJpegRayDataLoaderFactory,
            DataloaderType.TORCH: ImageClassificationJpegTorchDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
