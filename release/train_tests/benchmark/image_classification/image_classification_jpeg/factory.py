# Standard library imports
import logging
from typing import Dict

# Third-party imports
import torch
import torchvision
from torch.utils.data import IterableDataset
import pyarrow.fs

# Ray imports
import ray.train
from ray.data.datasource.partitioning import Partitioning

# Local imports
from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import BaseDataLoaderFactory
from image_classification.factory import (
    ImageClassificationRayDataLoaderFactory,
    ImageClassificationTorchDataLoaderFactory,
    ImageClassificationMockDataLoaderFactory,
)
from s3_reader import AWS_REGION
from .imagenet import get_preprocess_map_fn, IMAGENET_JPEG_SPLIT_S3_DIRS
from .torch_jpeg_image_iterable_dataset import S3JpegImageIterableDataset
from s3_jpeg_reader import S3JpegReader
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


class ImageClassificationJpegRayDataLoaderFactory(
    ImageClassificationRayDataLoaderFactory
):
    """Factory for creating Ray DataLoader for JPEG image classification.

    Extends ImageClassificationRayDataLoaderFactory to provide:
    1. S3 filesystem configuration with boto credentials
    2. Ray dataset creation with partitioning by class
    3. Resource allocation for concurrent validation
    4. Image preprocessing with optional random transforms
    """

    def get_s3fs_with_boto_creds(
        self, connection_timeout: int = 60, request_timeout: int = 60
    ) -> pyarrow.fs.S3FileSystem:
        """Create S3 filesystem with boto credentials.

        Args:
            connection_timeout: Timeout for establishing connection in seconds
            request_timeout: Timeout for requests in seconds

        Returns:
            Configured S3FileSystem instance with boto credentials
        """
        import boto3

        credentials = boto3.Session().get_credentials()

        s3fs = pyarrow.fs.S3FileSystem(
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

        Creates training and validation datasets with:
        1. Partitioning by class for efficient data loading
        2. Image preprocessing with optional random transforms
        3. Resource allocation for concurrent validation
        4. Row limits based on benchmark configuration

        Returns:
            Dictionary containing:
                - "train": Training dataset with random transforms
                - "val": Validation dataset without transforms
        """
        # Configure S3 filesystem connection
        s3fs = self.get_s3fs_with_boto_creds()

        # Create training dataset with class-based partitioning
        train_pattern = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        train_partitioning = Partitioning(
            "dir", base_dir=train_pattern, field_names=["class"]
        )
        train_ds = (
            ray.data.read_images(
                train_pattern,
                mode="RGB",
                include_paths=False,
                partitioning=train_partitioning,
                filesystem=s3fs,
            )
            .limit(self.benchmark_config.limit_training_rows)
            .map(get_preprocess_map_fn(random_transforms=True))
        )

        # Create validation dataset with same partitioning
        val_pattern = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        val_partitioning = Partitioning(
            "dir", base_dir=val_pattern, field_names=["class"]
        )
        val_ds = (
            ray.data.read_images(
                val_pattern,
                mode="RGB",
                include_paths=False,
                partitioning=val_partitioning,
                filesystem=s3fs,
            )
            .limit(self.benchmark_config.limit_validation_rows)
            .map(get_preprocess_map_fn(random_transforms=False))
        )

        return {"train": train_ds, "val": val_ds}


class ImageClassificationJpegTorchDataLoaderFactory(
    ImageClassificationTorchDataLoaderFactory, S3JpegReader
):
    """Factory for creating PyTorch DataLoaders for JPEG image classification.

    Features:
    - S3-based JPEG file reading with round-robin worker distribution
    - Device transfer and error handling for data batches
    - Row limits per worker for controlled processing
    - Dataset caching for efficiency
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)
        S3JpegReader.__init__(self)  # Initialize S3JpegReader to set up _s3_client
        self.train_url = IMAGENET_JPEG_SPLIT_S3_DIRS["train"]
        self._cached_datasets = None

    def get_iterable_datasets(self) -> Dict[str, IterableDataset]:
        """Get train and validation datasets with worker-specific configurations.

        Returns:
            Dictionary containing:
                - "train": Training dataset with random transforms
                - "val": Validation dataset without transforms
        """
        if self._cached_datasets is not None:
            return self._cached_datasets

        # Get row limits for workers and total processing
        (
            limit_training_rows_per_worker,
            limit_validation_rows_per_worker,
        ) = self._get_worker_row_limits()
        total_training_rows, total_validation_rows = self._get_total_row_limits()

        # Get file URLs for training and validation
        train_file_urls = val_file_urls = self._get_file_urls(self.train_url)
        train_ds = S3JpegImageIterableDataset(
            file_urls=train_file_urls,
            random_transforms=True,
            limit_rows_per_worker=limit_training_rows_per_worker,
        )

        # TODO: IMAGENET_JPEG_SPLIT_S3_DIRS["val"] does not have partitioning as "train" does. So we use "train" for validation.
        val_ds = S3JpegImageIterableDataset(
            file_urls=val_file_urls,
            random_transforms=False,
            limit_rows_per_worker=limit_validation_rows_per_worker,
        )

        self._cached_datasets = {"train": train_ds, "val": val_ds}
        return self._cached_datasets


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
