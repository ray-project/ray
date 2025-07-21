# Standard library imports
import logging
from typing import Dict

# Third-party imports
import torchvision
from torch.utils.data import IterableDataset
import pyarrow.fs

# Ray imports
import ray.train
from ray.data.datasource.partitioning import Partitioning

# Local imports
from constants import DatasetKey
from config import BenchmarkConfig
from image_classification.factory import (
    ImageClassificationRayDataLoaderFactory,
    ImageClassificationTorchDataLoaderFactory,
)
from image_classification.imagenet import get_transform
from s3_reader import AWS_REGION
from .imagenet import get_preprocess_map_fn
from .jpeg_iterable_dataset import S3JpegImageIterableDataset
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

    def __init__(self, benchmark_config: BenchmarkConfig, dataset_dirs: Dict[str, str]):
        super().__init__(benchmark_config)
        self._dataset_dirs = dataset_dirs

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
        train_dir = self._dataset_dirs[DatasetKey.TRAIN]
        # TODO: The validation dataset directory is not partitioned by class.
        val_dir = train_dir

        filesystem = (
            self.get_s3fs_with_boto_creds() if train_dir.startswith("s3://") else None
        )

        # Create training dataset with class-based partitioning
        train_partitioning = Partitioning(
            "dir", base_dir=train_dir, field_names=["class"]
        )
        train_ds = (
            ray.data.read_images(
                train_dir,
                mode="RGB",
                include_paths=False,
                partitioning=train_partitioning,
                filesystem=filesystem,
            ).map(get_preprocess_map_fn(random_transforms=True))
            # Add limit after map to enable operator fusion.
            .limit(self.get_dataloader_config().limit_training_rows)
        )

        # Create validation dataset with same partitioning
        val_partitioning = Partitioning("dir", base_dir=val_dir, field_names=["class"])
        val_ds = (
            ray.data.read_images(
                val_dir,
                mode="RGB",
                include_paths=False,
                partitioning=val_partitioning,
                filesystem=filesystem,
            ).map(get_preprocess_map_fn(random_transforms=False))
            # Add limit after map to enable operator fusion.
            .limit(self.get_dataloader_config().limit_validation_rows)
        )

        return {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }


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

    def __init__(self, benchmark_config: BenchmarkConfig, data_dirs: Dict[str, str]):
        super().__init__(benchmark_config)
        S3JpegReader.__init__(self)  # Initialize S3JpegReader to set up _s3_client
        self._data_dirs = data_dirs
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

        if self._data_dirs[DatasetKey.TRAIN].startswith("s3://"):
            return self._get_iterable_datasets_s3()
        else:
            return self._get_iterable_datasets_local()

    def _get_iterable_datasets_local(self) -> Dict[str, IterableDataset]:
        """Get train and validation datasets from local filesystem."""
        train_dir = self._data_dirs[DatasetKey.TRAIN]
        val_dir = self._data_dirs[DatasetKey.VALID]

        train_dataset = torchvision.datasets.ImageFolder(
            root=train_dir,
            transform=get_transform(to_torch_tensor=True, random_transforms=True),
        )

        val_dataset = torchvision.datasets.ImageFolder(
            root=val_dir,
            transform=get_transform(to_torch_tensor=True, random_transforms=False),
        )

        return {
            DatasetKey.TRAIN: train_dataset,
            DatasetKey.VALID: val_dataset,
        }

    def _get_iterable_datasets_s3(self) -> Dict[str, IterableDataset]:
        """Get train and validation datasets from S3."""

        train_dir = self._data_dirs[DatasetKey.TRAIN]

        # Get row limits for workers and total processing
        (
            limit_training_rows_per_worker,
            limit_validation_rows_per_worker,
        ) = self._get_worker_row_limits()

        # Get file URLs for training and validation
        train_file_urls = val_file_urls = self._get_file_urls(train_dir)
        train_ds = S3JpegImageIterableDataset(
            file_urls=train_file_urls,
            random_transforms=True,
            limit_rows_per_worker=limit_training_rows_per_worker,
        )

        # TODO: IMAGENET_JPEG_SPLIT_S3_DIRS["val"] does not have the label
        # partitioning like "train" does. So we use "train" for validation.
        val_ds = S3JpegImageIterableDataset(
            file_urls=val_file_urls,
            random_transforms=False,
            limit_rows_per_worker=limit_validation_rows_per_worker,
        )

        self._cached_datasets = {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }
        return self._cached_datasets
