# Standard library imports
import logging
from typing import Dict, Optional, Type

# Third-party imports
import torch
import torchvision
from torch.utils.data import IterableDataset
import ray
import ray.data
import ray.train

# Local imports
from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import BaseDataLoaderFactory
from image_classification.factory import (
    ImageClassificationRayDataLoaderFactory,
    ImageClassificationTorchDataLoaderFactory,
    ImageClassificationMockDataLoaderFactory,
)
from .imagenet import IMAGENET_PARQUET_SPLIT_S3_DIRS, get_preprocess_map_fn
from .torch_parquet_image_iterable_dataset import S3ParquetImageIterableDataset
from s3_parquet_reader import S3ParquetReader

logger = logging.getLogger(__name__)


class ImageClassificationParquetRayDataLoaderFactory(
    ImageClassificationRayDataLoaderFactory
):
    """Factory for creating Ray DataLoader for Parquet image classification.

    Features:
    - Parquet file reading with column selection
    - Image decoding and preprocessing
    - Resource allocation for concurrent validation
    - Row limits based on benchmark configuration
    """

    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        """Get Ray datasets for training and validation.

        Returns:
            Dictionary containing:
                - "train": Training dataset with random transforms
                - "val": Validation dataset without transforms
        """
        # Create training dataset with image decoding and transforms
        train_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(self.benchmark_config.limit_training_rows)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=True))
        )

        # Create validation dataset without random transforms
        val_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(self.benchmark_config.limit_validation_rows)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
        )

        return {"train": train_ds, "val": val_ds}


class ImageClassificationParquetTorchDataLoaderFactory(
    ImageClassificationTorchDataLoaderFactory, S3ParquetReader
):
    """Factory for creating PyTorch DataLoaders for Parquet image classification.

    Features:
    - Parquet file reading with row count-based distribution
    - Worker-based file distribution for balanced workloads
    - Row limits per worker for controlled processing
    - Dataset instance caching for efficiency
    """

    def __init__(self, benchmark_config: BenchmarkConfig) -> None:
        """Initialize factory with benchmark configuration.

        Args:
            benchmark_config: Configuration for benchmark parameters
        """
        super().__init__(benchmark_config)
        S3ParquetReader.__init__(
            self
        )  # Initialize S3ParquetReader to set up _s3_client
        self.train_url = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]
        self._cached_datasets: Optional[Dict[str, IterableDataset]] = None

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

        # Create training dataset
        train_file_urls = self._get_file_urls(self.train_url)
        train_ds = S3ParquetImageIterableDataset(
            file_urls=train_file_urls,
            random_transforms=True,
            limit_rows_per_worker=limit_training_rows_per_worker,
        )

        # Create validation dataset
        val_file_urls = train_file_urls
        val_ds = S3ParquetImageIterableDataset(
            file_urls=val_file_urls,
            random_transforms=False,
            limit_rows_per_worker=limit_validation_rows_per_worker,
        )

        self._cached_datasets = {"train": train_ds, "val": val_ds}
        return self._cached_datasets


class ImageClassificationParquetFactory(BenchmarkFactory):
    """Factory for creating Parquet-based image classification components.

    Features:
    - Support for mock, Ray, and PyTorch dataloaders
    - ResNet50 model initialization
    - Cross-entropy loss function
    """

    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        """Get appropriate dataloader factory based on configuration.

        Returns:
            Factory instance for the configured dataloader type
        """
        data_factory_cls: Type[BaseDataLoaderFactory] = {
            DataloaderType.MOCK: ImageClassificationMockDataLoaderFactory,
            DataloaderType.RAY_DATA: ImageClassificationParquetRayDataLoaderFactory,
            DataloaderType.TORCH: ImageClassificationParquetTorchDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        """Get ResNet50 model for image classification.

        Returns:
            ResNet50 model without pretrained weights
        """
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        """Get cross-entropy loss function.

        Returns:
            CrossEntropyLoss module for training
        """
        return torch.nn.CrossEntropyLoss()
