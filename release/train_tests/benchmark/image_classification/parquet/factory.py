# Standard library imports
import logging
from typing import Dict, Optional

# Third-party imports
from torch.utils.data import IterableDataset
import ray
import ray.data
import ray.train

# Local imports
from constants import DatasetKey
from config import BenchmarkConfig
from image_classification.factory import (
    ImageClassificationRayDataLoaderFactory,
    ImageClassificationTorchDataLoaderFactory,
)
from .imagenet import get_preprocess_map_fn
from .parquet_iterable_dataset import S3ParquetImageIterableDataset
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

    def __init__(
        self, benchmark_config: BenchmarkConfig, data_dirs: Dict[str, str]
    ) -> None:
        super().__init__(benchmark_config)
        self._data_dirs = data_dirs

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
                self._data_dirs[DatasetKey.TRAIN],
                columns=["image", "label"],
            ).map(get_preprocess_map_fn(decode_image=True, random_transforms=True))
            # Add limit after map to enable operator fusion.
            .limit(self.get_dataloader_config().limit_training_rows)
        )

        # Create validation dataset without random transforms
        val_ds = (
            ray.data.read_parquet(
                self._data_dirs[DatasetKey.TRAIN],
                columns=["image", "label"],
            ).map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
            # Add limit after map to enable operator fusion.
            .limit(self.get_dataloader_config().limit_validation_rows)
        )

        return {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }


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

    def __init__(
        self, benchmark_config: BenchmarkConfig, data_dirs: Dict[str, str]
    ) -> None:
        """Initialize factory with benchmark configuration.

        Args:
            benchmark_config: Configuration for benchmark parameters
        """
        super().__init__(benchmark_config)
        S3ParquetReader.__init__(
            self
        )  # Initialize S3ParquetReader to set up _s3_client
        self.train_url = data_dirs[DatasetKey.TRAIN]
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

        self._cached_datasets = {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }
        return self._cached_datasets
