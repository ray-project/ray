# Standard library imports
import logging
from typing import Dict

# Third-party imports
import ray.data

# Local imports
from constants import DatasetKey
from config import BenchmarkConfig
from image_classification.factory import ImageClassificationRayDataLoaderFactory
from .imagenet import (
    create_s3_url_dataset,
)

logger = logging.getLogger(__name__)


class ImageClassificationS3UrlRayDataLoaderFactory(
    ImageClassificationRayDataLoaderFactory
):
    """Factory for creating Ray DataLoader that downloads images from S3 URLs.

    This factory:
    1. Lists JPEG files from S3 using boto3
    2. Creates a Ray dataset from the file records
    3. Uses map_batches to download and process images from S3

    This approach separates file listing from image downloading, which can be
    more efficient for certain workloads as it allows parallel downloads during
    the map_batches execution on CPU workers.
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
        dataloader_config = self.get_dataloader_config()

        # Create training dataset
        train_limit = (
            dataloader_config.limit_training_rows
            if dataloader_config.limit_training_rows > 0
            else None
        )
        train_ds = create_s3_url_dataset(
            data_dir=self._data_dirs[DatasetKey.TRAIN],
            random_transforms=True,
            limit_rows=train_limit,
        )

        # Create validation dataset
        val_limit = (
            dataloader_config.limit_validation_rows
            if dataloader_config.limit_validation_rows > 0
            else None
        )
        val_ds = create_s3_url_dataset(
            data_dir=self._data_dirs[DatasetKey.TRAIN],
            random_transforms=False,
            limit_rows=val_limit,
        )

        return {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }
