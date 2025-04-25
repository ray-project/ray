import logging
from typing import Dict, Iterator, Tuple

from ray.data.datasource.partitioning import Partitioning
import torch
import torchvision
from torch.utils.data import IterableDataset
import ray.data

from constants import DatasetKey
from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import BaseDataLoaderFactory
from torch_dataloader_factory import TorchDataLoaderFactory
from image_classification.factory import (
    ImageClassificationMockDataLoaderFactory,
    ImageClassificationRayDataLoaderFactory,
)
from image_classification.imagenet import get_transform
from logger_utils import ContextLoggerAdapter
from image_classification.image_classification_jpeg.imagenet import (
    get_preprocess_map_fn,
)

logger = ContextLoggerAdapter(logging.getLogger(__name__))


# Use `download_input_data_from_s3.sh` to download the dataset
LOCALFS_JPEG_SPLIT_DIRS = {
    DatasetKey.TRAIN: "/mnt/local_storage/imagenet/train/",
    DatasetKey.VALID: "/mnt/local_storage/imagenet/val/",
}


class LocalFSImageClassificationRayDataLoaderFactory(
    ImageClassificationRayDataLoaderFactory
):
    """Factory for creating Ray DataLoader for local JPEG image classification."""

    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        """Get Ray datasets for training and validation from local filesystem."""
        # Create training dataset
        train_ds = ray.data.read_images(
            LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.TRAIN],
            mode="RGB",
            include_paths=True,
            partitioning=Partitioning(
                "dir",
                base_dir=LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.TRAIN],
                field_names=["class"],
            ),
        ).map(get_preprocess_map_fn(random_transforms=True))

        # Create validation dataset
        val_ds = ray.data.read_images(
            LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.VALID],
            mode="RGB",
            include_paths=True,
            partitioning=Partitioning(
                "dir",
                base_dir=LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.VALID],
                field_names=["class"],
            ),
        ).map(get_preprocess_map_fn(random_transforms=False))

        return {
            DatasetKey.TRAIN: train_ds,
            DatasetKey.VALID: val_ds,
        }


class LocalFSImageClassificationTorchDataLoaderFactory(TorchDataLoaderFactory):
    """Factory for creating PyTorch DataLoaders for local JPEG image classification.

    Uses torchvision.datasets.ImageFolder for efficient local filesystem access.
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)
        # Use the same transforms as the Ray Data implementation
        self.train_transform = get_transform(
            to_torch_tensor=True, random_transforms=True
        )
        self.val_transform = get_transform(
            to_torch_tensor=True, random_transforms=False
        )

    def create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a safe iterator that handles device transfer and error handling."""
        non_blocking = self.get_dataloader_config().torch_non_blocking
        for batch in dataloader:
            try:
                images, labels = batch
                images = images.to(device, non_blocking=non_blocking)
                labels = labels.to(device, non_blocking=non_blocking)
                yield images, labels
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                raise

    def get_iterable_datasets(self) -> Dict[str, IterableDataset]:
        """Get the train and validation datasets."""
        train_dataset = torchvision.datasets.ImageFolder(
            root=LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.TRAIN],
            transform=self.train_transform,
        )

        val_dataset = torchvision.datasets.ImageFolder(
            root=LOCALFS_JPEG_SPLIT_DIRS[DatasetKey.VALID],
            transform=self.val_transform,
        )

        return {
            DatasetKey.TRAIN: train_dataset,
            DatasetKey.VALID: val_dataset,
        }


class LocalFSImageClassificationFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        data_factory_cls = {
            DataloaderType.MOCK: ImageClassificationMockDataLoaderFactory,
            DataloaderType.RAY_DATA: LocalFSImageClassificationRayDataLoaderFactory,
            DataloaderType.TORCH: LocalFSImageClassificationTorchDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
