from typing import Dict

import torch
import torchvision

import ray
import ray.data

from config import DataloaderType
from factory import BenchmarkFactory
from dataloader_factory import RayDataLoaderFactory, BaseDataLoaderFactory
from image_classification.imagenet import (
    get_preprocess_map_fn,
    IMAGENET_PARQUET_SPLIT_S3_DIRS,
)


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationMockDataLoaderFactory(BaseDataLoaderFactory):
    def get_train_dataloader(self, batch_size: int):
        return mock_dataloader(num_batches=1024, batch_size=batch_size)

    def get_val_dataloader(self, batch_size: int):
        return mock_dataloader(num_batches=512, batch_size=batch_size)


class ImageClassificationRayDataLoaderFactory(RayDataLoaderFactory):
    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        train_ds = ray.data.read_parquet(
            IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
        ).map(get_preprocess_map_fn(decode_image=True, random_transforms=True))

        val_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(50000)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
        )

        return {"train": train_ds, "val": val_ds}

    def collate_fn(self, batch):
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        device = ray.train.torch.get_device()
        batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

        return batch["image"], batch["label"]


class ImageClassificationFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        if self.benchmark_config.dataloader_type == DataloaderType.MOCK:
            return ImageClassificationMockDataLoaderFactory()
        elif self.benchmark_config.dataloader_type == DataloaderType.RAY_DATA:
            return ImageClassificationRayDataLoaderFactory()
        else:
            raise ValueError(
                f"Invalid dataloader type: {self.benchmark_config.dataloader_type}"
            )

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
