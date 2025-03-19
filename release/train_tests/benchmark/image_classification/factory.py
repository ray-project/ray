from typing import Dict
import logging

import torch
import torchvision

import ray.train
from ray.data import Dataset

from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import (
    RayDataLoaderFactory,
    BaseDataLoaderFactory,
    TorchDataLoaderFactory,
)
from image_classification.imagenet import (
    get_preprocess_map_fn,
    IMAGENET_PARQUET_SPLIT_S3_DIRS,
)


logger = logging.getLogger(__name__)


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationMockDataLoaderFactory(BaseDataLoaderFactory):
    def get_train_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=1024, batch_size=dataloader_config.train_batch_size
        )

    def get_val_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=512, batch_size=dataloader_config.validation_batch_size
        )


class ImageClassificationRayDataLoaderFactory(RayDataLoaderFactory):
    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        train_ds = ray.data.read_parquet(
            IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
        ).map(get_preprocess_map_fn(decode_image=True, random_transforms=True))

        val_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(self.benchmark_config.limit_validation_rows)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
        )

        if self.benchmark_config.validate_every_n_steps > 0:
            # TODO: This runs really slowly and needs to be tuned.
            # Maybe move this to the RayDataLoaderFactory.
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
                f"[Dataloader] Reserving {cpus_to_exclude} CPUs for validation "
                "that happens concurrently with training every "
                f"{self.benchmark_config.validate_every_n_steps} steps. "
            )

        return {"train": train_ds, "val": val_ds}

    def collate_fn(self, batch):
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        device = ray.train.torch.get_device()
        batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

        return batch["image"], batch["label"]


class ImageClassificationTorchDataLoaderFactory(TorchDataLoaderFactory):
    """Factory for creating PyTorch DataLoaders for image classification tasks."""

    def __init__(self, benchmark_config: BenchmarkConfig):
        train_urls = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]
        val_urls = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]
        super().__init__(
            benchmark_config,
            train_urls,
            val_urls,
            limit_total_rows=benchmark_config.limit_validation_rows,
        )


class ImageClassificationFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        data_factory_cls = {
            DataloaderType.MOCK: ImageClassificationMockDataLoaderFactory,
            DataloaderType.RAY_DATA: ImageClassificationRayDataLoaderFactory,
            DataloaderType.TORCH: ImageClassificationTorchDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
