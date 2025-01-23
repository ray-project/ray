import torch

import ray.train

from factory import BenchmarkFactory
from config import DataloaderType


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationFactory(BenchmarkFactory):
    def get_model(self):
        if self.benchmark_config.model_name == "resnet50":
            from torchvision.models import resnet50

            return resnet50(weights=None)
        else:
            raise ValueError(f"Model {self.benchmark_config.model_name} not supported")

    def get_train_dataloader(self):
        if self.benchmark_config.dataloader_type == DataloaderType.RAY_DATA:
            ds_iterator = ray.train.get_dataset_shard("train")
            # TODO: configure this
            return ds_iterator.iter_torch_batches(batch_size=32)
        elif self.benchmark_config.dataloader_type == DataloaderType.MOCK:
            return mock_dataloader(num_batches=64, batch_size=32)
        else:
            raise ValueError(
                f"Dataloader type {self.benchmark_config.dataloader_type} not supported"
            )

    def get_val_dataloader(self):
        if self.benchmark_config.dataloader_type == DataloaderType.RAY_DATA:
            ds_iterator = ray.train.get_dataset_shard("val")
            return ds_iterator.iter_torch_batches(batch_size=32)
        elif self.benchmark_config.dataloader_type == DataloaderType.MOCK:
            return mock_dataloader(num_batches=16, batch_size=32)
        else:
            raise ValueError(
                f"Dataloader type {self.benchmark_config.dataloader_type} not supported"
            )

    def get_ray_datasets(self):
        if self.benchmark_config.dataloader_type != DataloaderType.RAY_DATA:
            return {}

        # TODO: for imagenet
        train_ds = ray.data.read_parquet()
        return {"train": train_ds}

    def get_loss_fn(self):
        return torch.nn.CrossEntropyLoss()
