from typing import Dict

import torch

import ray.data

from config import BenchmarkConfig


class BenchmarkFactory:
    def __init__(self, benchmark_config: BenchmarkConfig):
        self.benchmark_config = benchmark_config

    def get_model(self) -> torch.nn.Module:
        raise NotImplementedError

    def get_train_dataloader(self):  # -> "IterableDataset"
        raise NotImplementedError

    def get_val_dataloader(self):  # -> "IterableDataset"
        raise NotImplementedError

    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        """Returns a dict of Ray Datasets if the dataloader type is RAY_DATA"""
        raise NotImplementedError

    def get_loss_fn(self):
        raise NotImplementedError

    def get_dataloader_metrics(self) -> Dict:
        raise NotImplementedError
