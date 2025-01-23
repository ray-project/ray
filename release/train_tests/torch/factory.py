from typing import Dict

import ray.data

from config import BenchmarkConfig


class BenchmarkFactory:
    def __init__(self, benchmark_config: BenchmarkConfig):
        self.benchmark_config = benchmark_config

    def get_model(self):  # -> nn.Module
        pass

    def get_train_dataloader(self):  # -> "IterableDataset"
        pass

    def get_val_dataloader(self):  # -> "IterableDataset"
        pass

    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        """Returns a dict of Ray Datasets if the dataloader type is RAY_DATA"""
        pass

    def get_loss_fn(self):
        pass
