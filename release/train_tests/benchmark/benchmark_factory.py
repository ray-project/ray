from abc import ABC, abstractmethod

from config import BenchmarkConfig
from dataloader_factory import BaseDataLoaderFactory


class BenchmarkFactory(ABC):
    def __init__(self, benchmark_config: BenchmarkConfig):
        self.benchmark_config = benchmark_config
        self.dataloader_factory = self.get_dataloader_factory()
        self.dataset_creation_time = 0

    @abstractmethod
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        """Create the appropriate dataloader factory for this benchmark."""
        raise NotImplementedError

    # TODO: These can probably be moved to the train loop runner,
    # since xgboost does not require instantiating the model
    # and loss function in this way.
    @abstractmethod
    def get_model(self):
        raise NotImplementedError

    @abstractmethod
    def get_loss_fn(self):
        raise NotImplementedError

    def get_train_dataloader(self):
        return self.dataloader_factory.get_train_dataloader()

    def get_val_dataloader(self):
        return self.dataloader_factory.get_val_dataloader()

    def get_dataloader_metrics(self):
        return self.dataloader_factory.get_metrics()
