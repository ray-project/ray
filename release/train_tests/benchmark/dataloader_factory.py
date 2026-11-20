from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Tuple
import logging

import torch

from config import BenchmarkConfig, DataLoaderConfig

logger = logging.getLogger(__name__)


class BaseDataLoaderFactory(ABC):
    """Base class for creating and managing dataloaders."""

    def __init__(self, benchmark_config: BenchmarkConfig):
        self.benchmark_config = benchmark_config

    def get_dataloader_config(self) -> DataLoaderConfig:
        return self.benchmark_config.dataloader_config

    @abstractmethod
    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    @abstractmethod
    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Return metrics about dataloader performance."""
        return {}

    def supports_mid_epoch_resume(self) -> bool:
        """Whether checkpoints save and restore the train data iterator state."""
        return False

    def get_train_data_state(self) -> Optional[Dict[str, Any]]:
        """Return the train data iterator state to save in a checkpoint.

        Call it on every rank at the same step, since each rank saves its
        own part of the state.
        """
        return None

    def set_train_data_state(self, state_dict: Dict[str, Any]) -> None:
        """Set the train data iterator state to restore from."""
        raise NotImplementedError
