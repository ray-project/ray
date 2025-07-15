from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import torch

from ray.data import DataIterator
from ray.train import Checkpoint
from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class TrainContext(ABC):
    """Base class for training context in Ray Train.

    This is the parent class that defines the core interface for training contexts.
    All concrete implementations should inherit from this class.
    """

    @abstractmethod
    def get_experiment_name(self) -> str:
        """Get the experiment name for the corresponding trial.

        Returns:
            The experiment name.
        """
        pass

    @abstractmethod
    def get_world_size(self) -> int:
        """Get the current world size (i.e. total number of workers) for this run.

        Returns:
            The world size.
        """
        pass

    @abstractmethod
    def get_world_rank(self) -> int:
        """Get the world rank of this worker.

        Returns:
            The world rank.
        """
        pass

    @abstractmethod
    def get_local_rank(self) -> int:
        """Get the local rank of this worker (rank of the worker on its node).

        Returns:
            The local rank.
        """
        pass

    @abstractmethod
    def get_local_world_size(self) -> int:
        """Get the local world size of this node (i.e. number of workers on this node).

        Returns:
            The local world size.
        """
        pass

    @abstractmethod
    def get_node_rank(self) -> int:
        """Get the rank of this node.

        Returns:
            The node rank.
        """
        pass

    @abstractmethod
    def get_storage(self):
        """Get the storage context for this training run.

        Returns:
            The storage context.
        """
        pass

    @abstractmethod
    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        """Get the dataset shard for this training run.

        Returns:
            The dataset shard.
        """
        pass

    @abstractmethod
    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
        checkpoint_dir_name: Optional[str] = None,
    ):
        """Report the metrics and checkpoint to the controller."""
        pass

    @abstractmethod
    def get_devices(self) -> List[torch.device]:
        pass
