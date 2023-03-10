import logging
from pytorch_lightning.utilities.types import EVAL_DATALOADERS, TRAIN_DATALOADERS
import torch
from typing import Any, Dict, Optional

import pytorch_lightning as pl
from pytorch_lightning.utilities import rank_zero_only
from pytorch_lightning.strategies import DDPStrategy
from pytorch_lightning.plugins.environments import LightningEnvironment

import ray
from ray.air import session

from torch.utils.data import IterableDataset, DataLoader
from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)


class RayDDPStrategy(DDPStrategy):
    """Subclass of DDPStrategy to ensure compatibility with Ray orchestration."""

    @property
    def root_device(self) -> torch.device:
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )


class RayEnvironment(LightningEnvironment):
    """Setup Lightning DDP training environment for Ray cluster."""

    def world_size(self) -> int:
        return session.get_world_size()

    def global_rank(self) -> int:
        return session.get_world_rank()

    def local_rank(self) -> int:
        return session.get_local_rank()

    def node_rank(self) -> int:
        return session.get_node_rank()

    def set_world_size(self, size: int) -> None:
        self._world_size = session.get_world_size()

    def set_global_rank(self, rank: int) -> None:
        self._global_rank = session.get_world_rank()
        rank_zero_only.rank = rank

    def teardown(self):
        pass


class RayIterableDataset(IterableDataset):
    def __init__(self, dataset: "Dataset", config: Dict[str, Any]) -> None:
        super().__init__()
        self.dataset = dataset
        self.config = config

    def __iter__(self):
        return self.dataset.iter_torch_batches(**self.config)


class RayDataModule(pl.LightningDataModule):
    def __init__(
        self,
        dataset_iter_config: Dict[str, Any],
        train_dataset: "Dataset",
        val_dataset: Optional["Dataset"] = None,
    ) -> None:
        super().__init__()

        if not dataset_iter_config:
            raise RuntimeError(
                "To use Ray Datasets with LightningTrainer, you must specify "
                " `datasets_iter_config` in LightningConfig!"
            )

        def _train_dataloader() -> TRAIN_DATALOADERS:
            assert train_dataset
            ds = RayIterableDataset(train_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        def _val_dataloader() -> EVAL_DATALOADERS:
            assert val_dataset
            ds = RayIterableDataset(val_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        if train_dataset:
            self.train_dataloader = _train_dataloader

        if val_dataset:
            self.val_dataloader = _val_dataloader
