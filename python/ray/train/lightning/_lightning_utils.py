import os
import logging
import torch
from torch import Tensor
from copy import deepcopy
from typing import Any, Dict, Optional

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.utilities.types import STEP_OUTPUT
from pytorch_lightning.strategies import DDPStrategy
from pytorch_lightning.plugins.environments import LightningEnvironment
import ray
from ray.air import session
from ray.air.constants import MODEL_KEY
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint

from torch.utils.data import IterableDataset, DataLoader
from ray.data.dataset import DatasetIterator

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
        logger.warning("world_size setter is disabled in AIR LightningTrainer.")
        pass

    def set_global_rank(self, rank: int) -> None:
        logger.warning("global_rank setter is disabled in AIR LightningTrainer.")
        pass

    def teardown(self):
        pass


class RayIterableDataset(IterableDataset):
    def __init__(self, dataset: "DatasetIterator", config: Dict[str, Any]) -> None:
        super().__init__()
        self.dataset = dataset
        self.config = config

    def __iter__(self):
        return self.dataset.iter_torch_batches(**self.config)


class RayDataModule(pl.LightningDataModule):
    def __init__(
        self,
        dataset_iter_config: Dict[str, Any],
        train_dataset: "DatasetIterator",
        val_dataset: Optional["DatasetIterator"] = None,
    ) -> None:
        super().__init__()

        def _train_dataloader() -> DataLoader:
            assert train_dataset
            ds = RayIterableDataset(train_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        def _val_dataloader() -> DataLoader:
            assert val_dataset
            ds = RayIterableDataset(val_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        if train_dataset:
            self.train_dataloader = _train_dataloader

        # ``pl.Trainer`` checks if the val_dataloader method has been overridden
        # to determine whether to enable the validation loop. To align with this
        # setting, we only override this method when `val_dataset` is not `None`.
        if val_dataset:
            self.val_dataloader = _val_dataloader


class RayModelCheckpoint(ModelCheckpoint):
    """
    AIR customized ModelCheckpoint callback.

    A subclass of ``pytorch_lightning.callbacks.ModelCheckpoint``.
    This callback function reports the latest metrics to the AIR session and
    creates an AIR checkpoint whenever a lightning checkpoint is saved.
    """

    def setup(self, *args, **kwargs) -> None:
        super().setup(*args, **kwargs)
        self.last_best_k_models = {}
        self.last_best_model_path = None

    def format_checkpoint_name(
        self,
        metrics: Dict[str, Tensor],
        filename: Optional[str] = None,
        ver: Optional[int] = None,
    ) -> str:
        """
        Change checkpoint files path to align with AIR checkpoint format.

        e.g. './epoch=2-loss=0.12.ckpt' -> './epoch=2-loss=0.12.ckpt/model'
        """
        filepath = super().format_checkpoint_name(metrics, filename, ver)
        return f"{filepath}/{MODEL_KEY}"

    def _session_report(self, trainer: "pl.Trainer"):
        """Report latest metrics dict and checkpoint to AIR training session."""
        kwargs = {}

        # Report latest logged metrics
        metrics = self._monitor_candidates(trainer)
        for k, v in metrics.items():
            if isinstance(v, torch.Tensor):
                metrics[k] = v.cpu().numpy()
        kwargs["metrics"] = metrics

        filepath = None
        if self.monitor:
            # Capture metric-based top-k checkpoint
            new_checkpoint = self.best_k_models.keys() - self.last_best_k_models.keys()
            if new_checkpoint:
                filepath = new_checkpoint.pop()
        else:
            # Capture frequency-based checkpoint
            if self.last_best_model_path != self.best_model_path:
                filepath = self.best_model_path

        # Report latest saved checkpoint
        # Note that AIR only takes the checkpoint of rank 0.
        # Just save a dummy checkpoint on the other workers.
        if filepath:
            if trainer.global_rank == 0:
                kwargs["checkpoint"] = LightningCheckpoint.from_directory(
                    path=os.path.dirname(filepath)
                )
            else:
                kwargs["checkpoint"] = LightningCheckpoint.from_dict(
                    {"rank": session.get_world_rank()}
                )

        self.last_best_k_models = deepcopy(self.best_k_models)
        self.last_best_model_path = self.best_model_path
        session.report(**kwargs)

    def on_train_batch_end(
        self,
        trainer: "pl.Trainer",
        pl_module: "pl.LightningModule",
        outputs: STEP_OUTPUT,
        batch: Any,
        batch_idx: int,
    ) -> None:
        super().on_train_batch_end(trainer, pl_module, outputs, batch, batch_idx)
        self._session_report(trainer=trainer)

    def on_train_epoch_end(
        self, trainer: "pl.Trainer", pl_module: "pl.LightningModule"
    ) -> None:
        super().on_train_epoch_end(trainer, pl_module)
        self._session_report(trainer=trainer)

    def on_validation_end(
        self, trainer: "pl.Trainer", pl_module: "pl.LightningModule"
    ) -> None:
        super().on_validation_end(trainer, pl_module)
        self._session_report(trainer=trainer)

