import os
import logging
import torch
from torch import Tensor
from typing import Any, Dict, Optional
from copy import deepcopy

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.utilities import rank_zero_info, rank_zero_only
from pytorch_lightning.utilities.types import STEP_OUTPUT
from pytorch_lightning.strategies import DDPStrategy
from lightning_fabric.plugins.environments.lightning import LightningEnvironment

import ray
from ray.air import session
from torch.utils.data import IterableDataset, DataLoader
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

class RayModelCheckpoint(ModelCheckpoint):
    """
    AIR customized ModelCheckpoint callback.
    
    A subclass of ``pytorch_lightning.callbacks.ModelCheckpoint``.
    This callback function reports the latest metrics to the AIR session and 
    creates an AIR checkpoint when a lightning checkpoint is saved.
    """

    def setup(self, *args, **kwargs) -> None:
        super().setup(*args, **kwargs)
        self.last_best_k_models = {}

    def format_checkpoint_name(
        self, metrics: Dict[str, Tensor], filename: Optional[str] = None, ver: Optional[int] = None
    ) -> str:
        """
        Ensure different checkpoint files saved in seperate folders to align with AIR checkpoint format.

        e.g. './epoch=2-validation_loss=0.12.ckpt' -> './epoch=2-validation_loss=0.12/checkpoint.ckpt'
        """
        filepath = super().format_checkpoint_name(metrics, filename, ver)
        filepath = filepath.replace(self.FILE_EXTENSION, f"/checkpoint{self.FILE_EXTENSION}")
        return filepath

    def _session_report(self, trainer: "pl.Trainer"):
        """Report latest metrics dict and checkpoint to AIR training session."""
        kwargs = {}

        # Report latest logged metrics
        metrics = self._monitor_candidates(trainer)
        for k, v in metrics.items():
            if isinstance(v, torch.Tensor):
                metrics[k] = v.cpu().numpy()
        kwargs["metrics"] = metrics

        # Report updated checkpoint
        new_checkpoint = self.best_k_models.keys() - self.last_best_k_models.keys()
        if len(new_checkpoint) == 1:
            filepath = new_checkpoint.pop()

            # AIR only takes the checkpoint of rank 0. Report a dummy checkpoint on other workers.
            if trainer.global_rank == 0:
                kwargs["checkpoint"] = LightningCheckpoint.from_directory(path=os.path.dirname(filepath))
            else:
                kwargs["checkpoint"] = LightningCheckpoint.from_dict({"rank": session.get_world_rank()})

        self.last_best_k_models = deepcopy(self.best_k_models)
        session.report(**kwargs)

    def on_train_batch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule", outputs: STEP_OUTPUT, batch: Any, batch_idx: int) -> None:
        super().on_train_batch_end(trainer, pl_module, outputs, batch, batch_idx)
        self._session_report(trainer=trainer)

    def on_train_epoch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule") -> None:
        super().on_train_epoch_end(trainer, pl_module)
        self._session_report(trainer=trainer)
    
    def on_validation_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule") -> None:
        super().on_validation_end(trainer, pl_module)
        self._session_report(trainer=trainer)

class RayDDPStrategy(DDPStrategy):
    """Subclass of DDPStrategy that ensures DDP training correctly with Ray orchestration."""
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
    def __init__(self, 
                train_dataset: "Dataset", 
                val_dataset: Optional["Dataset"]=None,
                config: Optional[Dict[str, Any]]=None) -> None:
        super().__init__()
        self.train_dataset = train_dataset
        self.val_dataset = val_dataset
        self.config = config if config else {}

    def train_dataloader(self):
        ds = RayIterableDataset(self.train_dataset, self.config)
        return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

    def val_dataloader(self):
        if self.val_dataset:
            ds = RayIterableDataset(self.val_dataset, self.config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])
        else:
            raise RuntimeError("val_dataset is None for RayDataModule.")
