import torch
from torch import Tensor
from typing import Any, Dict, Optional
from copy import deepcopy

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.loggers.logger import Logger
from pytorch_lightning import Trainer
from pytorch_lightning.utilities import rank_zero_info, rank_zero_only
from pytorch_lightning.utilities.types import STEP_OUTPUT
from pytorch_lightning.strategies import DDPStrategy
from lightning_fabric.plugins.environments.lightning import LightningEnvironment

import ray
from ray.air import session
from ray.air.config import CheckpointConfig
from ray.air.checkpoint import Checkpoint


class RayModelCheckpoint(ModelCheckpoint):
    def setup(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule", stage: str) -> None:
        super().setup(trainer, pl_module, stage)
        self.last_best_k_models = {}

    def format_checkpoint_name(
        self, metrics: Dict[str, Tensor], filename: Optional[str] = None, ver: Optional[int] = None
    ) -> str:
        """
        Original ModelCheckpoint callback save all checkpoints in the same directory.
        However, AIR Checkpoint requires one folder only contains one checkpoint. 
        This function inserts an intermediate folder to the original checkpoint path.
        e.g. './epoch=2-validation_loss=0.12.ckpt' -> './epoch=2-validation_loss=0.12/checkpoint.ckpt'
        """
        filepath = super().format_checkpoint_name(metrics, filename, ver)
        filepath.replace(self.FILE_EXTENSION, f"/checkpoint{self.FILE_EXTENSION}")
        return filepath

    def pop_buffered_metrics(self, trainer: "pl.Trainer", on_step: bool = True) -> Dict[str, Any]:
        """
        trainer._results dynamically maintains the last reported values for all metrics. 
        By default, it will only get reset at the end of each epoch. However, AIR requires a 
        metric to be reported only once, so every time we fetch metrics for session.report(), 
        we have to reset this results table.
        """
        buffered_metrics = {}
        if trainer._results is not None:
            metrics = trainer._results.metrics(on_step=on_step)
            buffered_metrics.update(metrics["callback"])
            trainer._results.reset()
        return buffered_metrics

    def _session_report(self, trainer: "pl.Trainer"):
        kwargs = {}
        kwargs["metrics"] = self.pop_buffered_metrics(trainer)

        new_checkpoint = self.best_k_models.keys() - self.last_best_k_models.keys()
        if len(new_checkpoint) == 1:
            kwargs["checkpoint"] = Checkpoint.from_directory(path=new_checkpoint.pop())
        assert len(new_checkpoint) <= 1
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

# class RayLogger(Logger):
#     def __init__(self, checkpoint_config: CheckpointConfig):
#         super().__init__()
#         self._checkpoint_config = checkpoint_config
#         self._checkpoint_frequency = checkpoint_config.checkpoint_frequency
#         self._monitor = checkpoint_config.checkpoint_score_attribute
#         self._monitor_steps = 0
        
#     def set_trainer(self, trainer: Trainer):
#         self._trainer = trainer

#     def dump_checkpoint(self) -> Dict[str, Any]:
#         assert self._trainer, "Trainer not initialized in RayLogger."
#         return self._trainer._checkpoint_connector.dump_checkpoint(weights_only=False)

#     @rank_zero_only
#     def log_metrics(
#         self, metrics: Dict[str, float], step: Optional[int] = None
#     ) -> None:
#         print("Logget: ", metrics, step)

class RayDDPStrategy(DDPStrategy):
    @property
    def root_device(self) -> torch.device:
        return ray.train.torch.get_device()

class RayEnvironment(LightningEnvironment):
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


        