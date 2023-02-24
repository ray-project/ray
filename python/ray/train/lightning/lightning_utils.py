import pytorch_lightning as ptl
from pytorch_lightning.loggers.logger import Logger
from ray.air import session
from lightning_fabric.plugins.environments.lightning import LightningEnvironment
from pytorch_lightning import Trainer
from pytorch_lightning.utilities import rank_zero_info, rank_zero_only
from pytorch_lightning.strategies import DDPStrategy
from ray.air.config import CheckpointConfig
import torch
import ray
from typing import Any, Dict, Optional
from ray.air.checkpoint import Checkpoint

class RayLogger(Logger):
    def __init__(self, checkpoint_config: CheckpointConfig):
        super().__init__()
        self._checkpoint_config = checkpoint_config
        self._checkpoint_frequency = checkpoint_config.checkpoint_frequency
        self._monitor = checkpoint_config.checkpoint_score_attribute
        self._monitor_steps = 0

    def set_trainer(self, trainer: Trainer):
        self._trainer = trainer

    def dump_checkpoint(self) -> Dict[str, Any]:
        assert self._trainer, "Trainer not initialized in RayLogger."
        return self._trainer._checkpoint_connector.dump_checkpoint(weights_only=False)

    @rank_zero_only
    def log_metrics(
        self, metrics: Dict[str, float], step: Optional[int] = None
    ) -> None:
        if self._monitor is None or self._monitor in metrics:
            if (self._monitor_steps + 1) % self._checkpoint_frequency == 0:
                checkpoint = Checkpoint.from_dict(self.dump_checkpoint())
                session.report(metrics=metrics, checkpoint=checkpoint)
            else:
                session.report(metrics=metrics)
            self._monitor_steps += 1
        else:
            session.report(metrics=metrics)

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


        