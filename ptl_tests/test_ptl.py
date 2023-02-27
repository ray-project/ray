import os
os.environ['RAY_ML_DEV'] = "1"

from ray import train
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer, TorchCheckpoint
import ray
import socket
import pytorch_lightning as pl
from torch.utils.data import DataLoader
from torchvision.datasets import MNIST
from torchvision import transforms
import torch.nn.functional as F
from torch import nn
import torch
from torch import Tensor

from pytorch_lightning.loggers.logger import Logger
from pytorch_lightning import strategies

from typing import Any, Dict, Optional
from pytorch_lightning.utilities import rank_zero_info, rank_zero_only
from ray.air.checkpoint import Checkpoint
from ray.air import session


class Encoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(28 * 28, 64), nn.ReLU(), nn.Linear(64, 3))

    def forward(self, x):
        return self.l1(x)


class Decoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(3, 64), nn.ReLU(), nn.Linear(64, 28 * 28))

    def forward(self, x):
        return self.l1(x)


class LitAutoEncoder(pl.LightningModule):
    def __init__(self, encoder, decoder):
        super().__init__()
        self.encoder = encoder
        self.decoder = decoder
        self.global_steps = 0

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop.
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        loss = F.mse_loss(x_hat, x)

        self.global_steps += 1
        if self.global_steps % 100 == 0:
            loss_value = loss.item()
            self.log_dict({"loss": loss_value, "steps": self.global_steps})
        if self.global_step % 3 == 0:
            self.log("metric_step_3", self.global_steps)
        if self.global_step % 5 == 0:
            self.log("metric_step_5", self.global_steps)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer

    def training_epoch_end(self, outputs) -> None:
        loss = sum(output["loss"] for output in outputs) / len(outputs)
        self.log_dict({"epoch_end_metric": 123})

# model
encoder = Encoder()
decoder = Decoder()
autoencoder = LitAutoEncoder(encoder, decoder)

dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
train_loader = DataLoader(dataset, batch_size=100)

class RayLogger(Logger):
    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return "TrainReportCheckpointLogger"

    @property
    def version(self):
        return "123"

    def log_hyperparams(self, param):
        pass

    @rank_zero_only
    def log_metrics(
        self, metrics: Dict[str, float], step: Optional[int] = None
    ) -> None:
        print("Logger: ", step, metrics)

from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.utilities.types import STEP_OUTPUT

# class RayModelCheckpoint(ModelCheckpoint):
#     def on_train_batch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule", outputs: STEP_OUTPUT, batch: Any, batch_idx: int) -> None:
#         super().on_train_batch_end(trainer, pl_module, outputs, batch, batch_idx)
#         metrics = trainer._results.metrics(True)
#         if trainer.global_rank == 0:
#             print(metrics["callback"])
#         # trainer.logged_metrics.clear()
#         trainer._logger_connector.reset_results()

class RayModelCheckpoint(ModelCheckpoint):
    def setup(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule", stage: str) -> None:
        super().setup(trainer, pl_module, stage)
        self.last_best_model_path = ""

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
        return filepath.replace(self.FILE_EXTENSION, f"/checkpoint{self.FILE_EXTENSION}")

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

        # Only report checkpoint after updated
        if self.last_best_model_path != self.best_model_path:
            print("better model: ", self.best_model_path)
            kwargs["checkpoint"] = Checkpoint.from_directory(path=self.best_model_path)
            self.last_best_model_path = self.best_model_path
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

checkpoint_config = {
    # "dirpath": "/mnt/cluster_storage/lightining_logs",
    # "monitor": "loss",
    # "save_top_k": 3
}

# checkpoint_callback = RayModelCheckpoint(**checkpoint_config)
checkpoint_callback = ModelCheckpoint(**checkpoint_config)

# train model
# trainer = pl.Trainer(max_epochs=100, accelerator="gpu", strategy="ddp", enable_checkpointing=False, devices=[device.index])
# trainer = pl.Trainer(logger=RayLogger(), max_epochs=10, accelerator="gpu", strategy="ddp", enable_checkpointing=True, log_every_n_steps=1, callbacks=[checkpoint_callback])

trainer = pl.Trainer(max_epochs=10, accelerator="gpu", strategy="ddp", enable_checkpointing=True, callbacks=[checkpoint_callback])

trainer.fit(model=autoencoder, train_dataloaders=train_loader)

