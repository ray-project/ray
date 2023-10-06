# flake8: noqa
# fmt: off
# # isort: skip_file

from lightning_exp_tracking_model_dl import DummyModel, dataloader

# __lightning_experiment_tracking_wandb_start__
import os
import pytorch_lightning as pl
import wandb
from pytorch_lightning.loggers.wandb import WandbLogger
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    logger = None
    if ray.train.get_context().get_world_rank() == 0:
        logger = WandbLogger(name="demo-run", project="demo-project")

    ptl_trainer = pl.Trainer(
        max_epochs=5,
        accelerator="cpu",
        logger=logger,
        log_every_n_steps=1,
    )
    model = DummyModel()
    ptl_trainer.fit(model, train_dataloaders=dataloader)
    if ray.train.get_context().get_world_rank() == 0:
        wandb.finish()


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

assert (
    "WANDB_API_KEY" in os.environ
), 'Please set WANDB_API_KEY="abcde" when running this script.'

# This ensures that all workers have this env var set.
ray.init(
    runtime_env={"env_vars": {"WANDB_API_KEY": os.environ["WANDB_API_KEY"]}}
)
trainer = TorchTrainer(
    train_func,
    scaling_config=scaling_config,
)

trainer.fit()
