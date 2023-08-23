from lightning_exp_tracking_model_dl import DummyModel, dataloader

# __lightning_experiment_tracking_wandb_start__
import os
import pytorch_lightning as pl
from pytorch_lightning.loggers.wandb import WandbLogger
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import wandb


def train_func(config):

    # Note this is equivalent to calling `wandb.login` with API key.
    os.environ["WANDB_API_KEY"] = config["wandb_api_key"]
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


scaling_config = ScalingConfig(num_workers=4, use_gpu=False)

assert (
    "WANDB_API_KEY" in os.environ
), 'Please do WANDB_API_KEY="abcde" when running this script.'
trainer = TorchTrainer(
    train_func,
    train_loop_config={"wandb_api_key": os.environ["WANDB_API_KEY"]},
    scaling_config=scaling_config,
)

trainer.fit()
