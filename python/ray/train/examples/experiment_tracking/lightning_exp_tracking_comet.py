# isort: skip_file
from lightning_exp_tracking_model_dl import DummyModel, dataloader

# __lightning_experiment_tracking_comet_start__
import os
import pytorch_lightning as pl
from pytorch_lightning.loggers.comet import CometLogger
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    logger = None
    if ray.train.get_context().get_world_rank() == 0:
        logger = CometLogger(api_key=os.environ["COMET_API_KEY"])

    ptl_trainer = pl.Trainer(
        max_epochs=5,
        accelerator="cpu",
        logger=logger,
        log_every_n_steps=1,
    )
    model = DummyModel()
    ptl_trainer.fit(model, train_dataloaders=dataloader)


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

assert (
    "COMET_API_KEY" in os.environ
), 'Please do COMET_API_KEY="abcde" when running this script.'
# This makes sure that all workers have this env var set.
ray.init(runtime_env={"env_vars": {"COMET_API_KEY": os.environ["COMET_API_KEY"]}})
trainer = TorchTrainer(
    train_func,
    scaling_config=scaling_config,
)

trainer.fit()
