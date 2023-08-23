from lightning_exp_tracking_model_dl import DummyModel, dataloader

# __lightning_experiment_tracking_mlflow_start__
import os
import pytorch_lightning as pl
from pytorch_lightning.loggers.mlflow import MLFlowLogger

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):

    save_dir = config["save_dir"]
    logger = MLFlowLogger(
        experiment_name="demo-project",
        tracking_uri=f"file:{save_dir}",
    )

    ptl_trainer = pl.Trainer(
        max_epochs=5,
        accelerator="cpu",
        logger=logger,
        log_every_n_steps=1,
    )
    model = DummyModel()
    ptl_trainer.fit(model, train_dataloaders=dataloader)


scaling_config = ScalingConfig(num_workers=4, use_gpu=False)

assert (
    "SHARED_STORAGE_PATH" in os.environ
), "Please do SHARED_STORAGE_PATH=/a/b/c when running this script."
trainer = TorchTrainer(
    train_func,
    train_loop_config={"save_dir": os.path.join(os.environ["SHARED_STORAGE_PATH"], "mlruns")},
    scaling_config=scaling_config,
)

trainer.fit()