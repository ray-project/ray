# flake8: noqa
# isort: skip_file

# __lightning_experiment_tracking_model_data_start__
import os
import torch
import torch.nn.functional as F
import pytorch_lightning as pl
from torch.utils.data import TensorDataset, DataLoader

# create dummy data
X = torch.randn(128, 3)  # 128 samples, 3 features
y = torch.randint(0, 2, (128,))  # 128 binary labels

# create a TensorDataset to wrap the data
dataset = TensorDataset(X, y)

# create a DataLoader to iterate over the dataset
batch_size = 8
dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

# Define a dummy model
class DummyModel(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.layer = torch.nn.Linear(3, 1)

    def forward(self, x):
        return self.layer(x)

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.binary_cross_entropy_with_logits(y_hat.flatten(), y.float())

        # The metrics below will be reported to Loggers
        self.log("train_loss", loss)
        self.log_dict({"metric_1": 1 / (batch_idx + 1), "metric_2": batch_idx * 100})
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=1e-3)


# __lightning_experiment_tracking_model_data_end__

# __lightning_experiment_tracking_wandb_start__
import pytorch_lightning as pl
from pytorch_lightning.loggers.wandb import WandbLogger

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):

    # Note this is equivalent to calling `wandb.login` with API key.
    os.environ["WANDB_API_KEY"] = config["wandb_api_key"]
    logger = WandbLogger(name="demo-run", project="demo-project")

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
    "WANDB_API_KEY" in os.environ
), 'Please do WANDB_API_KEY="abcde" when running this script.'
trainer = TorchTrainer(
    train_func,
    {"wandb_api_key": os.environ["WANDB_API_KEY"]},
    scaling_config=scaling_config,
)

trainer.fit()
# __lightning_experiment_tracking_wandb_end__

# __lightning_experiment_tracking_comet_start__
import pytorch_lightning as pl
from pytorch_lightning.loggers.comet import CometLogger

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):

    comet_api_key = config["comet_api_key"]
    logger = CometLogger(api_key=comet_api_key, name="demo-run", project="demo-project")

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
    "COMET_API_KEY" in os.environ
), 'Please do COMET_API_KEY="abcde" when running this script.'
trainer = TorchTrainer(
    train_func,
    {"comet_api_key": os.environ["COMET_API_KEY"]},
    scaling_config=scaling_config,
)

trainer.fit()
# __lightning_experiment_tracking_comet_end__

# __lightning_experiment_tracking_mlflow_start__
import pytorch_lightning as pl
from pytorch_lightning.loggers.mlflow import MLFlowLogger

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):

    save_dir = config["save_dir"]
    logger = MLFlowLogger(
        run_name="demo-run",
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
), 'Please do SHARED_STORAGE_PATH="\a\b\c" when running this script.'
trainer = TorchTrainer(
    train_func,
    {"save_dir": os.path.join(os.environ["SHARED_STORAGE_PATH"], "mlflow")},
    scaling_config=scaling_config,
)

trainer.fit()
# __lightning_experiment_tracking_mlflow_end__

# __lightning_experiment_tracking_tensorboard_start__
import pytorch_lightning as pl
from pytorch_lightning.loggers.tensorboard import TensorBoardLogger

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):

    save_dir = config["save_dir"]
    logger = TensorBoardLogger(name="demo-run", save_dir=f"file:{save_dir}")

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
), 'Please do SHARED_STORAGE_PATH="\a\b\c" when running this script.'
trainer = TorchTrainer(
    train_func,
    {"save_dir": os.path.join(os.environ["SHARED_STORAGE_PATH"], "mlflow")},
    scaling_config=scaling_config,
)

trainer.fit()
# __lightning_experiment_tracking_tensorboard_end__
