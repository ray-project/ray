# flake8: noqa
# isort: skip_file

# __pytorch_save_start__
import os
import tempfile

import numpy as np
import torch
import torch.nn as nn
from torch.optim import Adam

import ray.train.torch
from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    n = 100
    # create a toy dataset
    # data   : X - dim = (n, 4)
    # target : Y - dim = (n, 1)
    X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
    Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))
    # toy neural network : 1-layer
    # Wrap the model in DDP
    model = ray.train.torch.prepare_model(nn.Linear(4, 1))
    criterion = nn.MSELoss()

    optimizer = Adam(model.parameters(), lr=3e-4)
    for epoch in range(config["num_epochs"]):
        y = model.forward(X)
        loss = criterion(y, Y)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        metrics = {"loss": loss.item()}

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            checkpoint = None

            should_checkpoint = epoch % config.get("checkpoint_freq", 1) == 0
            # In standard DDP training, where the model is the same across all ranks,
            # only the global rank 0 worker needs to save and report the checkpoint
            if train.get_context().get_world_rank() == 0 and should_checkpoint:
                torch.save(
                    model.module.state_dict(),  # NOTE: Unwrap the model.
                    os.path.join(temp_checkpoint_dir, "model.pt"),
                )
                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

            train.report(metrics, checkpoint=checkpoint)


trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
)
result = trainer.fit()
# __pytorch_save_end__

# __pytorch_restore_start__
import os
import tempfile

import numpy as np
import torch
import torch.nn as nn
from torch.optim import Adam

import ray.train.torch
from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    n = 100
    # create a toy dataset
    # data   : X - dim = (n, 4)
    # target : Y - dim = (n, 1)
    X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
    Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))
    # toy neural network : 1-layer
    model = nn.Linear(4, 1)
    optimizer = Adam(model.parameters(), lr=3e-4)
    criterion = nn.MSELoss()

    # Wrap the model in DDP and move it to GPU.
    model = ray.train.torch.prepare_model(model)

    # ====== Resume training state from the checkpoint. ======
    start_epoch = 0
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            model_state_dict = torch.load(
                os.path.join(checkpoint_dir, "model.pt"),
                # map_location=...,  # Load onto a different device if needed.
            )
            model.module.load_state_dict(model_state_dict)
            optimizer.load_state_dict(
                torch.load(os.path.join(checkpoint_dir, "optimizer.pt"))
            )
            start_epoch = (
                torch.load(os.path.join(checkpoint_dir, "extra_state.pt"))["epoch"] + 1
            )
    # ========================================================

    for epoch in range(start_epoch, config["num_epochs"]):
        y = model.forward(X)
        loss = criterion(y, Y)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        metrics = {"loss": loss.item()}

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            checkpoint = None

            should_checkpoint = epoch % config.get("checkpoint_freq", 1) == 0
            # In standard DDP training, where the model is the same across all ranks,
            # only the global rank 0 worker needs to save and report the checkpoint
            if train.get_context().get_world_rank() == 0 and should_checkpoint:
                # === Make sure to save all state needed for resuming training ===
                torch.save(
                    model.module.state_dict(),  # NOTE: Unwrap the model.
                    os.path.join(temp_checkpoint_dir, "model.pt"),
                )
                torch.save(
                    optimizer.state_dict(),
                    os.path.join(temp_checkpoint_dir, "optimizer.pt"),
                )
                torch.save(
                    {"epoch": epoch},
                    os.path.join(temp_checkpoint_dir, "extra_state.pt"),
                )
                # ================================================================
                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

            train.report(metrics, checkpoint=checkpoint)

        if epoch == 1:
            raise RuntimeError("Intentional error to showcase restoration!")


trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
    run_config=train.RunConfig(failure_config=train.FailureConfig(max_failures=1)),
)
result = trainer.fit()

# Seed a training run with a checkpoint using `resume_from_checkpoint`
trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
    resume_from_checkpoint=result.checkpoint,
)
# __pytorch_restore_end__

# __checkpoint_from_single_worker_start__
import tempfile

from ray import train


def train_fn(config):
    ...

    metrics = {...}
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        checkpoint = None

        # Only the global rank 0 worker saves and reports the checkpoint
        if train.get_context().get_world_rank() == 0:
            ...  # Save checkpoint to temp_checkpoint_dir

            checkpoint = Checkpoint.from_directory(tmpdir)

        train.report(metrics, checkpoint=checkpoint)


# __checkpoint_from_single_worker_end__


# __lightning_save_example_start__
import pytorch_lightning as pl

from ray import train
from ray.train.lightning import RayTrainReportCallback
from ray.train.torch import TorchTrainer


class MyLightningModule(pl.LightningModule):
    # ...

    def on_validation_epoch_end(self):
        ...
        mean_acc = calculate_accuracy()
        self.log("mean_accuracy", mean_acc, sync_dist=True)


def train_func():
    ...
    model = MyLightningModule(...)
    datamodule = MyLightningDataModule(...)

    trainer = pl.Trainer(
        # ...
        callbacks=[RayTrainReportCallback()]
    )
    trainer.fit(model, datamodule=datamodule)


ray_trainer = TorchTrainer(
    train_func,
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(
        checkpoint_config=train.CheckpointConfig(
            num_to_keep=2,
            checkpoint_score_attribute="mean_accuracy",
            checkpoint_score_order="max",
        ),
    ),
)
# __lightning_save_example_end__


# __lightning_custom_save_example_start__
import os
from tempfile import TemporaryDirectory

from pytorch_lightning.callbacks import Callback

import ray
import ray.train
from ray.train import Checkpoint


class CustomRayTrainReportCallback(Callback):
    def on_train_epoch_end(self, trainer, pl_module):
        should_checkpoint = trainer.current_epoch % 3 == 0

        with TemporaryDirectory() as tmpdir:
            # Fetch metrics
            metrics = trainer.callback_metrics
            metrics = {k: v.item() for k, v in metrics.items()}

            # Add customized metrics
            metrics["epoch"] = trainer.current_epoch
            metrics["custom_metric"] = 123

            checkpoint = None
            global_rank = ray.train.get_context().get_world_rank() == 0
            if global_rank == 0 and should_checkpoint:
                # Save model checkpoint file to tmpdir
                ckpt_path = os.path.join(tmpdir, "ckpt.pt")
                trainer.save_checkpoint(ckpt_path, weights_only=False)

                checkpoint = Checkpoint.from_directory(tmpdir)

            # Report to train session
            ray.train.report(metrics=metrics, checkpoint=checkpoint)


# __lightning_custom_save_example_end__

# __lightning_restore_example_start__
import os

from ray import train
from ray.train import Checkpoint
from ray.train.torch import TorchTrainer
from ray.train.lightning import RayTrainReportCallback


def train_func():
    model = MyLightningModule(...)
    datamodule = MyLightningDataModule(...)
    trainer = pl.Trainer(..., callbacks=[RayTrainReportCallback()])

    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            ckpt_path = os.path.join(ckpt_dir, "checkpoint.ckpt")
            trainer.fit(model, datamodule=datamodule, ckpt_path=ckpt_path)
    else:
        trainer.fit(model, datamodule=datamodule)


# Build a Ray Train Checkpoint
# Suppose we have a Lightning checkpoint at `s3://bucket/ckpt_dir/checkpoint.ckpt`
checkpoint = Checkpoint("s3://bucket/ckpt_dir")

# Resume training from checkpoint file
ray_trainer = TorchTrainer(
    train_func,
    scaling_config=train.ScalingConfig(num_workers=2),
    resume_from_checkpoint=checkpoint,
)
# __lightning_restore_example_end__


# __transformers_save_example_start__
from transformers import TrainingArguments

from ray import train
from ray.train.huggingface.transformers import RayTrainReportCallback, prepare_trainer
from ray.train.torch import TorchTrainer


def train_func(config):
    ...

    # Configure logging, saving, evaluation strategies as usual.
    args = TrainingArguments(
        ...,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="step",
    )

    trainer = transformers.Trainer(args, ...)

    # Add a report callback to transformers Trainer
    # =============================================
    trainer.add_callback(RayTrainReportCallback())
    trainer = prepare_trainer(trainer)

    trainer.train()


ray_trainer = TorchTrainer(
    train_func,
    run_config=train.RunConfig(
        checkpoint_config=train.CheckpointConfig(
            num_to_keep=3,
            checkpoint_score_attribute="eval_loss",  # The monitoring metric
            checkpoint_score_order="min",
        )
    ),
)
# __transformers_save_example_end__


# __transformers_custom_save_example_start__
from ray import train

from transformers.trainer_callback import TrainerCallback


class MyTrainReportCallback(TrainerCallback):
    def __init__(self):
        super().__init__()
        self.metrics = {}

    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        """Log is called on evaluation step and logging step."""
        self.metrics.update(logs)

    def on_save(self, args, state, control, **kwargs):
        """Event called after a checkpoint save."""

        checkpoint = None
        if train.get_context().get_world_rank() == 0:
            # Build a Ray Train Checkpoint from the latest checkpoint
            checkpoint_path = transformers.trainer.get_last_checkpoint(args.output_dir)
            checkpoint = Checkpoint.from_directory(checkpoint_path)

        # Report to Ray Train with up-to-date metrics
        ray.train.report(metrics=self.metrics, checkpoint=checkpoint)

        # Clear the metrics buffer
        self.metrics = {}


# __transformers_custom_save_example_end__


# __distributed_checkpointing_start__
from ray import train
from ray.train import Checkpoint
from ray.train.torch import TorchTrainer


def train_func(config):
    ...

    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        rank = train.get_context().get_world_rank()
        torch.save(
            ...,
            os.path.join(temp_checkpoint_dir, f"model-rank={rank}.pt"),
        )
        checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

        train.report(metrics, checkpoint=checkpoint)


trainer = TorchTrainer(
    train_func,
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(storage_path="s3://bucket/"),
)
# The checkpoint in cloud storage will contain: model-rank=0.pt, model-rank=1.pt
# __distributed_checkpointing_end__

# __inspect_checkpoint_example_start__
from pathlib import Path

from ray.train import Checkpoint

# For demonstration, create a locally available directory with a `model.pt` file.
example_checkpoint_dir = Path("/tmp/test-checkpoint")
example_checkpoint_dir.mkdir()
example_checkpoint_dir.joinpath("model.pt").touch()

# Create the checkpoint, which is a reference to the directory.
checkpoint = Checkpoint.from_directory(example_checkpoint_dir)

# Inspect the checkpoint's contents with either `as_directory` or `to_directory`:
with checkpoint.as_directory() as checkpoint_dir:
    assert Path(checkpoint_dir).joinpath("model.pt").exists()

checkpoint_dir = checkpoint.to_directory()
assert Path(checkpoint_dir).joinpath("model.pt").exists()
# __inspect_checkpoint_example_end__

# __inspect_transformers_checkpoint_example_start__
# After training finished
checkpoint = result.checkpoint
with checkpoint.as_directory() as checkpoint_dir:
    hf_checkpoint_path = f"{checkpoint_dir}/checkpoint/"
# __inspect_transformers_checkpoint_example_end__

# __inspect_lightning_checkpoint_example_start__
# After training finished
checkpoint = result.checkpoint
with checkpoint.as_directory() as checkpoint_dir:
    lightning_checkpoint_path = f"{checkpoint_dir}/checkpoint.ckpt"
# __inspect_lightning_checkpoint_example_end__
