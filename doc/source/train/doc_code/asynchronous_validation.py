# __validation_fn_simple_start__
import os
import torch

import ray.train
import ray.data

# Define Ray Data validation dataset outside validation function because it is not json serializable
validation_dataset = ...


def validation_fn(checkpoint: ray.train.Checkpoint) -> dict:
    # Load the checkpoint
    model = ...
    with checkpoint.as_directory() as checkpoint_dir:
        model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
        model.load_state_dict(model_state_dict)
    model.eval()

    # Perform validation on the data
    total_accuracy = 0
    with torch.no_grad():
        for batch in validation_dataset.iter_torch_batches(batch_size=128):
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            total_accuracy += (outputs.argmax(1) == labels).sum().item()
    return {"score": total_accuracy / len(validation_dataset)}
# __validation_fn_simple_end__

# __validation_fn_torch_trainer_start__
import torchmetrics
from torch.nn import CrossEntropyLoss

import ray.train.torch


def eval_only_train_fn(config_dict: dict) -> dict:
    # Load the checkpoint
    model = ...
    with config_dict["checkpoint"].as_directory() as checkpoint_dir:
        model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
        model.load_state_dict(model_state_dict)
    model.cuda().eval()

    # Set up metrics and data loaders
    criterion = CrossEntropyLoss()
    mean_valid_loss = torchmetrics.MeanMetric().cuda()
    test_data_shard = ray.train.get_dataset_shard("validation")
    test_dataloader = test_data_shard.iter_torch_batches(batch_size=128)

    # Compute metric and return it directly from the train function
    with torch.no_grad():
        for batch in test_dataloader:
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            loss = criterion(outputs, labels)
            mean_valid_loss(loss)
    # Report metrics
    return {"score": mean_valid_loss.compute().item()}


def validation_fn(checkpoint: ray.train.Checkpoint, train_run_name: str, epoch: int) -> dict:
    trainer = ray.train.torch.TorchTrainer(
        eval_only_train_fn,
        train_loop_config={"checkpoint": checkpoint},
        scaling_config=ray.train.ScalingConfig(
            num_workers=2, use_gpu=True, accelerator_type="A10G"
        ),
        # Give unique name to validation run so it does not attempt to load placeholder checkpoint.
        # Also allows you to better associate training runs with validation runs.
        run_config=ray.train.RunConfig(
            name=f"{train_run_name}_validation_epoch_{epoch}"
        ),
        # Use weaker GPUs for validation
        datasets={"validation": validation_dataset},
    )
    result = trainer.fit()
    # return_values holds the return value of the train function from worker 0
    return result.return_value
# __validation_fn_torch_trainer_end__

# __validation_fn_map_batches_start__
class Predictor:
    def __init__(self, checkpoint: ray.train.Checkpoint):
        self.model = ...
        with checkpoint.as_directory() as checkpoint_dir:
            model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
            self.model.load_state_dict(model_state_dict)
        self.model.cuda().eval()

    def __call__(self, batch: dict) -> dict:
        image = torch.as_tensor(batch["image"], dtype=torch.float32, device="cuda")
        label = torch.as_tensor(batch["label"], dtype=torch.float32, device="cuda")
        pred = self.model(image)
        return {"res": (pred.argmax(1) == label).cpu().numpy()}


def validation_fn(checkpoint: ray.train.Checkpoint) -> dict:
    # Set name to avoid confusion; default name is "Dataset"
    validation_dataset.set_name("validation")
    eval_res = validation_dataset.map_batches(
        Predictor,
        batch_size=128,
        num_gpus=1,
        fn_constructor_kwargs={"checkpoint": checkpoint},
        concurrency=2,
    )
    mean = eval_res.mean(["res"])
    return {
        "score": mean,
    }
# __validation_fn_map_batches_end__

# __validation_fn_report_start__
import tempfile

from ray.train import ValidationConfig, ValidationTaskConfig


def train_func(config: dict) -> None:
    ...
    epochs = ...
    model = ...
    rank = ray.train.get_context().get_world_rank()
    for epoch in epochs:
        ...  # training step
        if rank == 0:
            training_metrics = {"loss": ..., "epoch": epoch}
            local_checkpoint_dir = tempfile.mkdtemp()
            torch.save(
                model.module.state_dict(),
                os.path.join(local_checkpoint_dir, "model.pt"),
            )
            ray.train.report(
                training_metrics,
                checkpoint=ray.train.Checkpoint.from_directory(local_checkpoint_dir),
                checkpoint_upload_mode=ray.train.CheckpointUploadMode.ASYNC,
                validation=ValidationTaskConfig(fn_kwargs={
                    "train_run_name": ray.train.get_context().get_experiment_name(),
                    "epoch": epoch,
                }),
            )
        else:
            ray.train.report({}, None)


def run_trainer() -> ray.train.Result:
    train_dataset = ray.data.read_parquet(...)
    trainer = ray.train.torch.TorchTrainer(
        train_func,
        validation_config=ValidationConfig(fn=validation_fn),
        # Pass training dataset in datasets arg to split it across training workers
        datasets={"train": train_dataset},
        scaling_config=ray.train.ScalingConfig(
            num_workers=2,
            use_gpu=True,
            # Use powerful GPUs for training
            accelerator_type="A100",
        ),
    )
    return trainer.fit()
# __validation_fn_report_end__

# __exp_tracking_same_run_wandb_start__
import wandb
import ray.train
from ray.train import ValidationConfig, ValidationTaskConfig


entity = "my_entity"
project = "my_project"
num_epochs = ...


def validation_fn(checkpoint: ray.train.Checkpoint, wandb_run_id: str, val_step: int) -> dict:
    wandb.init(
        entity=entity,
        project=project,
        settings=wandb.Settings(mode="shared", x_primary=False),
        id=wandb_run_id,
    )
    score = ...
    wandb.log({"validation/loss": score, "val_step": val_step})
    wandb.finish()  # flush the metrics
    return {"validation/loss": score}


def train_func():
    if ray.train.get_context().get_world_rank() == 0:
        run = wandb.init(
            entity=entity,
            project=project,
            settings=wandb.Settings(mode="shared", x_primary=True,)
        )
        wandb.define_metric("val_step", hidden=True)
        wandb.define_metric("train_step", hidden=True)
        wandb.define_metric("validation/loss", step_metric="val_step")
        wandb.define_metric("train/loss", step_metric="train_step")

    for epoch in range(num_epochs):
        loss = ...
        if ray.train.get_context().get_world_rank() == 0:
            wandb.log({"train/loss": loss, "train_step": epoch})
            checkpoint = ...
            ray.train.report(
                {"train/loss": loss},
                checkpoint=checkpoint,
                validation=ValidationTaskConfig(
                    fn_kwargs={"wandb_run_id": run.id, "val_step": epoch}
                ),
            )
        else:
            ray.train.report({}, None)

    if ray.train.get_context().get_world_rank() == 0:
        wandb.finish()


# __exp_tracking_same_run_wandb_end__

# __exp_tracking_same_run_mlflow_start__
import mlflow
from mlflow.tracking import MlflowClient
import ray.train
from ray.train import ValidationConfig, ValidationTaskConfig


tracking_uri = "my_uri"
experiment_name = "my_experiment"
num_epochs = ...

def validation_fn(
    checkpoint: ray.train.Checkpoint, mlflow_run_id: str, val_step: int
) -> dict:
    client = MlflowClient(tracking_uri=tracking_uri)
    score = ...
    client.log_metric(mlflow_run_id, "val_score", score, step=val_step)
    return {"val_score": score}


def train_func():
    if ray.train.get_context().get_world_rank() == 0:
        client = MlflowClient(tracking_uri=tracking_uri)
        experiment = client.get_experiment_by_name(experiment_name)
        run = client.create_run(experiment_id=experiment.experiment_id)

    for epoch in range(num_epochs):
        loss = ...
        if ray.train.get_context().get_world_rank() == 0:
            client.log_metric(run.info.run_id, "train_loss", loss, step=epoch)
            checkpoint = ...
            ray.train.report(
                {"train_loss": loss},
                checkpoint=checkpoint,
                validation=ValidationTaskConfig(
                    fn_kwargs={"mlflow_run_id": run.info.run_id, "val_step": epoch}
                ),
            )
        else:
            ray.train.report({}, None)

    if ray.train.get_context().get_world_rank() == 0:
        client.set_terminated(run.info.run_id)

# __exp_tracking_same_run_mlflow_end__
