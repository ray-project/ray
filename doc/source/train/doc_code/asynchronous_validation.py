# __validate_fn_simple_start__

import os
import torch

import ray.train


def validate_fn(checkpoint: ray.train.Checkpoint, config: dict) -> dict:
    # Load the checkpoint
    model = ...
    with checkpoint.as_directory() as checkpoint_dir:
        model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
        model.load_state_dict(model_state_dict)
    model.eval()

    # Perform validation on the data
    total_accuracy = 0
    dataset = config["dataset"]
    with torch.no_grad():
        for batch in dataset.iter_torch_batches(batch_size=128):
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            total_accuracy += (outputs.argmax(1) == labels).sum().item()
    return {"score": total_accuracy / len(dataset)}


# __validate_fn_simple_end__

# __validate_fn_torch_trainer_start__
import torchmetrics
from torch.nn import CrossEntropyLoss

import ray.train.torch


def eval_only_train_fn(config_dict: dict) -> None:
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

    # Compute and report metric
    with torch.no_grad():
        for batch in test_dataloader:
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            loss = criterion(outputs, labels)
            mean_valid_loss(loss)
    ray.train.report(
        metrics={"score": mean_valid_loss.compute().item()},
        checkpoint=ray.train.Checkpoint(
            ray.train.get_context()
            .get_storage()
            .build_checkpoint_path_from_name("placeholder")
        ),
        checkpoint_upload_mode=ray.train.CheckpointUploadMode.NO_UPLOAD,
    )


def validate_fn(checkpoint: ray.train.Checkpoint, config: dict) -> dict:
    trainer = ray.train.torch.TorchTrainer(
        eval_only_train_fn,
        train_loop_config={"checkpoint": checkpoint},
        scaling_config=ray.train.ScalingConfig(
            num_workers=2, use_gpu=True, accelerator_type="A10G"
        ),
        # Name validation run to easily associate it with training run
        run_config=ray.train.RunConfig(
            name=f"{config['train_run_name']}_validation_epoch_{config['epoch']}"
        ),
        # User weaker GPUs for validation
        datasets={"validation": config["dataset"]},
    )
    result = trainer.fit()
    return result.metrics


# __validate_fn_torch_trainer_end__

# __validate_fn_map_batches_start__


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


def validate_fn(checkpoint: ray.train.Checkpoint, config: dict) -> dict:
    # Set name to avoid confusion; default name is "Dataset"
    config["dataset"].set_name("validation")
    eval_res = config["dataset"].map_batches(
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


# __validate_fn_map_batches_end__

# __validate_fn_report_start__
import tempfile

import ray.data


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
                validate_fn=validate_fn,
                validate_config={
                    "dataset": config["validation_dataset"],
                    "train_run_name": ray.train.get_context().get_experiment_name(),
                    "epoch": epoch,
                },
            )
        else:
            ray.train.report({}, None)


def run_trainer() -> ray.train.Result:
    train_dataset = ray.data.read_parquet(...)
    validation_dataset = ray.data.read_parquet(...)
    trainer = ray.train.torch.TorchTrainer(
        train_func,
        # Pass training dataset in datasets arg to split it across training workers
        datasets={"train": train_dataset},
        # Pass validation dataset in train_loop_config so validate_fn can choose how to use it later
        train_loop_config={"validation_dataset": validation_dataset},
        scaling_config=ray.train.ScalingConfig(
            num_workers=2,
            use_gpu=True,
            # Use powerful GPUs for training
            accelerator_type="A100",
        ),
    )
    return trainer.fit()


# __validate_fn_report_end__
