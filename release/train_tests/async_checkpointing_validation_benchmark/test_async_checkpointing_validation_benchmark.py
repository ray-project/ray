import logging
import os
import tempfile
import time
from typing import Callable, Optional

import torch
import torchmetrics
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torchvision import transforms
from torchvision.models import VisionTransformer
from torchvision.transforms import ToTensor, Normalize

import ray
import ray.train
import ray.train.torch
from ray.train.v2.api.report_config import CheckpointUploadMode
from ray._private.test_utils import safe_write_to_results_json


logger = logging.getLogger(__name__)

MAXIMUM_ALLOWED_ACCURACY_DIFF = 0.2
MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER = 1.1

# ==== Start dataset and model creation ======

STORAGE_PATH_PREFIX = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "artifact_storage")
STORAGE_PATH = f"{STORAGE_PATH_PREFIX}/ray_summit_24_train_demo"


def transform_cifar(row: dict):
    transform = transforms.Compose(
        [ToTensor(), Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )
    row["image"] = transform(row["image"])
    return row


def create_model():
    return VisionTransformer(
        image_size=32,  # CIFAR-10 image size is 32x32
        patch_size=4,  # Patch size is 4x4
        num_layers=24,  # Number of transformer layers
        num_heads=8,  # Number of attention heads
        hidden_dim=384,  # Hidden size (can be adjusted)
        mlp_dim=768,  # MLP dimension (can be adjusted)
        num_classes=10,  # CIFAR-10 has 10 classes
    )


# ==== End dataset and model creation ======

# ==== Start map_batches approach ======


class Predictor:
    def __init__(self, checkpoint):
        self.model = create_model()
        with checkpoint.as_directory() as checkpoint_dir:
            model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
            self.model.load_state_dict(model_state_dict)
        self.model.cuda().eval()

    def __call__(self, batch):
        image = torch.as_tensor(batch["image"], dtype=torch.float32, device="cuda")
        label = torch.as_tensor(batch["label"], dtype=torch.int8, device="cuda")
        pred = self.model(image)
        return {"res": (pred.argmax(1) == label).cpu().numpy()}


def validate_with_map_batches(checkpoint, config):
    start_time = time.time()
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
        "validation_time": time.time() - start_time,
    }


# ==== End map_batches approach ======

# ==== Start TorchTrainer approach ======


def eval_only_train_func(config_dict):
    # Load the checkpoint
    model = create_model()
    with config_dict["checkpoint"].as_directory() as checkpoint_dir:
        model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
        model.load_state_dict(model_state_dict)
    model.cuda().eval()

    # Get the data
    test_data_shard = ray.train.get_dataset_shard("test")
    test_dataloader = test_data_shard.iter_torch_batches(batch_size=128)

    # Report metrics with dummy checkpoint
    mean_acc = torchmetrics.Accuracy(task="multiclass", num_classes=10, top_k=1).cuda()
    with torch.no_grad():
        for batch in test_dataloader:
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            mean_acc(outputs.argmax(1), labels)
    ray.train.report(
        metrics={"score": mean_acc.compute().item()},
        checkpoint=ray.train.Checkpoint(
            ray.train.get_context()
            .get_storage()
            .build_checkpoint_path_from_name("placeholder")
        ),
        checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
    )


def validate_with_torch_trainer(checkpoint, config):
    start_time = time.time()
    trainer = ray.train.torch.TorchTrainer(
        eval_only_train_func,
        train_loop_config={"checkpoint": checkpoint},
        scaling_config=ray.train.ScalingConfig(num_workers=2, use_gpu=True),
        datasets={"test": config["dataset"]},
        run_config=ray.train.RunConfig(
            name=f"{config['parent_run_name']}-validation_epoch={config['epoch']}_batch_idx={config['batch_idx']}"
        ),
    )
    result = trainer.fit()
    return {
        "score": result.metrics["score"],
        "validation_time": time.time() - start_time,
    }


# ==== End TorchTrainer approach ======


def validate_and_report(
    model,
    epoch,
    batch_idx,
    blocked_times,
    config,
    loss,
):
    validate_within_trainer = config["validate_within_trainer"]
    num_epochs = config["num_epochs"]
    checkpoint_upload_mode = config["checkpoint_upload_mode"]
    validate_fn = config["validate_fn"]
    if validate_within_trainer:
        test_dataloader = ray.train.get_dataset_shard("test").iter_torch_batches(
            batch_size=128
        )

    # Validate model within training loop
    val_elapsed_time = None
    if validate_within_trainer:
        val_start_time = time.time()
        mean_acc = torchmetrics.Accuracy(
            task="multiclass", num_classes=10, top_k=1
        ).cuda()
        model.eval()
        with torch.no_grad():
            for batch in test_dataloader:
                X, y = batch["image"], batch["label"]
                outputs = model(X)
                mean_acc(outputs.argmax(1), y)
        val_elapsed_time = time.time() - val_start_time

    # Report metrics + checkpoint + validate
    metrics = {"loss": loss.item(), "epoch": epoch}
    if validate_within_trainer and epoch == num_epochs - 1:
        metrics["score"] = mean_acc.compute().item()
    if ray.train.get_context().get_world_rank() == 0:
        if val_elapsed_time:
            metrics["validation_time"] = val_elapsed_time
        iteration_checkpoint_dir = tempfile.mkdtemp()
        torch.save(
            model.module.state_dict(),
            os.path.join(iteration_checkpoint_dir, "model.pt"),
        )
        start_time = time.time()
        if validate_fn:
            validate_config = {
                "dataset": config["test"],
                "parent_run_name": ray.train.get_context().get_experiment_name(),
                "epoch": epoch,
                "batch_idx": batch_idx,
            }
        else:
            validate_config = None
        ray.train.report(
            metrics,
            checkpoint=ray.train.Checkpoint.from_directory(iteration_checkpoint_dir),
            checkpoint_upload_mode=checkpoint_upload_mode,
            validate_fn=validate_fn,
            validate_config=validate_config,
        )
        blocked_times.append(time.time() - start_time)
    else:
        ray.train.report({}, None)


def train_func(config):
    batch_size = 256
    num_epochs = config["num_epochs"]
    midpoint_batch = int(config["rows_per_worker"] / batch_size / 2)

    # Prepare model, dataloader, and possibly metrics
    model = create_model()
    model = ray.train.torch.prepare_model(model)
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)
    train_data_shard = ray.train.get_dataset_shard("train")
    train_dataloader = train_data_shard.iter_torch_batches(batch_size=batch_size)

    # Train / eval / report loop
    blocked_times = []
    for epoch in range(num_epochs):

        # Train model, then validate/report at midpoint and end of epoch
        model.train()
        i = 0
        for i, batch in enumerate(train_dataloader):
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if i == midpoint_batch:
                validate_and_report(model, epoch, i, blocked_times, config, loss)
        validate_and_report(model, epoch, i, blocked_times, config, loss)

    # Report train_func metrics with dummy checkpoint since that is the only way to
    # return metrics
    if ray.train.get_context().get_world_rank() == 0:
        with tempfile.TemporaryDirectory() as temp_dir:
            ray.train.report(
                metrics={
                    "report_blocked_times": blocked_times,
                    "train_func_return_time": time.time(),
                },
                checkpoint=ray.train.Checkpoint.from_directory(temp_dir),
            )
    else:
        ray.train.report({}, None)


def run_training_with_validation(
    checkpoint_upload_mode: CheckpointUploadMode,
    validate_fn: Optional[Callable],
    validate_within_trainer: bool,
    num_epochs: int,
    train_dataset: ray.data.Dataset,
    test_dataset: ray.data.Dataset,
    training_rows: int,
):
    # Launch distributed training job.
    start_time = time.time()
    scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)
    datasets = {"train": train_dataset}
    train_loop_config = {
        "validate_within_trainer": validate_within_trainer,
        "num_epochs": num_epochs,
        "checkpoint_upload_mode": checkpoint_upload_mode,
        "validate_fn": validate_fn,
        "rows_per_worker": training_rows / 2,
    }
    if validate_within_trainer:
        datasets["test"] = test_dataset
    else:
        train_loop_config["test"] = test_dataset
    trainer = ray.train.torch.TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets=datasets,
        run_config=ray.train.RunConfig(
            storage_path="/mnt/cluster_storage",
        ),
    )
    result = trainer.fit()
    end_time = time.time()

    # Return metrics
    # TODO: consider measuring how long it takes to kick off validation,
    # how long checkpoint upload takes, distribution of times
    train_func_metrics = result.best_checkpoints[-1][1]
    metrics = {}
    metrics["e2e_time"] = end_time - start_time
    metrics["final_validation_waiting_time"] = (
        end_time - train_func_metrics["train_func_return_time"]
    )
    metrics["total_report_blocked_time"] = sum(
        train_func_metrics["report_blocked_times"]
    )
    metrics["total_validation_time"] = sum(
        t[1]["validation_time"] for t in result.best_checkpoints[:-1]
    )
    metrics["final_score"] = result.best_checkpoints[-2][1]["score"]
    return metrics


def main():
    train_dataset = ray.data.read_parquet(f"{STORAGE_PATH}/cifar10-parquet/train").map(
        transform_cifar
    )
    training_rows = train_dataset.count()
    test_dataset = ray.data.read_parquet(f"{STORAGE_PATH}/cifar10-parquet/test").map(
        transform_cifar
    )
    consolidated_metrics = {}
    num_epochs = 10
    consolidated_metrics["sync_cp_inline_val_metrics"] = run_training_with_validation(
        CheckpointUploadMode.SYNC,
        None,
        True,
        num_epochs,
        train_dataset,
        test_dataset,
        training_rows,
    )
    consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC,
        validate_with_torch_trainer,
        False,
        num_epochs,
        train_dataset,
        test_dataset,
        training_rows,
    )
    consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC,
        validate_with_map_batches,
        False,
        num_epochs,
        train_dataset,
        test_dataset,
        training_rows,
    )
    logger.info(consolidated_metrics)
    safe_write_to_results_json(consolidated_metrics)

    # Assert final scores aren't too far off, which would imply an inaccurate comparison
    # Example value: 0.55
    sync_final_score = consolidated_metrics["sync_cp_inline_val_metrics"]["final_score"]
    async_torchtrainer_final_score = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["final_score"]
    async_map_batches_final_score = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["final_score"]
    assert (
        abs(sync_final_score - async_torchtrainer_final_score)
        < MAXIMUM_ALLOWED_ACCURACY_DIFF
        and abs(sync_final_score - async_map_batches_final_score)
        < MAXIMUM_ALLOWED_ACCURACY_DIFF
    )

    # Assert async checkpointing/validation e2e time is faster; add multipler to account for training time variance
    # Example values: 1385s vs 1317s vs 1304s
    sync_e2e_time = consolidated_metrics["sync_cp_inline_val_metrics"]["e2e_time"]
    async_torchtrainer_e2e_time = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["e2e_time"]
    async_map_batches_e2e_time = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["e2e_time"]
    assert (
        async_torchtrainer_e2e_time
        < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
        and async_map_batches_e2e_time
        < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
    )

    # map_batches is faster than TorchTrainer. Note that inline is the fastest but is blocking
    # Example values: 92s vs 387s vs 264s (gap between sync and async smaller if more data)
    sync_validation_time = consolidated_metrics["sync_cp_inline_val_metrics"][
        "total_validation_time"
    ]

    # Assert report blocking time is (way) less with async checkpointing
    # Example values: 3.66s vs 0.033s
    sync_report_blocked_time = consolidated_metrics["sync_cp_inline_val_metrics"][
        "total_report_blocked_time"
    ]
    async_torchtrainer_report_blocked_time = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["total_report_blocked_time"]
    async_map_batches_report_blocked_time = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["total_report_blocked_time"]
    assert (
        async_torchtrainer_report_blocked_time < sync_report_blocked_time
        and async_map_batches_report_blocked_time < sync_report_blocked_time
    )

    # Assert sync blocking time (report + validation + final validation) is less than async blocking time (report + final validation)
    # Example values of final validation blocking time: 40s vs 26s
    sync_final_validation_blocking_time = consolidated_metrics[
        "sync_cp_inline_val_metrics"
    ]["final_validation_waiting_time"]
    async_torchtrainer_final_validation_blocking_time = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["final_validation_waiting_time"]
    async_map_batches_final_validation_blocking_time = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["final_validation_waiting_time"]
    sync_blocking_time = (
        sync_report_blocked_time
        + sync_validation_time
        + sync_final_validation_blocking_time
    )
    async_torchtrainer_blocking_time = (
        async_torchtrainer_report_blocked_time
        + async_torchtrainer_final_validation_blocking_time
    )
    async_map_batches_blocking_time = (
        async_map_batches_report_blocked_time
        + async_map_batches_final_validation_blocking_time
    )
    assert (
        sync_blocking_time > async_torchtrainer_blocking_time
        and sync_blocking_time > async_map_batches_blocking_time
    )

    # TODO: consider correctness checks like validating that local checkpoints get deleted
    # TODO: track validation startup metrics: schedule validation task, autoscale nodes,
    # start TorchTrainer/map_batches, load checkpoint.


if __name__ == "__main__":
    main()
