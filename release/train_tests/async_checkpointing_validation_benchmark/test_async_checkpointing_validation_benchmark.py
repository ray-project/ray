import logging
import os
import pytest
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

# ==== Start dataset and model creation ======

STORAGE_PATH_PREFIX = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "artifact_storage")
STORAGE_PATH = f"{STORAGE_PATH_PREFIX}/ray_summit_24_train_demo"


def transform_cifar(row: dict):
    transform = transforms.Compose(
        [ToTensor(), Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )
    row["image"] = transform(row["image"])
    return row


train_dataset = ray.data.read_parquet(f"{STORAGE_PATH}/cifar10-parquet/train").map(
    transform_cifar
)
test_dataset = ray.data.read_parquet(f"{STORAGE_PATH}/cifar10-parquet/test").map(
    transform_cifar
)


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
        label = torch.as_tensor(batch["label"], dtype=torch.float32, device="cuda")
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
    mean_acc = torchmetrics.Accuracy(task="multiclass", num_classes=10).cuda()
    with torch.no_grad():
        for batch in test_dataloader:
            images, labels = batch["image"], batch["label"]
            outputs = model(images)
            mean_acc(outputs.argmax(1) == labels)
    with tempfile.TemporaryDirectory() as temp_dir:
        ray.train.report(
            metrics={"score": mean_acc.compute().item()},
            checkpoint=ray.train.Checkpoint.from_directory(temp_dir),
            checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
        )


def validate_with_torch_trainer(checkpoint, config):
    start_time = time.time()
    trainer = ray.train.torch.TorchTrainer(
        eval_only_train_func,
        train_loop_config={"checkpoint": checkpoint},
        scaling_config=ray.train.ScalingConfig(num_workers=2, use_gpu=True),
        datasets={"test": config["dataset"]},
    )
    result = trainer.fit()
    return {
        "score": result.metrics["score"],
        "validation_time": time.time() - start_time,
    }


# ==== End TorchTrainer approach ======


def run_training_with_validation(
    checkpoint_upload_mode: CheckpointUploadMode,
    validate_fn: Optional[Callable],
    validate_within_trainer: bool,
    num_epochs: int = 10,
):
    def train_func(config):
        # Prepare model, dataloader, and possibly metrics
        model = create_model()
        model = ray.train.torch.prepare_model(model)
        criterion = CrossEntropyLoss()
        optimizer = Adam(model.parameters(), lr=0.001)
        train_data_shard = ray.train.get_dataset_shard("train")
        train_dataloader = train_data_shard.iter_torch_batches(batch_size=256)
        if validate_within_trainer:
            mean_acc = torchmetrics.Accuracy(task="multiclass", num_classes=10).cuda()
            test_dataloader = ray.train.get_dataset_shard("test").iter_torch_batches(
                batch_size=128
            )

        # Train / eval / report loop
        blocked_times = []
        rank = ray.train.get_context().get_world_rank()
        for epoch in range(num_epochs):

            # Train model
            model.train()
            for batch in train_dataloader:
                images, labels = batch["image"], batch["label"]
                outputs = model(images)
                loss = criterion(outputs, labels)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            # Validate model within training loop
            val_elapsed_time = None
            if validate_within_trainer:
                val_start_time = time.time()
                model.eval()
                with torch.no_grad():
                    for batch in test_dataloader:
                        X, y = batch["image"], batch["label"]
                        outputs = model(X)
                        mean_acc(outputs.argmax(1) == y)
                val_elapsed_time = time.time() - val_start_time

            # Report metrics + checkpoint + validate
            if rank == 0:
                metrics = {"loss": loss.item(), "epoch": epoch}
                if val_elapsed_time:
                    metrics["validation_time"] = val_elapsed_time
                iteration_checkpoint_dir = tempfile.mkdtemp()
                torch.save(
                    model.module.state_dict(),
                    os.path.join(iteration_checkpoint_dir, "model.pt"),
                )
                start_time = time.time()
                validate_config = {"dataset": test_dataset}
                ray.train.report(
                    metrics,
                    checkpoint=ray.train.Checkpoint.from_directory(
                        iteration_checkpoint_dir
                    ),
                    checkpoint_upload_mode=checkpoint_upload_mode,
                    validate_fn=validate_fn,
                    validate_config=validate_config,
                )
                blocked_times.append(time.time() - start_time)
            else:
                ray.train.report({}, None)

        # Report train_func metrics with dummy checkpoint since that is the only way to
        # return metrics
        if rank == 0:
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

    # Launch distributed training job.
    start_time = time.time()
    scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)
    datasets = {"train": train_dataset}
    if validate_within_trainer:
        datasets["test"] = test_dataset
    trainer = ray.train.torch.TorchTrainer(
        train_func,
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
        t[1]["validation_time"] for t in result.best_checkpoints[:num_epochs]
    )
    metrics["final_score"] = metrics.best_checkpoints[-2][1]["score"]
    return metrics


def test_async_checkpointing_validation_benchmark():
    consolidated_metrics = {}
    num_epochs = 10
    consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC, validate_with_map_batches, False, num_epochs
    )
    consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC, validate_with_torch_trainer, False, num_epochs
    )
    consolidated_metrics["sync_cp_inline_val_metrics"] = run_training_with_validation(
        CheckpointUploadMode.SYNC, None, True, num_epochs
    )
    logger.info(consolidated_metrics)
    safe_write_to_results_json(consolidated_metrics)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
