from enum import Enum
import logging
import os
import tempfile
import time

import torch
import torch.distributed.checkpoint as dist_cp
import torchmetrics
from torch.distributed.checkpoint.state_dict import get_state_dict
from torch.distributed.checkpoint.state_dict_saver import async_save
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torchvision import transforms
from torchvision.models import VisionTransformer
from torchvision.transforms import ToTensor, Normalize
import ray
import ray.train
import ray.train.torch
from ray.train import CheckpointUploadMode, ValidationConfig, ValidationTaskConfig
from ray._private.test_utils import safe_write_to_results_json


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ValidationType(Enum):
    INLINE = "inline"
    TORCH_TRAINER = "torch_trainer"
    MAP_BATCHES = "map_batches"


class CheckpointSaveMode(Enum):
    # save to disk with torch.save
    TORCH_SAVE = "torch_save"
    # synchronous save via Torch DCP
    TORCH_DCP_SYNC = "torch_dcp_sync"
    # asynchronous save, Ray Train's background thread waits for completion.
    TORCH_DCP_ASYNC = "torch_dcp_async"


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


validation_dataset = ray.data.read_parquet(f"{STORAGE_PATH}/cifar10-parquet/test").map(
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
            model_pt = os.path.join(checkpoint_dir, "model.pt")
            if os.path.exists(model_pt):
                self.model.load_state_dict(torch.load(model_pt))
            else:
                state_dict = {"model": self.model.state_dict()}
                dist_cp.load(
                    state_dict,
                    storage_reader=dist_cp.FileSystemReader(checkpoint_dir),
                )
                self.model.load_state_dict(state_dict["model"])

        self.model.cuda().eval()

    def __call__(self, batch):
        image = torch.as_tensor(batch["image"], dtype=torch.float32, device="cuda")
        label = torch.as_tensor(batch["label"], dtype=torch.int8, device="cuda")
        pred = self.model(image)
        return {"res": (pred.argmax(1) == label).cpu().numpy()}


def validate_with_map_batches(checkpoint):
    start_time = time.time()
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
        "validation_time": time.time() - start_time,
    }


# ==== End map_batches approach ======

# ==== Start TorchTrainer approach ======


def eval_only_train_func(config_dict):
    # Load the checkpoint
    model = create_model()

    checkpoint = config_dict["checkpoint"]
    with checkpoint.as_directory() as checkpoint_dir:
        model_pt = os.path.join(checkpoint_dir, "model.pt")
        if os.path.exists(model_pt):
            model.load_state_dict(torch.load(model_pt))
        else:
            state_dict = {"model": model.state_dict()}
            dist_cp.load(
                state_dict,
                storage_reader=dist_cp.FileSystemReader(checkpoint_dir),
            )
            model.load_state_dict(state_dict["model"])

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

    # TODO - Replace with `return {"score": mean_acc.compute().item()}`
    ray.train.report(
        metrics={"score": mean_acc.compute().item()},
        checkpoint=ray.train.Checkpoint(
            ray.train.get_context()
            .get_storage()
            .build_checkpoint_path_from_name("placeholder")
        ),
        checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
    )


def validate_with_torch_trainer(checkpoint, parent_run_name, epoch, batch_idx):
    start_time = time.time()
    trainer = ray.train.torch.TorchTrainer(
        eval_only_train_func,
        train_loop_config={"checkpoint": checkpoint},
        scaling_config=ray.train.ScalingConfig(num_workers=2, use_gpu=True),
        datasets={"test": validation_dataset},
        run_config=ray.train.RunConfig(
            name=f"{parent_run_name}-validation_epoch={epoch}_batch_idx={batch_idx}"
        ),
    )
    result = trainer.fit()
    return {
        # TODO Update to `result.return_value["score"]`
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
    validation_type = config["validation_type"]
    checkpoint_save_mode = config["checkpoint_save_mode"]

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

    # Record how long the upload process takes
    start_time = time.time()

    # DCP save is a distributed collective so all ranks must call it together.
    ckpt_ref = None  # Only used by TORCH_DCP_ASYNC
    iteration_checkpoint_dir = None
    if checkpoint_save_mode in (
        CheckpointSaveMode.TORCH_DCP_SYNC,
        CheckpointSaveMode.TORCH_DCP_ASYNC,
    ):
        # For DCP, all workers write shards to the same shared storage path so that
        # the full checkpoint is available without any upload step.
        iteration_checkpoint_dir = (
            ray.train.get_context()
            .get_storage()
            .build_checkpoint_path_from_name(f"dcp_epoch_{epoch}_batch_{batch_idx}")
        )

        storage_writer = dist_cp.FileSystemWriter(iteration_checkpoint_dir)
        model_dict, _ = get_state_dict(model=model, optimizers=())

        if checkpoint_save_mode == CheckpointSaveMode.TORCH_DCP_SYNC:
            # Save via Torch DCP
            dist_cp.save({"model": model_dict}, storage_writer=storage_writer)
        elif checkpoint_save_mode == CheckpointSaveMode.TORCH_DCP_ASYNC:
            # Initiate async save; rank 0 will wait via checkpoint_upload_fn
            ckpt_ref = async_save({"model": model_dict}, storage_writer=storage_writer)
        else:
            raise NotImplementedError

    if ray.train.get_context().get_world_rank() == 0:
        if val_elapsed_time:
            metrics["validation_time"] = val_elapsed_time

        if validation_type == ValidationType.TORCH_TRAINER:
            validation = ValidationTaskConfig(
                fn_kwargs={
                    "parent_run_name": ray.train.get_context().get_experiment_name(),
                    "epoch": epoch,
                    "batch_idx": batch_idx,
                }
            )
        elif validation_type == ValidationType.MAP_BATCHES:
            validation = True
        else:
            validation = False

        if checkpoint_save_mode == CheckpointSaveMode.TORCH_SAVE:
            # We can't use `tempfile.TemporaryDirectory()` due to CheckpointUploadMode.ASYNC
            iteration_checkpoint_dir = tempfile.mkdtemp()
            torch.save(
                model.module.state_dict(),
                os.path.join(iteration_checkpoint_dir, "model.pt"),
            )

            ray.train.report(
                metrics,
                checkpoint=ray.train.Checkpoint.from_directory(
                    iteration_checkpoint_dir
                ),
                checkpoint_upload_mode=checkpoint_upload_mode,
                validation=validation,
            )
        elif checkpoint_save_mode == CheckpointSaveMode.TORCH_DCP_SYNC:
            # Shards are already in shared storage; no upload needed.
            ray.train.report(
                metrics,
                checkpoint=ray.train.Checkpoint.from_directory(
                    iteration_checkpoint_dir
                ),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
                validation=validation,
            )
        elif checkpoint_save_mode == CheckpointSaveMode.TORCH_DCP_ASYNC:
            # Shards are being written directly to shared storage.
            # Wait for the async save to finish in a background thread before
            # marking the checkpoint as ready, but don't upload anything.
            def wait_async_save(
                checkpoint, checkpoint_dir_name, upload_complete_ref=ckpt_ref
            ):
                upload_complete_ref.result()
                # todo (mark): this might be unnecessary as the data isn't moving so should be able to use `return checkpoint`
                path = (
                    ray.train.get_context()
                    .get_storage()
                    .build_checkpoint_path_from_name(checkpoint_dir_name)
                )
                return ray.train.Checkpoint.from_directory(path)

            ray.train.report(
                metrics,
                checkpoint=ray.train.Checkpoint.from_directory(
                    iteration_checkpoint_dir
                ),
                checkpoint_upload_fn=wait_async_save,
                checkpoint_dir_name=f"dcp_epoch_{epoch}_batch_{batch_idx}",
                checkpoint_upload_mode=CheckpointUploadMode.ASYNC,
                # iteration_checkpoint_dir is already in shared storage so don't delete it.
                delete_local_checkpoint_after_upload=False,
                validation=validation,
            )
        else:
            raise NotImplementedError

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
    # TODO - Replace with `return metrics`
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
    validation_type: ValidationType,
    validate_within_trainer: bool,
    num_epochs: int,
    train_dataset: ray.data.Dataset,
    training_rows: int,
    checkpoint_save_mode: CheckpointSaveMode,
):
    # Launch distributed training job.
    start_time = time.time()
    scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

    if validation_type == ValidationType.INLINE:
        validation_config = None
    elif validation_type == ValidationType.TORCH_TRAINER:
        validation_config = ValidationConfig(validate_with_torch_trainer)
    elif validation_type == ValidationType.MAP_BATCHES:
        validation_config = ValidationConfig(validate_with_map_batches)
    else:
        raise NotImplementedError

    datasets = {"train": train_dataset}
    train_loop_config = {
        "validate_within_trainer": validate_within_trainer,
        "num_epochs": num_epochs,
        "checkpoint_upload_mode": checkpoint_upload_mode,
        "rows_per_worker": training_rows / 2,
        "validation_type": validation_type,
        "checkpoint_save_mode": checkpoint_save_mode,
    }
    if validate_within_trainer:
        datasets["test"] = validation_dataset

    # async_save additionally requires a CPU process group alongside the GPU one
    #   because it runs collectives in a background thread.
    if checkpoint_save_mode == CheckpointSaveMode.TORCH_DCP_ASYNC:
        torch_config = ray.train.torch.TorchConfig(backend="cpu:gloo,cuda:nccl")
    else:
        torch_config = None

    trainer = ray.train.torch.TorchTrainer(
        train_func,
        validation_config=validation_config,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets=datasets,
        torch_config=torch_config,
        run_config=ray.train.RunConfig(storage_path="/mnt/cluster_storage"),
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
    consolidated_metrics = {}
    num_epochs = 10
    consolidated_metrics["sync_cp_inline_val_metrics"] = run_training_with_validation(
        CheckpointUploadMode.SYNC,
        ValidationType.INLINE,
        True,
        num_epochs,
        train_dataset,
        training_rows,
        CheckpointSaveMode.TORCH_SAVE,
    )
    consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC,
        ValidationType.TORCH_TRAINER,
        False,
        num_epochs,
        train_dataset,
        training_rows,
        CheckpointSaveMode.TORCH_SAVE,
    )
    consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC,
        ValidationType.MAP_BATCHES,
        False,
        num_epochs,
        train_dataset,
        training_rows,
        CheckpointSaveMode.TORCH_SAVE,
    )
    consolidated_metrics[
        "sync_dcp_map_batches_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.NO_UPLOAD,
        ValidationType.MAP_BATCHES,
        False,
        num_epochs,
        train_dataset,
        training_rows,
        CheckpointSaveMode.TORCH_DCP_SYNC,
    )
    consolidated_metrics[
        "async_dcp_map_batches_val_metrics"
    ] = run_training_with_validation(
        CheckpointUploadMode.ASYNC,
        ValidationType.MAP_BATCHES,
        False,
        num_epochs,
        train_dataset,
        training_rows,
        CheckpointSaveMode.TORCH_DCP_ASYNC,
    )
    safe_write_to_results_json(consolidated_metrics)
    for run_name, metrics in consolidated_metrics.items():
        logger.info(f"{run_name}={metrics}")

    # Assert final scores aren't too far off, which would imply an inaccurate comparison
    # Example value: 0.55
    sync_final_score = consolidated_metrics["sync_cp_inline_val_metrics"]["final_score"]
    async_torchtrainer_final_score = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["final_score"]
    async_map_batches_final_score = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["final_score"]
    sync_dcp_final_score = consolidated_metrics["sync_dcp_map_batches_val_metrics"][
        "final_score"
    ]
    async_dcp_final_score = consolidated_metrics["async_dcp_map_batches_val_metrics"][
        "final_score"
    ]
    logger.info(
        "Validation metrics order=",
        dict(
            sorted(
                ((k, v["final_score"]) for k, v in consolidated_metrics.items()),
                key=lambda a: a[1],
            )
        ),
    )

    assert (
        abs(sync_final_score - async_torchtrainer_final_score)
        < MAXIMUM_ALLOWED_ACCURACY_DIFF
    )
    assert (
        abs(sync_final_score - async_map_batches_final_score)
        < MAXIMUM_ALLOWED_ACCURACY_DIFF
    )
    assert abs(sync_final_score - sync_dcp_final_score) < MAXIMUM_ALLOWED_ACCURACY_DIFF
    assert abs(sync_final_score - async_dcp_final_score) < MAXIMUM_ALLOWED_ACCURACY_DIFF

    # Assert async checkpointing/validation e2e time is faster; add multipler to account for training time variance
    # Example values: 1385s vs 1317s vs 1304s
    sync_e2e_time = consolidated_metrics["sync_cp_inline_val_metrics"]["e2e_time"]
    async_torchtrainer_e2e_time = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["e2e_time"]
    async_map_batches_e2e_time = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["e2e_time"]
    sync_dcp_e2e_time = consolidated_metrics["sync_dcp_map_batches_val_metrics"][
        "e2e_time"
    ]
    async_dcp_e2e_time = consolidated_metrics["async_dcp_map_batches_val_metrics"][
        "e2e_time"
    ]
    logger.info(
        "Total end-to-end time order=",
        dict(
            sorted(
                ((k, v["e2e_time"]) for k, v in consolidated_metrics.items()),
                key=lambda a: a[1],
            )
        ),
    )

    assert (
        async_torchtrainer_e2e_time
        < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
    ), f"{async_torchtrainer_e2e_time=}, {sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER=} ({sync_e2e_time=})"
    assert (
        async_map_batches_e2e_time < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
    ), f"{async_map_batches_e2e_time=}, {sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER=} ({sync_e2e_time=})"
    assert (
        sync_dcp_e2e_time < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
    ), f"{sync_dcp_e2e_time=}, {sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER=} ({sync_e2e_time=})"
    assert (
        async_dcp_e2e_time < sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER
    ), f"{async_dcp_e2e_time=}, {sync_e2e_time * MAXIMUM_ALLOWED_E2E_TIME_MULTIPLIER=} ({sync_e2e_time=})"

    # map_batches is faster than TorchTrainer. Note that inline is the fastest but is blocking
    # Example values: 92s vs 387s vs 264s (gap between sync and async smaller if more data)
    sync_validation_time = consolidated_metrics["sync_cp_inline_val_metrics"][
        "total_validation_time"
    ]

    sync_report_blocked_time = consolidated_metrics["sync_cp_inline_val_metrics"][
        "total_report_blocked_time"
    ]
    async_torchtrainer_report_blocked_time = consolidated_metrics[
        "async_cp_torch_trainer_val_metrics"
    ]["total_report_blocked_time"]
    async_map_batches_report_blocked_time = consolidated_metrics[
        "async_cp_map_batches_val_metrics"
    ]["total_report_blocked_time"]
    sync_dcp_report_blocked_time = consolidated_metrics[
        "sync_dcp_map_batches_val_metrics"
    ]["total_report_blocked_time"]
    async_dcp_report_blocked_time = consolidated_metrics[
        "async_dcp_map_batches_val_metrics"
    ]["total_report_blocked_time"]
    logger.info(
        "Total report blocked time order=",
        dict(
            sorted(
                (
                    (k, v["total_report_blocked_time"])
                    for k, v in consolidated_metrics.items()
                ),
                key=lambda a: a[1],
            )
        ),
    )

    # Assert report blocking time is less than with async checkpointing.
    # Example values: 3.66s vs 0.033s vs 0.028s
    assert async_torchtrainer_report_blocked_time < sync_report_blocked_time
    assert async_map_batches_report_blocked_time < sync_report_blocked_time
    assert sync_dcp_report_blocked_time < sync_report_blocked_time
    assert async_dcp_report_blocked_time < sync_dcp_report_blocked_time

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
    sync_dcp_final_validation_blocking_time = consolidated_metrics[
        "sync_dcp_map_batches_val_metrics"
    ]["final_validation_waiting_time"]
    async_dcp_final_validation_blocking_time = consolidated_metrics[
        "async_dcp_map_batches_val_metrics"
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
    sync_dcp_blocking_time = (
        sync_dcp_report_blocked_time + sync_dcp_final_validation_blocking_time
    )
    async_dcp_blocking_time = (
        async_dcp_report_blocked_time + async_dcp_final_validation_blocking_time
    )
    logger.info(
        "Total validation blocking time order=",
        dict(
            sorted(
                (
                    (
                        k,
                        v["total_report_blocked_time"]
                        + v["final_validation_waiting_time"],
                    )
                    for k, v in consolidated_metrics.items()
                ),
                key=lambda a: a[1],
            )
        ),
    )

    assert sync_blocking_time > async_torchtrainer_blocking_time
    assert sync_blocking_time > async_map_batches_blocking_time
    assert sync_blocking_time > sync_dcp_blocking_time
    assert sync_blocking_time > async_dcp_blocking_time

    # TODO: consider correctness checks like validating that local checkpoints get deleted
    # TODO: track validation startup metrics: schedule validation task, autoscale nodes,
    #   start TorchTrainer/map_batches, load checkpoint.


if __name__ == "__main__":
    main()
