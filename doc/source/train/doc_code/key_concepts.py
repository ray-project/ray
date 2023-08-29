# flake8: noqa
# isort: skip_file

# __session_report_start__
from ray import train
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    for i in range(10):
        train.report({"step": i})


trainer = DataParallelTrainer(
    train_loop_per_worker=train_fn, scaling_config=train.ScalingConfig(num_workers=1)
)
trainer.fit()

# __session_report_end__


# __session_data_info_start__
import ray.data

from ray.train import ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    context = ray.train.get_context()
    dataset_shard = train.get_dataset_shard("train")

    ray.train.report(
        {
            # Global world size
            "world_size": context.get_world_size(),
            # Global worker rank on the cluster
            "world_rank": context.get_world_rank(),
            # Local worker rank on the current machine
            "local_rank": context.get_local_rank(),
            # Data
            "data_shard": next(iter(dataset_shard.iter_batches(batch_format="pandas"))),
        }
    )


trainer = DataParallelTrainer(
    train_loop_per_worker=train_fn,
    scaling_config=ScalingConfig(num_workers=2),
    datasets={"train": ray.data.from_items([1, 2, 3, 4])},
)
trainer.fit()
# __session_data_info_end__


# __session_checkpoint_start__
import json
import os
import tempfile

from ray import train
from ray.train import ScalingConfig, Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    checkpoint = train.get_checkpoint()

    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint.json"), "r") as f:
                state = json.load(f)
            state["step"] += 1
    else:
        state = {"step": 0}

    for i in range(state["step"], 10):
        state["step"] += 1
        with tempfile.TemporaryDirectory() as tempdir:
            with open(os.path.join(tempdir, "checkpoint.json"), "w") as f:
                json.dump(state, f)

            train.report(
                metrics={"step": state["step"], "loss": (100 - i) / 100},
                checkpoint=Checkpoint.from_directory(tempdir),
            )


example_checkpoint_dir = tempfile.mkdtemp()
with open(os.path.join(example_checkpoint_dir, "checkpoint.json"), "w") as f:
    json.dump({"step": 4}, f)

trainer = DataParallelTrainer(
    train_loop_per_worker=train_fn,
    scaling_config=ScalingConfig(num_workers=1),
    resume_from_checkpoint=Checkpoint.from_directory(example_checkpoint_dir),
)
trainer.fit()

# __session_checkpoint_end__


# __scaling_config_start__
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    # Number of distributed workers.
    num_workers=2,
    # Turn on/off GPU.
    use_gpu=True,
    # Specify resources used for trainer.
    trainer_resources={"CPU": 1},
    # Try to schedule workers on different nodes.
    placement_strategy="SPREAD",
)
# __scaling_config_end__

# __run_config_start__
from ray.train import RunConfig
from ray.air.integrations.wandb import WandbLoggerCallback

run_config = RunConfig(
    # Name of the training run (directory name).
    name="my_train_run",
    # The experiment results will be saved to: storage_path/name
    storage_path=os.path.expanduser("~/ray_results"),
    # storage_path="s3://my_bucket/tune_results",
    # Custom and built-in callbacks
    callbacks=[WandbLoggerCallback()],
    # Stopping criteria
    stop={"training_iteration": 10},
)
# __run_config_end__

# __failure_config_start__
from ray.train import RunConfig, FailureConfig

run_config = RunConfig(
    failure_config=FailureConfig(
        # Tries to recover a run up to this many times.
        max_failures=2
    )
)
# __failure_config_end__

# __checkpoint_config_start__
from ray.train import RunConfig, CheckpointConfig

run_config = RunConfig(
    checkpoint_config=CheckpointConfig(
        # Only keep the 2 *best* checkpoints and delete the others.
        num_to_keep=2,
        # *Best* checkpoints are determined by these params:
        checkpoint_score_attribute="mean_accuracy",
        checkpoint_score_order="max",
    ),
    # This will store checkpoints on S3.
    storage_path="s3://remote-bucket/location",
)
# __checkpoint_config_end__

# __checkpoint_config_ckpt_freq_start__
from ray.train import RunConfig, CheckpointConfig

run_config = RunConfig(
    checkpoint_config=CheckpointConfig(
        # Checkpoint every iteration.
        checkpoint_frequency=1,
        # Only keep the latest checkpoint and delete the others.
        num_to_keep=1,
    )
)

# from ray.train.xgboost import XGBoostTrainer
# trainer = XGBoostTrainer(..., run_config=run_config)
# __checkpoint_config_ckpt_freq_end__


# __result_metrics_start__
result = trainer.fit()

print("Observed metrics:", result.metrics)
# __result_metrics_end__


# __result_dataframe_start__
df = result.metrics_dataframe
print("Minimum loss", min(df["loss"]))
# __result_dataframe_end__


# __result_checkpoint_start__
print("Last checkpoint:", result.checkpoint)

with result.checkpoint.as_directory() as tmpdir:
    # Load model from directory
    ...
# __result_checkpoint_end__

# __result_best_checkpoint_start__
# Print available checkpoints
for checkpoint, metrics in result.best_checkpoints:
    print("Loss", metrics["loss"], "checkpoint", checkpoint)

# Get checkpoint with minimal loss
best_checkpoint = min(result.best_checkpoints, key=lambda bc: bc[1]["loss"])[0]

with best_checkpoint.as_directory() as tmpdir:
    # Load model from directory
    ...
# __result_best_checkpoint_end__

# __result_path_start__
result_path = result.path
print("Results location", result_path)
# __result_path_end__


# TODO(justinvyu): Re-enable this after updating all of doc_code.
# __result_restore_start__
from ray.train import Result

restored_result = Result.from_path(result_path)
print("Restored loss", result.metrics["loss"])
# __result_restore_end__


# __result_error_start__
if result.error:
    assert isinstance(result.error, Exception)

    print("Got exception:", result.error)
# __result_error_end__
