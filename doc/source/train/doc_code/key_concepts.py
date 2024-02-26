# flake8: noqa
# isort: skip_file

from pathlib import Path
import tempfile

from ray import train
from ray.train import Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    for i in range(3):
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            Path(temp_checkpoint_dir).joinpath("model.pt").touch()
            train.report(
                {"loss": i}, checkpoint=Checkpoint.from_directory(temp_checkpoint_dir)
            )


trainer = DataParallelTrainer(
    train_fn, scaling_config=train.ScalingConfig(num_workers=2)
)


# __run_config_start__
import os

from ray.train import RunConfig

run_config = RunConfig(
    # Name of the training run (directory name).
    name="my_train_run",
    # The experiment results will be saved to: storage_path/name
    storage_path=os.path.expanduser("~/ray_results"),
    # storage_path="s3://my_bucket/tune_results",
    # Stopping criteria
    stop={"training_iteration": 10},
)
# __run_config_end__

# __failure_config_start__
from ray.train import RunConfig, FailureConfig


# Tries to recover a run up to this many times.
run_config = RunConfig(failure_config=FailureConfig(max_failures=2))

# No limit on the number of retries.
run_config = RunConfig(failure_config=FailureConfig(max_failures=-1))
# __failure_config_end__

# __checkpoint_config_start__
from ray.train import RunConfig, CheckpointConfig

# Example 1: Only keep the 2 *most recent* checkpoints and delete the others.
run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=2))


# Example 2: Only keep the 2 *best* checkpoints and delete the others.
run_config = RunConfig(
    checkpoint_config=CheckpointConfig(
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
best_checkpoint = min(
    result.best_checkpoints, key=lambda checkpoint: checkpoint[1]["loss"]
)[0]

with best_checkpoint.as_directory() as tmpdir:
    # Load model from directory
    ...
# __result_best_checkpoint_end__

import pyarrow

# __result_path_start__
result_path: str = result.path
result_filesystem: pyarrow.fs.FileSystem = result.filesystem

print("Results location (fs, path) = ({result_filesystem}, {result_path})")
# __result_path_end__


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
