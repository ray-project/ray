# flake8: noqa
# isort: skip_file

# __session_report_start__
from ray.air import session, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    for i in range(10):
        session.report({"step": i})


trainer = DataParallelTrainer(
    train_loop_per_worker=train_fn, scaling_config=ScalingConfig(num_workers=1)
)
trainer.fit()

# __session_report_end__


# __session_data_info_start__
import ray.data
from ray.air import session, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    dataset_shard = session.get_dataset_shard("train")

    session.report(
        {
            # Global world size
            "world_size": session.get_world_size(),
            # Global worker rank on the cluster
            "world_rank": session.get_world_rank(),
            # Local worker rank on the current machine
            "local_rank": session.get_local_rank(),
            # Data
            "data_shard": dataset_shard.to_pandas().to_numpy().tolist(),
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
from ray.air import session, ScalingConfig, Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer


def train_fn(config):
    checkpoint = session.get_checkpoint()

    if checkpoint:
        state = checkpoint.to_dict()
    else:
        state = {"step": 0}

    for i in range(state["step"], 10):
        state["step"] += 1
        session.report(
            metrics={"step": state["step"]}, checkpoint=Checkpoint.from_dict(state)
        )


trainer = DataParallelTrainer(
    train_loop_per_worker=train_fn,
    scaling_config=ScalingConfig(num_workers=1),
    resume_from_checkpoint=Checkpoint.from_dict({"step": 4}),
)
trainer.fit()

# __session_checkpoint_end__


# __scaling_config_start__
from ray.air import ScalingConfig

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
from ray.air import RunConfig

run_config = RunConfig(
    # Name of the training run (directory name).
    name="my_train_run",
    # Directory to store results in (will be local_dir/name).
    local_dir="~/ray_results",
    # Low training verbosity.
    verbose=1,
)
# __run_config_end__

# __failure_config_start__
from ray.air import RunConfig, FailureConfig

run_config = RunConfig(
    failure_config=FailureConfig(
        # Tries to recover a run up to this many times.
        max_failures=2
    )
)
# __failure_config_end__

# __sync_config_start__
from ray.air import RunConfig
from ray.tune import SyncConfig

run_config = RunConfig(
    sync_config=SyncConfig(
        # This will store checkpoints on S3.
        upload_dir="s3://remote-bucket/location"
    )
)
# __sync_config_end__

# __checkpoint_config_start__
from ray.air import RunConfig, CheckpointConfig

run_config = RunConfig(
    checkpoint_config=CheckpointConfig(
        # Only keep this many checkpoints.
        num_to_keep=2
    )
)
# __checkpoint_config_end__


# __results_start__
result = trainer.fit()

# Print metrics
print("Observed metrics:", result.metrics)

checkpoint_data = result.checkpoint.to_dict()
print("Checkpoint data:", checkpoint_data["step"])
# __results_end__
