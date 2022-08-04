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
