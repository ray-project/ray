import os

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# __failure_config_start__
import ray.train

# Tries to recover a run up to this many times.
failure_config = ray.train.FailureConfig(max_failures=2)

# No limit on the number of retries.
failure_config = ray.train.FailureConfig(max_failures=-1)
# __failure_config_end__

# __worker_fault_tolerance_start__
import tempfile
import uuid

import ray.train
import ray.train.torch


def train_fn_per_worker(train_loop_config: dict):
    # [1] Train worker restoration logic.
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as temp_checkpoint_dir:
            # model.load_state_dict(torch.load(...))
            ...

    # [2] Checkpoint saving and reporting logic.
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        # torch.save(...)
        ray.train.report(
            {"loss": 0.1},
            checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
        )


trainer = ray.train.torch.TorchTrainer(
    train_fn_per_worker,
    scaling_config=ray.train.ScalingConfig(num_workers=4),
    run_config=ray.train.RunConfig(
        # (If multi-node, configure S3 / NFS as the storage path.)
        # storage_path="s3://...",
        name=f"train_run-{uuid.uuid4().hex}",
        # [3] Enable worker-level fault tolerance to gracefully handle
        # Train worker failures.
        failure_config=ray.train.FailureConfig(max_failures=3),
    ),
)
trainer.fit()
# __worker_fault_tolerance_end__

# Avoid running the code below so that the argument parser is not used.
__name__ = "__dummy__"

# __job_driver_fault_tolerance_start__
# entrypoint.py

import argparse
import tempfile
import uuid

import ray.train
import ray.train.torch


def train_fn_per_worker(train_loop_config: dict):
    # [1] Train worker restoration logic.
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as temp_checkpoint_dir:
            # model.load_state_dict(torch.load(...))
            ...

    # [2] Checkpoint saving and reporting logic.
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        # torch.save(...)
        ray.train.report(
            {"loss": 0.1},
            checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_path", type=str, required=True)
    parser.add_argument("--run_name", type=str, required=True)
    args = parser.parse_args()

    trainer = ray.train.torch.TorchTrainer(
        train_fn_per_worker,
        scaling_config=ray.train.ScalingConfig(num_workers=4),
        run_config=ray.train.RunConfig(
            # [3] Enable worker-level fault tolerance to gracefully handle
            # Train worker failures.
            failure_config=ray.train.FailureConfig(max_failures=3),
            # [4] (Recommendation) The (storage_path, name) pair should be
            # determined by the job submitter and passed in as arguments
            # to the entrypoint script.
            storage_path=args.storage_path,
            name=args.run_name,
        ),
    )
    trainer.fit()
# __job_driver_fault_tolerance_end__
