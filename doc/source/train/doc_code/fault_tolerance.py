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

# __job_driver_fault_tolerance_start__
import ray.train
import ray.train.torch


# __job_driver_fault_tolerance_end__
