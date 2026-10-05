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

# __preemption_jit_checkpoint_start__
import os
import tempfile
import uuid

import torch

import ray.train
import ray.train.torch


def save_checkpoint(model: torch.nn.Module, step: int):
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        checkpoint = None
        # Save the checkpoint from rank 0, which holds a full model replica.
        if ray.train.get_context().get_world_rank() == 0:
            torch.save(
                {"model": model.state_dict(), "step": step},
                os.path.join(temp_checkpoint_dir, "checkpoint.pt"),
            )
            checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)
        # Call `report` on every rank, with or without a checkpoint.
        ray.train.report({"step": step}, checkpoint=checkpoint)


def train_fn_per_worker(config: dict):
    model = torch.nn.Linear(8, 1)
    start_step = 0

    # [1] Resume from the latest checkpoint, which can be a just-in-time one.
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            state = torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
            model.load_state_dict(state["model"])
            start_step = state["step"] + 1

    saved_on_preemption = False
    for step in range(start_step, config["num_steps"]):
        ...  # Run a training step.

        # [2] Call `get_preemption_info` on every rank the same number of
        # times, because it synchronizes the answer across all workers.
        preemption_info = ray.train.get_preemption_info()
        if preemption_info is not None and not saved_on_preemption:
            # Save one extra checkpoint before the node is preempted, then
            # keep training until Ray Train restarts the run.
            save_checkpoint(model, step)
            saved_on_preemption = True


trainer = ray.train.torch.TorchTrainer(
    train_fn_per_worker,
    train_loop_config={"num_steps": 10},
    scaling_config=ray.train.ScalingConfig(num_workers=2),
    run_config=ray.train.RunConfig(
        # (If multi-node, configure S3 / NFS as the storage path.)
        # storage_path="s3://...",
        name=f"train_run-{uuid.uuid4().hex}",
        failure_config=ray.train.FailureConfig(
            # [3] Retry preemptions from a budget that's separate from
            # `max_failures`. The default of -1 retries without a limit.
            max_preemption_failures=-1,
            max_failures=3,
        ),
    ),
)
trainer.fit()
# __preemption_jit_checkpoint_end__

# __preemption_relax_collectives_start__
import ray.train

failure_config = ray.train.FailureConfig(
    max_preemption_failures=-1,
    # Complete `report` and `get_preemption_info` without the workers on
    # the preempted node.
    relax_collectives_on_preemption=True,
    # Keep the surviving workers running for up to two minutes past the
    # drain deadline, so that a slow checkpoint upload can finish.
    preemption_grace_s=120.0,
)
# __preemption_relax_collectives_end__


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
