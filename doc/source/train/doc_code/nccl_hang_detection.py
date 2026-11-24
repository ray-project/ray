import os

# __enable_start__
import os
import uuid

import ray.train
import ray.train.torch

# Set these on the driver. Ray Train propagates them to its controller.
os.environ["RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR"] = "1"
# Log a confirmed hang and capture diagnostics without failing the run.
# Set to "fail" to reattempt the run instead.
os.environ["RAY_TRAIN_NCCL_RAS_ACTION"] = "observe"


def train_fn(train_loop_config: dict):
    # No changes to the training function are needed.
    ...


trainer = ray.train.torch.TorchTrainer(
    train_fn,
    scaling_config=ray.train.ScalingConfig(num_workers=2),
    run_config=ray.train.RunConfig(
        # Diagnostics are uploaded under <storage_path>/<name>/hang_detector/.
        # storage_path="s3://...",
        name=f"train_run-{uuid.uuid4().hex}",
    ),
)
trainer.fit()
# __enable_end__

# __fail_start__
import os
import uuid

import ray.train
import ray.train.torch

os.environ["RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR"] = "1"
# Reattempt the run on a confirmed hang, through the failure policy.
os.environ["RAY_TRAIN_NCCL_RAS_ACTION"] = "fail"
# Confirm a hang after 5 minutes frozen. Keep this well below
# TorchConfig(timeout_s), which defaults to 1800 seconds.
os.environ["RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S"] = "300"
# Arm the PyTorch Flight Recorder so the detector can dump the in-flight
# collective of every rank. It's propagated to the workers for you.
os.environ["TORCH_FR_BUFFER_SIZE"] = "2000"


def train_fn(train_loop_config: dict):
    ...


trainer = ray.train.torch.TorchTrainer(
    train_fn,
    scaling_config=ray.train.ScalingConfig(num_workers=2),
    run_config=ray.train.RunConfig(
        name=f"train_run-{uuid.uuid4().hex}",
        # A confirmed hang counts as a worker failure, so each one uses one
        # retry.
        failure_config=ray.train.FailureConfig(max_failures=2),
    ),
)
trainer.fit()
# __fail_end__
