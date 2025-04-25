# flake8: noqa
# isort: skip_file

import os

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# __quickstart_start__
import random
import tempfile
import uuid

import ray.train
import ray.train.torch
import ray.tune
from ray.tune.integration.ray_train import TuneReportCallback


# [1] Define your Ray Train worker code.
def train_fn_per_worker(train_loop_config: dict):
    # Unpack train worker hyperparameters.
    # Train feeds in the `train_loop_config` defined below.
    lr = train_loop_config["lr"]

    # training code here...
    print(
        ray.train.get_context().get_world_size(),
        ray.train.get_context().get_world_rank(),
        train_loop_config,
    )
    # model = ray.train.torch.prepare_model(...)  # Wrap model in DDP.
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        ray.train.report(
            {"loss": random.random()},
            checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
        )


# [2] Define a function that launches the Ray Train run.
def train_driver_fn(config: dict):
    # Unpack run-level hyperparameters.
    # Tune feeds in hyperparameters defined in the `param_space` below.
    num_workers = config["num_workers"]

    trainer = ray.train.torch.TorchTrainer(
        train_fn_per_worker,
        train_loop_config=config["train_loop_config"],
        scaling_config=ray.train.ScalingConfig(
            num_workers=num_workers,
            # Uncomment to use GPUs.
            # use_gpu=True,
        ),
        run_config=ray.train.RunConfig(
            # [3] Assign unique names to each run.
            # Recommendation: use the trial id as part of the run name.
            name=f"train-trial_id={ray.tune.get_context().get_trial_id()}",
            # [4] (Optional) Pass in a `TuneReportCallback` to propagate
            # reported results to the Tuner.
            callbacks=[TuneReportCallback()],
            # (If multi-node, configure S3 / NFS as the storage path.)
            # storage_path="s3://...",
        ),
    )
    trainer.fit()


# Launch a single Train run.
train_driver_fn({"num_workers": 4, "train_loop_config": {"lr": 1e-3}})


# Launch a sweep of hyperparameters with Ray Tune.
tuner = ray.tune.Tuner(
    train_driver_fn,
    param_space={
        "num_workers": ray.tune.choice([2, 4]),
        "train_loop_config": {
            "lr": ray.tune.grid_search([1e-3, 3e-4]),
            "batch_size": ray.tune.grid_search([32, 64]),
        },
    },
    run_config=ray.tune.RunConfig(
        name=f"tune_train_example-{uuid.uuid4().hex[:6]}",
        # (If multi-node, configure S3 / NFS as the storage path.)
        # storage_path="s3://...",
    ),
    # [5] (Optional) Set the maximum number of concurrent trials
    # in order to prevent too many Train driver processes from
    # being launched at once.
    tune_config=ray.tune.TuneConfig(max_concurrent_trials=2),
)
results = tuner.fit()

print(results.get_best_result(metric="loss", mode="min"))
# __quickstart_end__

# __max_concurrent_trials_start__
# For a fixed size cluster, calculate this based on the limiting resource (ex: GPUs).
total_cluster_gpus = 8
num_gpu_workers_per_trial = 4
max_concurrent_trials = total_cluster_gpus // num_gpu_workers_per_trial


def train_driver_fn(config: dict):
    trainer = ray.train.torch.TorchTrainer(
        train_fn_per_worker,
        scaling_config=ray.train.ScalingConfig(
            num_workers=num_gpu_workers_per_trial, use_gpu=True
        ),
    )
    trainer.fit()


tuner = ray.tune.Tuner(
    train_driver_fn,
    tune_config=ray.tune.TuneConfig(max_concurrent_trials=max_concurrent_trials),
)
# __max_concurrent_trials_end__

# __trainable_resources_start__
# Cluster setup:
# head_node:
#     resources:
#         CPU: 16.0
# worker_node_cpu:
#     resources:
#         CPU: 32.0
#         TRAIN_DRIVER_RESOURCE: 1.0
# worker_node_gpu:
#     resources:
#         GPU: 4.0

import ray.tune


def train_driver_fn(config):
    # trainer = TorchTrainer(...)
    ...


tuner = ray.tune.Tuner(
    ray.tune.with_resources(
        train_driver_fn,
        # Note: 0.01 is an arbitrary value to schedule the actor
        # onto the `worker_node_cpu` node type.
        {"TRAIN_DRIVER_RESOURCE": 0.01},
    ),
)
# __trainable_resources_end__


# __fault_tolerance_start__
import tempfile

import ray.tune
import ray.train
import ray.train.torch


def train_fn_per_worker(train_loop_config: dict):
    # [1] Train worker restoration logic.
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as temp_checkpoint_dir:
            # model.load_state_dict(torch.load(...))
            ...

    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        # torch.save(...)
        ray.train.report(
            {"loss": 0.1},
            checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
        )


def train_fn_driver(config: dict):
    trainer = ray.train.torch.TorchTrainer(
        train_fn_per_worker,
        run_config=ray.train.RunConfig(
            # [2] Train driver restoration is automatic, as long as
            # the (storage_path, name) remains the same across trial restarts.
            # The easiest way to do this is to attach the trial ID in the name.
            # **Do not include any timestamps or random values in the name.**
            name=f"train-trial_id={ray.tune.get_context().get_trial_id()}",
            # [3] Enable worker-level fault tolerance to gracefully handle
            # Train worker failures.
            failure_config=ray.train.FailureConfig(max_failures=3),
            # (If multi-node, configure S3 / NFS as the storage path.)
            # storage_path="s3://...",
        ),
    )
    trainer.fit()


tuner = ray.tune.Tuner(
    train_fn_driver,
    run_config=ray.tune.RunConfig(
        # [4] Enable trial-level fault tolerance to gracefully handle
        # Train driver process failures.
        failure_config=ray.tune.FailureConfig(max_failures=3)
    ),
)
tuner.fit()
# __fault_tolerance_end__
