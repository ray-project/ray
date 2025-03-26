"""Train multi-node persistence/checkpoint release test.

This test is a multi-node version of `test_new_persistence.py` and is meant to
be run on a cluster with NFS or S3 storage configured.

This test also records timing metrics on checkpoint save (to disk), save (to storage),
and load (from storage) operations and outputs them as release test metrics.

Setup:
- 4x 8 CPU instances
- 8 workers, each allocated 4 CPUs

Test owner: justinvyu
"""

import collections
from contextlib import contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import pickle
import shutil
import subprocess
import time
from typing import Any, Dict
import uuid

import fsspec
import numpy as np
import pyarrow.fs
import pytest
import torch
import torch.distributed as dist

import ray
from ray import train
from ray._private.dict import flatten_dict
from ray.air.constants import TRAINING_ITERATION
from ray.air._internal.uri_utils import URI
from ray.train import Checkpoint
from ray.train.base_trainer import TrainingFailedError
from ray.train.torch import TorchTrainer


from test_new_persistence import (
    train_fn,
    _assert_storage_contents,
    _resume_from_checkpoint,
)

# Add a unique ID to the storage path to avoid collisions between release test runs.
TEST_ID = uuid.uuid4().hex[:4] + "_" + datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
CLOUD_TEST_DIR = (
    os.environ["ANYSCALE_ARTIFACT_STORAGE"] + f"/test-persistence-{TEST_ID}/"
)
NFS_TEST_DIR = f"/mnt/cluster_storage/test-persistence-{TEST_ID}/"


class TestConstants:
    NUM_ITERATIONS = 10  # == num_checkpoints == num_artifacts
    NUM_TRIALS = 2

    # 4 * 8 = 32 CPUs total
    NUM_WORKERS = 8
    NUM_CPUS_PER_WORKER = 4

    SCORE_KEY = "score"

    NUM_GB = 2
    NUM_MB = 10
    NUM_KB = 10


def update_output_json(metrics: Dict[str, Any]):
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    data = {}
    if os.path.exists(test_output_json):
        with open(test_output_json, "r") as f:
            data = json.load(f)

    data.update(metrics)
    with open(test_output_json, "w") as f:
        json.dump(data, f)


def create_checkpoint(checkpoint_dir: str) -> float:
    """Create a somewhat realistic checkpoint of a given size.

    Returns the time it takes to dump this checkpoint to disk."""
    start = time.perf_counter()
    # Small (1kb) files
    for i in range(TestConstants.NUM_KB):
        with open(os.path.join(checkpoint_dir, f"1kb-{i}.txt"), "w") as f:
            f.write("a" * 1024)

    # Medium files (1 mb)
    for i in range(TestConstants.NUM_MB):
        with open(os.path.join(checkpoint_dir, f"1mb-{i}.txt"), "w") as f:
            f.write("a" * 1024 * 1024)

    # Large files (1 gb)
    for i in range(TestConstants.NUM_GB):
        with open(os.path.join(checkpoint_dir, f"1gb-{i}.txt"), "w") as f:
            f.write("a" * 1024 * 1024 * 1024)
    return time.perf_counter() - start


def custom_restore_fn(checkpoint: Checkpoint):
    start = time.perf_counter()
    with checkpoint.as_directory() as checkpoint_dir:
        time_to_load = time.perf_counter() - start

        dist.barrier()
        time_tensor = torch.tensor([time_to_load, 1.0])
        dist.reduce(time_tensor, dst=0, op=dist.ReduceOp.SUM)

        if train.get_context().get_world_rank() == 0:
            aggregated_metrics = {"load": time_tensor[0].item() / time_tensor[1].item()}
            checkpoint.update_metadata(aggregated_metrics)
            print("[checkpoint] Restore metrics:\n", aggregated_metrics)

        # This is a file populated by the default saving logic in `train_fn`.
        with open(os.path.join(checkpoint_dir, "checkpoint.pkl"), "rb") as f:
            state = pickle.load(f)
            return state


@contextmanager
def custom_save_fn(temp_checkpoint_dir: str):
    time_to_save = create_checkpoint(temp_checkpoint_dir)

    start = time.perf_counter()
    yield  # train.report happens here
    time_to_report = time.perf_counter() - start

    # Do an all-gather and have rank 0 write the aggregated timing metrics
    dist.barrier()
    timing_metrics = torch.tensor([time_to_save, time_to_report, 1.0])
    dist.reduce(timing_metrics, dst=0, op=dist.ReduceOp.SUM)

    if train.get_context().get_world_rank() == 0:
        persisted_checkpoint = train.get_checkpoint()
        aggregated_metrics = {
            "save_to_disk": timing_metrics[0].item() / timing_metrics[2].item(),
            "report": timing_metrics[1].item() / timing_metrics[2].item(),
        }
        persisted_checkpoint.update_metadata(aggregated_metrics)
        print("[checkpoint] Save metrics:\n", aggregated_metrics)


def get_custom_cloud_fs() -> pyarrow.fs.FileSystem:
    fsspec_fs, _ = fsspec.core.url_to_fs(os.environ["ANYSCALE_ARTIFACT_STORAGE"])
    return pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fsspec_fs))


def strip_prefix(path: str) -> str:
    return path.replace("s3://", "").replace("gs://", "")


def delete_at_uri(uri: str):
    if uri.startswith("s3://"):
        subprocess.check_output(["aws", "s3", "rm", "--recursive", uri])
    elif uri.startswith("gs://"):
        subprocess.check_output(["gsutil", "-m", "rm", "-r", uri])
    else:
        raise NotImplementedError(f"Invalid URI: {uri}")


def download_from_uri(uri: str, local_path: str):
    if uri.startswith("s3://"):
        subprocess.check_output(["aws", "s3", "cp", "--recursive", uri, local_path])
    elif uri.startswith("gs://"):
        subprocess.check_output(
            ["gsutil", "-m", "cp", "-r", uri.rstrip("/") + "/*", local_path]
        )
    else:
        raise NotImplementedError(f"Invalid URI: {uri}")


@pytest.mark.parametrize(
    "root_path_storage_filesystem_label",
    [
        (CLOUD_TEST_DIR, None, "cloud"),
        (NFS_TEST_DIR, None, "nfs"),
        (strip_prefix(CLOUD_TEST_DIR), get_custom_cloud_fs(), "cloud+custom_fs"),
    ],
)
def test_trainer(root_path_storage_filesystem_label, tmp_path, monkeypatch):
    """Tests that a data parallel trainer can save and restore checkpoints to
    various storage types properly. Also records checkpoint save/restore timing.

    Here's the rundown of what this test does:
    1. Passes in a `custom_save_fn` and `custom_restore_fn` to the trainer to
       record how long the operations take, as well as save a large checkpoint.
       See `create_checkpoint` for details on the checkpoint contents.
    2. Configures the training loop to fail 3 times.
    3. Runs the trainer, which will fail 2 times and recover via FailureConfig.
       This first run will exit on the 3rd failure.
    4. Manually restores the trainer, which will restore from the 3rd failure and
       run to completion.
    5. Downloads the results from the storage path and asserts that the contents
       are all correct. See `ray.train.test_new_persistence` for the expected filetree.
    6. Tests a new run with `resume_from_checkpoint`.
    """
    ray.init(runtime_env={"working_dir": "."}, ignore_reinit_error=True)

    root_path, storage_filesystem, label = root_path_storage_filesystem_label
    storage_path = root_path + label
    checkpoint_config = train.CheckpointConfig(
        num_to_keep=TestConstants.NUM_ITERATIONS // 2
    )
    exp_name = "test_trainer"

    print(
        "\nSaving results under (storage_path, exp_name) = "
        f"({storage_path}, {exp_name})\n"
    )

    trainer = TorchTrainer(
        train_fn,
        train_loop_config={
            "in_trainer": True,
            "fail_iters": [3, 6, 8],
            "time_per_iter": 1.0,
            "num_iterations": TestConstants.NUM_ITERATIONS,
            "custom_save_fn": custom_save_fn,
            "custom_restore_fn": custom_restore_fn,
        },
        scaling_config=train.ScalingConfig(
            num_workers=TestConstants.NUM_WORKERS,
            trainer_resources={"CPU": 0},
            resources_per_worker={"CPU": TestConstants.NUM_CPUS_PER_WORKER},
        ),
        run_config=train.RunConfig(
            failure_config=train.FailureConfig(max_failures=2),
            name=exp_name,
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
            checkpoint_config=checkpoint_config,
            sync_config=train.SyncConfig(sync_artifacts=True),
        ),
    )
    print("\nStarting initial run.\n")
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    print("\nStarting manually restored run.\n")
    restored_trainer = TorchTrainer.restore(
        path=str(URI(storage_path) / exp_name),
        storage_filesystem=storage_filesystem,
    )
    result = restored_trainer.fit()
    print(result)

    print("\nAsserting contents of uploaded results.\n")
    local_inspect_dir = tmp_path / "inspect_dir"
    local_inspect_dir.mkdir()
    # Download the results from storage
    if "cloud" in label:
        # NOTE: Use the CLI to download, since the python libraries
        # (pyarrow, fsspec) aren't consistent across cloud platforms (s3, gs).
        cloud_uri = CLOUD_TEST_DIR + label
        print("\nDownloading from cloud URI:", cloud_uri, "\n")
        download_from_uri(cloud_uri, str(local_inspect_dir))
    elif label == "nfs":
        local_inspect_dir = Path(storage_path)
    else:
        raise NotImplementedError(f"Invalid storage type: {label}")

    _assert_storage_contents(
        local_inspect_dir,
        exp_name,
        checkpoint_config,
        "TorchTrainer",
        test_trainer=True,
        constants=TestConstants,
    )

    # Test `resume_from_checkpoint`
    _resume_from_checkpoint(
        result.checkpoint,
        expected_state={"iter": TestConstants.NUM_ITERATIONS - 1},
        storage_path=storage_path,
        storage_filesystem=storage_filesystem,
    )

    # Upload checkpoint save and restore timing release test metrics
    all_checkpoint_timing_metrics = collections.defaultdict(list)
    for checkpoint, _ in result.best_checkpoints:
        metadata = checkpoint.get_metadata()
        for metric, value in metadata.items():
            all_checkpoint_timing_metrics[metric].append(value)

    aggregated_metrics = {
        key: np.mean(values) for key, values in all_checkpoint_timing_metrics.items()
    }
    checkpoint_size_mb = (
        TestConstants.NUM_GB * 1000 + TestConstants.NUM_MB + TestConstants.NUM_KB / 1000
    )
    speeds = {
        key + "_speed_mbps": checkpoint_size_mb / time_s
        for key, time_s in aggregated_metrics.items()
    }
    # Add units as the suffix
    aggregated_metrics = {
        key + "_avg_s": time_s for key, time_s in aggregated_metrics.items()
    }
    aggregated_metrics.update(speeds)
    aggregated_metrics["checkpoint_size_mb"] = checkpoint_size_mb

    print(aggregated_metrics)
    update_output_json(flatten_dict({label: aggregated_metrics}))

    print("Deleting files from the run...")
    if "cloud" in label:
        # NOTE: Use the CLI to delete files on cloud, since the python libraries
        # (pyarrow, fsspec) aren't consistent across cloud platforms (s3, gs).
        delete_at_uri(CLOUD_TEST_DIR)
    elif label == "nfs":
        shutil.rmtree(NFS_TEST_DIR, ignore_errors=True)
    else:
        raise NotImplementedError(f"Invalid storage type: {label}")


def test_no_storage_error(tmp_path, monkeypatch):
    """Tests that an error is raised if you do multi-node checkpointing
    w/ no persistent storage configured."""
    ray.init(runtime_env={"working_dir": "."}, ignore_reinit_error=True)

    trainer = TorchTrainer(
        train_fn,
        train_loop_config={
            "in_trainer": True,
            "time_per_iter": 1.0,
            "num_iterations": TestConstants.NUM_ITERATIONS,
        },
        scaling_config=train.ScalingConfig(
            num_workers=TestConstants.NUM_WORKERS,
            trainer_resources={"CPU": 0},
            resources_per_worker={"CPU": TestConstants.NUM_CPUS_PER_WORKER},
        ),
        run_config=train.RunConfig(name="test_trainer", storage_path=None),
    )
    with pytest.raises(TrainingFailedError):
        trainer.fit()


def test_no_storage_no_checkpoints(tmp_path, monkeypatch):
    """Tests that it's ok to run multi-node with no persistent storage
    if you never report checkpoints."""
    ray.init(runtime_env={"working_dir": "."}, ignore_reinit_error=True)

    trainer = TorchTrainer(
        train_fn,
        train_loop_config={
            "in_trainer": True,
            "time_per_iter": 1.0,
            "num_iterations": TestConstants.NUM_ITERATIONS,
            # Don't report any checkpoints
            "no_checkpoint_ranks": list(range(TestConstants.NUM_WORKERS)),
        },
        scaling_config=train.ScalingConfig(
            num_workers=TestConstants.NUM_WORKERS,
            trainer_resources={"CPU": 0},
            resources_per_worker={"CPU": TestConstants.NUM_CPUS_PER_WORKER},
        ),
        run_config=train.RunConfig(
            failure_config=train.FailureConfig(max_failures=2),
            name="test_trainer",
            storage_path=None,
            sync_config=train.SyncConfig(sync_artifacts=True),
        ),
    )
    result = trainer.fit()

    assert result.metrics[TRAINING_ITERATION] == TestConstants.NUM_ITERATIONS
    assert len(result.metrics_dataframe) == TestConstants.NUM_ITERATIONS


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
