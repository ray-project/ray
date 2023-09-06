import collections
from contextlib import contextmanager
import json
import os
import pickle
import time
from typing import Any, Dict

import fsspec
import numpy as np
import pyarrow.fs
import pytest
import torch
import torch.distributed as dist

from ray import train
from ray._private.dict import flatten_dict
from ray.air._internal.uri_utils import URI
from ray.train import Checkpoint
from ray.train.base_trainer import TrainingFailedError
from ray.train.torch import TorchTrainer
from ray.train._internal.storage import (
    _exists_at_fs_path,
    _delete_fs_path,
    _download_from_fs_path,
    get_fs_and_path,
)

from ray.train.tests.test_new_persistence import (
    train_fn,
    _assert_storage_contents,
    _resume_from_checkpoint,
)


class TestConstants:
    NUM_ITERATIONS = 12  # == num_checkpoints == num_artifacts
    NUM_TRIALS = 2
    NUM_WORKERS = 8

    SCORE_KEY = "score"

    NUM_GB = 3
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


def create_checkpoint(checkpoint_dir) -> float:
    start = time.perf_counter()
    # 10 small (1kb) files
    for i in range(TestConstants.NUM_KB):
        with open(os.path.join(checkpoint_dir, f"1kb-{i}.txt"), "w") as f:
            f.write("a" * 1024)

    # 10 medium files (1 mb)
    for i in range(TestConstants.NUM_MB):
        with open(os.path.join(checkpoint_dir, f"1mb-{i}.txt"), "w") as f:
            f.write("a" * 1024 * 1024)

    # 3 large files (1 gb)
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


@pytest.mark.parametrize(
    "storage_path_storage_filesystem_label",
    [
        (os.environ["ANYSCALE_ARTIFACT_STORAGE"] + "/test-persistence", None, "cloud"),
        ("/mnt/cluster_storage/test-persistence", None, "nfs"),
        (
            os.environ["ANYSCALE_ARTIFACT_STORAGE"].replace("s3://", "")
            + "/test-persistence",
            get_custom_cloud_fs(),
            "cloud+custom_fs",
        ),
    ],
)
def test_trainer(storage_path_storage_filesystem_label, tmp_path, monkeypatch):
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(tmp_path / "ray_results"))

    storage_path, storage_filesystem, label = storage_path_storage_filesystem_label
    checkpoint_config = train.CheckpointConfig(
        num_to_keep=TestConstants.NUM_ITERATIONS // 2
    )
    exp_name = "test_trainer"

    # Delete the existing contents at the storage path (ex: from previous runs)
    fs, storage_fs_path = get_fs_and_path(storage_path, storage_filesystem)
    experiment_fs_path = os.path.join(storage_fs_path, exp_name)
    if _exists_at_fs_path(fs, experiment_fs_path):
        print("\nDeleting results from a previous run...\n")
        _delete_fs_path(fs, experiment_fs_path)

    trainer = TorchTrainer(
        train_fn,
        train_loop_config={
            "in_trainer": True,
            "fail_iters": [4, 8, 10],
            "time_per_iter": 1.0,
            "num_iterations": TestConstants.NUM_ITERATIONS,
            "custom_save_fn": custom_save_fn,
            "custom_restore_fn": custom_restore_fn,
        },
        scaling_config=train.ScalingConfig(
            num_workers=TestConstants.NUM_WORKERS,
            trainer_resources={"CPU": 0},
            resources_per_worker={"CPU": 8},
        ),
        run_config=train.RunConfig(
            failure_config=train.FailureConfig(max_failures=2),
            name="test_trainer",
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

    # First, inspect that the result object returns the correct paths.
    print(result)
    trial_fs_path = result.path
    assert trial_fs_path.startswith(storage_fs_path)
    for checkpoint, _ in result.best_checkpoints:
        assert checkpoint.path.startswith(trial_fs_path)

    print("\nAsserting contents of uploaded results.\n")
    local_inspect_dir = tmp_path / "inspect_dir"

    fs = result.filesystem
    if label == "cloud+custom_fs":
        # NOTE: Using a pyarrow-wrapped version of an fsspec filesystem
        # causes this download to segfault -- but checkpoints were still
        # uploaded properly during training.
        # Workaround: Just use the pyarrow default filesystem.
        fs, _ = pyarrow.fs.FileSystem.from_uri(os.environ["ANYSCALE_ARTIFACT_STORAGE"])

    _download_from_fs_path(fs, storage_fs_path, str(local_inspect_dir))
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
    # 3 GB + 10 MB + 10 KB = 3010.01 MB
    checkpoint_size_mb = (
        TestConstants.NUM_GB * 1000 + TestConstants.NUM_MB + TestConstants.NUM_KB / 1000
    )
    speeds = {
        key + "_speed_mbps": checkpoint_size_mb / time_s
        for key, time_s in aggregated_metrics.items()
    }
    aggregated_metrics.update(speeds)
    aggregated_metrics["checkpoint_size_mb"] = checkpoint_size_mb

    print(aggregated_metrics)
    update_output_json(flatten_dict({label: aggregated_metrics}))


def test_no_persistence(tmp_path):
    ...


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
