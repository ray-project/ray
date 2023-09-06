import collections
from contextlib import contextmanager
import json
import os
import pickle
import time
from typing import Any, Dict

import numpy as np
import pytest
import torch
import torch.distributed as dist

from ray import train
from ray._private.dict import flatten_dict
from ray.train import Checkpoint
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
    NUM_ITERATIONS = 6  # == num_checkpoints == num_artifacts
    NUM_TRIALS = 2
    NUM_WORKERS = 8

    SCORE_KEY = "score"


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
    for i in range(10):
        with open(os.path.join(checkpoint_dir, f"1kb-{i}.txt"), "w") as f:
            f.write("a" * 1024)

    # 10 medium files (1 mb)
    for i in range(10):
        with open(os.path.join(checkpoint_dir, f"1mb-{i}.txt"), "w") as f:
            f.write("a" * 1024 * 1024)

    # 3 large files (1 gb)
    # for i in range(3):
    #     with open(os.path.join(checkpoint_dir, f"1gb-{i}.txt"), "w") as f:
    #         f.write("a" * 1024 * 1024 * 1024)
    return time.perf_counter() - start


def custom_restore_fn(checkpoint: Checkpoint):
    start = time.perf_counter()
    with checkpoint.as_directory() as checkpoint_dir:
        time_to_load = time.perf_counter() - start

        dist.barrier()
        time_tensor = torch.tensor([time_to_load])
        dist.reduce(time_tensor, dst=0, op=dist.ReduceOp.AVG)

        if train.get_context().get_world_rank() == 0:
            checkpoint.update_metadata({"time_to_load": time_tensor.item()})

        # This is a file populated by the
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
    timing_metrics = torch.tensor([time_to_save, time_to_report])
    dist.reduce(timing_metrics, dst=0, op=dist.ReduceOp.AVG)

    if train.get_context().get_world_rank() == 0:
        persisted_checkpoint = train.get_checkpoint()
        aggregated_metrics = {
            "time_to_save": timing_metrics[0].item(),
            "time_to_report": timing_metrics[1].item(),
        }
        persisted_checkpoint.update_metadata(aggregated_metrics)


@pytest.mark.parametrize(
    "storage_path_storage_filesystem_label",
    [
        (os.environ["ANYSCALE_ARTIFACT_STORAGE"] + "/justinvyu-testing", None, "cloud"),
        ("/mnt/cluster_storage", None, "nfs"),
    ],
)
def test_trainer(
    storage_path_storage_filesystem_label, checkpoint_config, tmp_path, monkeypatch
):
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(tmp_path / "ray_results"))

    storage_path, storage_filesystem, label = storage_path_storage_filesystem_label
    checkpoint_config = train.CheckpointConfig(num_to_keep=4)
    exp_name = "test_trainer"

    # Delete the existing contents at the storage path (ex: from previous runs)
    fs, storage_fs_path = get_fs_and_path(storage_path, storage_filesystem)
    experiment_fs_path = os.path.join(storage_fs_path, exp_name)
    if _exists_at_fs_path(fs, experiment_fs_path):
        _delete_fs_path(fs, experiment_fs_path)

    trainer = TorchTrainer(
        train_fn,
        train_loop_config={
            "fail_iters": [2, 4],
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
            failure_config=train.FailureConfig(max_failures=1),
            name="test_trainer",
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
            checkpoint_config=checkpoint_config,
            sync_config=train.SyncConfig(sync_artifacts=True),
        ),
    )
    print("\nStarting initial run.\n")
    # with pytest.raises(TrainingFailedError):
    result = trainer.fit()

    # print("\nStarting manually restored run.\n")
    # restored_trainer = TorchTrainer.restore(
    #     path=str(URI(storage_path or str(LOCAL_CACHE_DIR)) / exp_name),
    #     storage_filesystem=storage_filesystem,
    # )
    # result = restored_trainer.fit()

    local_inspect_dir = tmp_path / "inspect_dir"
    _download_from_fs_path(fs, storage_fs_path, str(local_inspect_dir))
    _assert_storage_contents(
        local_inspect_dir,
        exp_name,
        checkpoint_config,
        "TorchTrainer",
        test_trainer=True,
        constants=TestConstants,
    )

    all_checkpoint_timing_metrics = collections.defaultdict(list)
    for checkpoint, _ in result.best_checkpoints:
        metadata = checkpoint.get_metadata()
        for metric, value in metadata.items():
            all_checkpoint_timing_metrics[metric].append(value)

    aggregated_metrics = {
        key: np.mean(values) for key, values in all_checkpoint_timing_metrics.items()
    }
    print(aggregated_metrics)
    update_output_json(flatten_dict({label: aggregated_metrics}))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
