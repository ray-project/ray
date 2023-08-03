from contextlib import contextmanager
import os
from pathlib import Path
import pickle
import pytest
import tempfile
import time
from typing import Optional, Tuple

import pyarrow.fs

from ray import train, tune
from ray.air.constants import EXPR_RESULT_FILE
from ray.train._internal.storage import _download_from_fs_path
from ray.train._checkpoint import Checkpoint as NewCheckpoint
from ray.train.data_parallel_trainer import DataParallelTrainer

from ray.air.tests.test_checkpoints import mock_s3_bucket_uri


_SCORE_KEY = "score"


@contextmanager
def dummy_context_manager():
    yield "dummy value"


@pytest.fixture(autouse=True)
def enable_new_persistence_mode(monkeypatch):
    monkeypatch.setenv("RAY_AIR_NEW_PERSISTENCE_MODE", "1")
    yield
    monkeypatch.setenv("RAY_AIR_NEW_PERSISTENCE_MODE", "0")


def _create_mock_custom_fs(custom_fs_root_dir: Path) -> pyarrow.fs.FileSystem:
    from fsspec.implementations.dirfs import DirFileSystem
    from fsspec.implementations.local import LocalFileSystem

    custom_fs_root_dir.mkdir(parents=True, exist_ok=True)
    storage_filesystem = pyarrow.fs.PyFileSystem(
        pyarrow.fs.FSSpecHandler(
            DirFileSystem(path=str(custom_fs_root_dir), fs=LocalFileSystem())
        )
    )
    return storage_filesystem


@contextmanager
def _resolve_storage_type(
    storage_path_type: str, tmp_path: Path
) -> Tuple[str, Optional[pyarrow.fs.FileSystem]]:
    storage_path, storage_filesystem = None, None

    context_manager = (
        mock_s3_bucket_uri if storage_path_type == "cloud" else dummy_context_manager
    )

    with context_manager() as cloud_storage_path:
        if storage_path_type == "nfs":
            storage_path = str(tmp_path / "fake_nfs")
        elif storage_path_type == "cloud":
            storage_path = str(cloud_storage_path)
        elif storage_path_type == "custom_fs":
            storage_path = "mock_bucket"
            storage_filesystem = _create_mock_custom_fs(tmp_path / "custom_fs")

        yield storage_path, storage_filesystem


def train_fn(config):
    in_trainer = config.get("in_trainer", False)
    if in_trainer:
        from ray.air._internal.session import _get_session
        from ray.train._internal.session import _TrainSession

        train_session = _get_session()

        assert isinstance(train_session, _TrainSession)
        assert train_session.storage
        assert train_session.storage.checkpoint_fs_path

        # Check that the working dir for each worker is the shared trial dir.
        assert os.getcwd() == train_session.storage.trial_local_path

    start = 0

    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint.pkl"), "rb") as f:
                state = pickle.load(f)
        print("Loaded back state from checkpoint:", state)
        start = state["iter"] + 1

    for i in range(start, config.get("num_iterations", 5)):
        time.sleep(0.25)

        checkpoint_file_name = "checkpoint.pkl"
        artifact_file_name = f"artifact-iter={i}.txt"
        if in_trainer:
            rank = train.get_context().get_world_rank()
            checkpoint_file_name = f"checkpoint_shard-rank={rank}.pkl"
            artifact_file_name = f"artifact-rank={rank}-iter={i}.txt"

        with open(artifact_file_name, "w") as f:
            f.write(f"{i}")

        temp_dir = tempfile.mkdtemp()
        with open(os.path.join(temp_dir, checkpoint_file_name), "wb") as f:
            pickle.dump({"iter": i}, f)

        train.report(
            {"iter": i, _SCORE_KEY: i},
            checkpoint=NewCheckpoint.from_directory(temp_dir),
        )


@pytest.mark.parametrize("storage_path_type", [None, "nfs", "cloud", "custom_fs"])
def test_tuner(monkeypatch, storage_path_type, tmp_path):
    """End-to-end test that the new persistence mode works with the Tuner API.
    This test covers many `storage_path_type` options:
    - storage_path=None --> save locally to the default local path (e.g., ~/ray_results)
    - storage_path="nfs" --> save locally to a fake NFS path
    - storage_path="cloud" --> save to a mock S3 bucket
    - storage_path="custom_fs" --> save to a custom pyarrow filesystem
        - The custom fs is a local filesystem that appends a path prefix to every path.

    This is the expected output at the storage path:

    {storage_path}/{exp_name}
    ├── tuner.pkl                   <- Driver artifacts (global experiment state)
    ├── basic-variant-state.json
    ├── experiment_state.json
    ├── train_fn_a2b9e_00000_0_...
    │   ├── artifact-iter=0.txt     <- Trial artifacts
    │   ├── ...
    │   ├── checkpoint_000000       <- Trial checkpoints
    │   │   └── checkpoint.pkl
    │   ├── ...
    │   ├── events.out.tfevents...  <- Driver artifacts (trial results)
    │   ├── params.json
    │   ├── params.pkl
    │   ├── progress.csv
    │   └── result.json
    └── train_fn_a2b9e_00001_1_...
        └── ...                     <- Same as above
    """

    # Set the cache dir to some temp directory
    LOCAL_CACHE_DIR = tmp_path / "ray_results"
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(LOCAL_CACHE_DIR))

    exp_name = "simple_persistence_test"

    with _resolve_storage_type(storage_path_type, tmp_path) as (
        storage_path,
        storage_filesystem,
    ):
        NUM_ITERATIONS = 6  # == num_checkpoints == num_artifacts
        NUM_TRIALS = 2
        tuner = tune.Tuner(
            train_fn,
            param_space={"num_iterations": NUM_ITERATIONS},
            run_config=train.RunConfig(
                storage_path=storage_path,
                storage_filesystem=storage_filesystem,
                name="simple_persistence_test",
                verbose=0,
                failure_config=train.FailureConfig(max_failures=1),
            ),
            # 2 samples, running 1 at at time to test with actor reuse
            tune_config=tune.TuneConfig(
                num_samples=NUM_TRIALS, max_concurrent_trials=1
            ),
        )
        tuner.fit()

        local_inspect_dir = tmp_path / "inspect"
        if storage_path:
            if storage_filesystem:
                fs, fs_path = storage_filesystem, storage_path
            else:
                fs, fs_path = pyarrow.fs.FileSystem.from_uri(storage_path)
            _download_from_fs_path(
                fs=fs, fs_path=fs_path, local_path=str(local_inspect_dir)
            )
        else:
            local_inspect_dir = LOCAL_CACHE_DIR

    assert len(list(local_inspect_dir.glob("*"))) == 1  # Only expect 1 experiment dir
    exp_dir = local_inspect_dir / exp_name

    # Files synced by the driver
    assert len(list(exp_dir.glob("basic-variant-state-*"))) == 1
    assert len(list(exp_dir.glob("experiment_state-*"))) == 1
    assert len(list(exp_dir.glob("tuner.pkl"))) == 1


@pytest.mark.parametrize("storage_path_type", [None, "nfs", "cloud", "custom_fs"])
@pytest.mark.parametrize(
    "checkpoint_config",
    [
        train.CheckpointConfig(),
        train.CheckpointConfig(num_to_keep=2),
        train.CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute=_SCORE_KEY,
            checkpoint_score_order="max",
        ),
    ],
)
def test_trainer(
    tmp_path, monkeypatch, storage_path_type, checkpoint_config: train.CheckpointConfig
):
    """
    TODO(justinvyu): Test for these once implemented:
    - artifacts
    - restoration, train.get_checkpoint

    {storage_path}/{exp_name}
    ├── experiment_state-2023-07-28_10-00-38.json
    ├── basic-variant-state-2023-07-28_10-00-38.json
    ├── trainer.pkl
    ├── tuner.pkl
    └── DataParallelTrainer_46367_00000_0_...
        ├── events.out.tfevents...
        ├── params.json
        ├── params.pkl
        ├── progress.csv
        ├── result.json
        ├── checkpoint_000000
        │   ├── checkpoint_shard-rank=0.pkl                  <- Worker checkpoint shards
        │   └── checkpoint_shard-rank=1.pkl
        ├── ...
        ├── artifact-rank=0-iter=0.txt            <- Worker artifacts
        ├── artifact-rank=1-iter=0.txt
        ├── ...
        ├── artifact-rank=0-iter=1.txt
        ├── artifact-rank=1-iter=1.txt
        └── ...
    """
    LOCAL_CACHE_DIR = tmp_path / "ray_results"
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(LOCAL_CACHE_DIR))
    exp_name = "trainer_new_persistence"

    with _resolve_storage_type(storage_path_type, tmp_path) as (
        storage_path,
        storage_filesystem,
    ):
        NUM_ITERATIONS = 6
        NUM_WORKERS = 2
        trainer = DataParallelTrainer(
            train_fn,
            train_loop_config={"in_trainer": True, "num_iterations": NUM_ITERATIONS},
            scaling_config=train.ScalingConfig(num_workers=2),
            run_config=train.RunConfig(
                storage_path=storage_path,
                storage_filesystem=storage_filesystem,
                name=exp_name,
                verbose=0,
                checkpoint_config=checkpoint_config,
            ),
        )
        result = trainer.fit()

        local_inspect_dir = tmp_path / "inspect"
        if storage_path:
            if storage_filesystem:
                fs, storage_fs_path = storage_filesystem, storage_path
            else:
                fs, storage_fs_path = pyarrow.fs.FileSystem.from_uri(storage_path)
            _download_from_fs_path(
                fs=fs, fs_path=storage_fs_path, local_path=str(local_inspect_dir)
            )
        else:
            fs, storage_fs_path = pyarrow.fs.LocalFileSystem(), str(LOCAL_CACHE_DIR)
            local_inspect_dir = LOCAL_CACHE_DIR

    # First, inspect that the result object returns the correct paths.
    # TODO(justinvyu): [custom_fs_path_expansion]
    # This doesn't work for the `custom_fs` case right now
    # because Result.path <- Trial.remote_path/local_path <- Experiment.path,
    # which expands the storage path to an absolute path.
    # We shouldn't expand the storage path to an absolute path if a custom fs is passed.
    if not storage_filesystem:
        _, trial_fs_path = pyarrow.fs.FileSystem.from_uri(result.path)
        assert trial_fs_path.startswith(storage_fs_path)
        assert result.checkpoint.path.startswith(trial_fs_path)

    # Second, inspect the contents of the storage path
    assert len(list(local_inspect_dir.glob("*"))) == 1  # Only expect 1 experiment dir
    exp_dir = local_inspect_dir / exp_name

    # Files synced by the driver
    assert len(list(exp_dir.glob("basic-variant-state-*"))) == 1
    assert len(list(exp_dir.glob("experiment_state-*"))) == 1
    assert len(list(exp_dir.glob("tuner.pkl"))) == 1
    assert len(list(exp_dir.glob("trainer.pkl"))) == 1

    # Files synced by the worker
    assert len(list(exp_dir.glob("DataParallelTrainer_*"))) == 1
    for trial_dir in exp_dir.glob("DataParallelTrainer_*"):
        # If set, expect num_to_keep. Otherwise, expect to see all of them.
        expected_num_checkpoints = checkpoint_config.num_to_keep or NUM_ITERATIONS

        assert len(list(trial_dir.glob("checkpoint_*"))) == expected_num_checkpoints
        for checkpoint_dir in trial_dir.glob("checkpoint_*"):
            # 1 checkpoint shard per worker.
            assert (
                len(list(checkpoint_dir.glob("checkpoint_shard-*.pkl"))) == NUM_WORKERS
            )

        # NOTE: These next 2 are technically synced by the driver.
        # TODO(justinvyu): In a follow-up PR, artifacts will be synced by the workers.
        # TODO(justinvyu): [custom_fs_path_expansion] Same issue as above.
        if not storage_filesystem:
            assert (
                len(list(trial_dir.glob("artifact-*"))) == NUM_ITERATIONS * NUM_WORKERS
            )
            assert len(list(trial_dir.glob(EXPR_RESULT_FILE))) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
