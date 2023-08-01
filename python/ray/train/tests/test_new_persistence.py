from contextlib import contextmanager
import os
from pathlib import Path
import pickle
import pytest
import tempfile
import time

import pyarrow.fs

from ray import air, train, tune
from ray.air.tests.test_checkpoints import mock_s3_bucket_uri
from ray.train._internal.storage import _download_from_fs_path
from ray.train.data_parallel_trainer import DataParallelTrainer


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


def train_fn(config):
    start = 0

    checkpoint = train.get_context().get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "dummy.pkl"), "rb") as f:
                state = pickle.load(f)
        print("Loaded back state from checkpoint:", state)
        start = state["iter"] + 1

    for i in range(start, config.get("num_iterations", 5)):
        time.sleep(0.5)

        with open(f"artifact-{i}.txt", "w") as f:
            f.write(f"{i}")

        temp_dir = tempfile.mkdtemp()
        with open(os.path.join(temp_dir, "dummy.pkl"), "wb") as f:
            pickle.dump({"iter": i}, f)

        train.report({"iter": i}, checkpoint=air.Checkpoint.from_directory(temp_dir))


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
    │   ├── artifact-0.txt          <- Trial artifacts
    │   ├── ...
    │   ├── checkpoint_000000       <- Trial checkpoints
    │   │   └── dummy.pkl
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

    context_manager = (
        mock_s3_bucket_uri if storage_path_type == "cloud" else dummy_context_manager
    )

    exp_name = "simple_persistence_test"

    with context_manager() as cloud_storage_path:
        storage_filesystem = None
        if storage_path_type is None:
            storage_path = None
        elif storage_path_type == "nfs":
            storage_path = str(tmp_path / "fake_nfs")
        elif storage_path_type == "cloud":
            storage_path = str(cloud_storage_path)
        elif storage_path_type == "custom_fs":
            storage_path = "mock_bucket"
            storage_filesystem = _create_mock_custom_fs(tmp_path / "custom_fs")

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


def test_trainer(tmp_path):
    """For now, this is just a dummy test to inspect that the storage context
    has been passed to the train workers properly."""
    storage_path = str(tmp_path / "fake_nfs")

    def dummy_train_fn(config):
        from ray.air._internal.session import _get_session
        from ray.train._internal.session import _TrainSession

        train_session = _get_session()
        print(train_session.storage)

        assert isinstance(train_session, _TrainSession)
        assert train_session.storage
        assert train_session.storage.checkpoint_fs_path

    trainer = DataParallelTrainer(
        dummy_train_fn,
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(
            storage_path=storage_path,
            name="trainer_new_persistence",
        ),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
