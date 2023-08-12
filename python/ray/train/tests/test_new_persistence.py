from contextlib import contextmanager
import os
from pathlib import Path
import pickle
import pytest
import tempfile
import time
from typing import Optional, Tuple

import pyarrow.fs

import ray
from ray import train, tune
from ray.air._internal.uri_utils import URI
from ray.air.constants import EXPR_RESULT_FILE
from ray.train._internal.storage import (
    _download_from_fs_path,
    _use_storage_context,
    StorageContext,
)
from ray.train._checkpoint import Checkpoint as NewCheckpoint
from ray.train.base_trainer import TrainingFailedError
from ray.train.constants import RAY_AIR_NEW_PERSISTENCE_MODE
from ray.train.data_parallel_trainer import DataParallelTrainer

from ray.air.tests.test_checkpoints import mock_s3_bucket_uri


_SCORE_KEY = "score"


@contextmanager
def dummy_context_manager():
    yield "dummy value"


@pytest.fixture(scope="module")
def enable_new_persistence_mode():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv(RAY_AIR_NEW_PERSISTENCE_MODE, "1")
        yield
        mp.setenv(RAY_AIR_NEW_PERSISTENCE_MODE, "0")


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus(enable_new_persistence_mode):
    # Make sure to set the env var before calling ray.init()
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


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


def _get_local_inspect_dir(
    root_local_path: Path,
    storage_path: str,
    storage_local_path: Path,
    storage_filesystem: Optional[pyarrow.fs.FileSystem],
) -> Tuple[Path, str]:
    """Downloads the storage path -> local dir for inspecting contents.

    Returns:
        Tuple: (local_inspect_dir, storage_fs_path), where storage_fs_path
            is the path to the storage path on the filesystem (e.g., prefix stripped).
            This is used to check the correctness of paths returned from `Result`'s,
            since URIs are hard to do comparisons with.
    """
    local_inspect_dir = root_local_path / "inspect"
    if storage_path:
        if storage_filesystem:
            fs, storage_fs_path = storage_filesystem, storage_path
        else:
            fs, storage_fs_path = pyarrow.fs.FileSystem.from_uri(storage_path)
        _download_from_fs_path(
            fs=fs, fs_path=storage_fs_path, local_path=str(local_inspect_dir)
        )
    else:
        fs, storage_fs_path = pyarrow.fs.LocalFileSystem(), str(storage_local_path)
        local_inspect_dir = storage_local_path

    return local_inspect_dir, storage_fs_path


def _convert_path_to_fs_path(
    path: str, storage_filesystem: Optional[pyarrow.fs.FileSystem]
) -> str:
    """Converts a path to a (prefix-stripped) filesystem path.

    Ex: "s3://bucket/path/to/file" -> "bucket/path/to/file"
    Ex: "/mnt/nfs/path/to/file" -> "/mnt/nfs/bucket/path/to/file"
    """
    if not storage_filesystem:
        _, fs_path = pyarrow.fs.FileSystem.from_uri(path)
        return fs_path

    # Otherwise, we're using a custom filesystem,
    # and the provided path is already the fs path.
    return path


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

        temp_dir = tempfile.mkdtemp()
        with open(os.path.join(temp_dir, "checkpoint.pkl"), "wb") as f:
            pickle.dump({"iter": i}, f)

        artifact_file_name = f"artifact-iter={i}.txt"
        if in_trainer:
            rank = train.get_context().get_world_rank()
            artifact_file_name = f"artifact-rank={rank}-iter={i}.txt"

            checkpoint_file_name = f"checkpoint_shard-rank={rank}.pkl"
            with open(os.path.join(temp_dir, checkpoint_file_name), "wb") as f:
                pickle.dump({"iter": i}, f)

        with open(artifact_file_name, "w") as f:
            f.write(f"{i}")

        train.report(
            {"iter": i, _SCORE_KEY: i},
            checkpoint=NewCheckpoint.from_directory(temp_dir),
        )
        if i in config.get("fail_iters", []):
            raise RuntimeError(f"Failing on iter={i}!!")


def _resume_from_checkpoint(checkpoint: NewCheckpoint, expected_state: dict):
    print(f"\nStarting run with `resume_from_checkpoint`: {checkpoint}\n")

    def assert_fn(config):
        checkpoint_to_check = train.get_checkpoint()
        with checkpoint_to_check.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint.pkl"), "rb") as f:
                state = pickle.load(f)

        print("Loaded state from `resume_from_checkpoint`:", state)
        print("Expected state:", expected_state)
        assert state == expected_state, (state, expected_state)

        dummy_ckpt = tempfile.mkdtemp()
        with open(os.path.join(dummy_ckpt, "dummy.txt"), "w") as f:
            f.write("data")
        train.report({"dummy": 1}, checkpoint=NewCheckpoint.from_directory(dummy_ckpt))

    trainer = DataParallelTrainer(
        assert_fn,
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(name="test_resume_from_checkpoint"),
        resume_from_checkpoint=checkpoint,
    )
    result = trainer.fit()

    # Make sure that the checkpoint indexing starts from scratch.
    assert Path(
        result.checkpoint.path
    ).name == StorageContext._make_checkpoint_dir_name(0)


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
def test_tuner(
    monkeypatch, tmp_path, storage_path_type, checkpoint_config: train.CheckpointConfig
):
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
            param_space={"num_iterations": NUM_ITERATIONS, "fail_iters": [2, 4]},
            run_config=train.RunConfig(
                storage_path=storage_path,
                storage_filesystem=storage_filesystem,
                name=exp_name,
                verbose=0,
                failure_config=train.FailureConfig(max_failures=1),
                checkpoint_config=checkpoint_config,
            ),
            # 2 samples, running 1 at at time to test with actor reuse
            tune_config=tune.TuneConfig(
                num_samples=NUM_TRIALS, max_concurrent_trials=1
            ),
        )
        result_grid = tuner.fit()
        assert result_grid.errors

        restored_tuner = tune.Tuner.restore(
            path=str(URI(storage_path or str(LOCAL_CACHE_DIR)) / exp_name),
            trainable=train_fn,
            storage_filesystem=storage_filesystem,
            resume_errored=True,
        )
        result_grid = restored_tuner.fit()
        assert not result_grid.errors

        local_inspect_dir, storage_fs_path = _get_local_inspect_dir(
            root_local_path=tmp_path,
            storage_path=storage_path,
            storage_local_path=LOCAL_CACHE_DIR,
            storage_filesystem=storage_filesystem,
        )

    # First, check that the ResultGrid returns the correct paths.
    experiment_fs_path = _convert_path_to_fs_path(
        result_grid.experiment_path, storage_filesystem
    )
    assert experiment_fs_path == os.path.join(storage_fs_path, exp_name)
    assert len(result_grid) == NUM_TRIALS
    for result in result_grid:
        trial_fs_path = _convert_path_to_fs_path(result.path, storage_filesystem)
        assert trial_fs_path.startswith(experiment_fs_path)
        for checkpoint, _ in result.best_checkpoints:
            assert checkpoint.path.startswith(trial_fs_path)

    # Next, inspect the storage path contents.
    assert len(list(local_inspect_dir.glob("*"))) == 1  # Only expect 1 experiment dir
    exp_dir = local_inspect_dir / exp_name

    # Files synced by the driver
    assert (exp_dir / "tuner.pkl").exists()
    # 2 copies of these files:
    # 1 for the initial run, and 1 for the manually restored run.
    assert len(list(exp_dir.glob("basic-variant-state-*"))) == 2
    assert len(list(exp_dir.glob("experiment_state-*"))) == 2

    # Files synced by the worker
    assert len(list(exp_dir.glob("train_fn_*"))) == NUM_TRIALS
    for trial_dir in exp_dir.glob("train_fn_*"):
        # If set, expect num_to_keep. Otherwise, expect to see all of them.
        expected_num_checkpoints = checkpoint_config.num_to_keep or NUM_ITERATIONS

        assert len(list(trial_dir.glob("checkpoint_*"))) == expected_num_checkpoints
        for checkpoint_dir in trial_dir.glob("checkpoint_*"):
            # 1 shared checkpoint.pkl file, written by all workers.
            assert len(list(checkpoint_dir.glob("checkpoint.pkl"))) == 1

        # NOTE: These next 2 are technically synced by the driver.
        # TODO(justinvyu): In a follow-up PR, artifacts will be synced by the workers.
        assert len(list(trial_dir.glob("artifact-*"))) == NUM_ITERATIONS
        assert len(list(trial_dir.glob(EXPR_RESULT_FILE))) == 1


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

    {storage_path}/{exp_name}
    ├── experiment_state-2023-07-28_10-00-38.json       <- Initial exp state
    ├── basic-variant-state-2023-07-28_10-00-38.json
    ├── experiment_state-2023-07-28_10-01-38.json       <- Restored exp state
    ├── basic-variant-state-2023-07-28_10-01-38.json
    ├── trainer.pkl
    ├── tuner.pkl
    └── DataParallelTrainer_46367_00000_0_...
        ├── events.out.tfevents...
        ├── params.json
        ├── params.pkl
        ├── progress.csv
        ├── result.json
        ├── checkpoint_000000
        │   ├── checkpoint.pkl                    <- Shared checkpoint file
        │   ├── checkpoint_shard-rank=0.pkl       <- Worker checkpoint shards
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
            train_loop_config={
                "in_trainer": True,
                "num_iterations": NUM_ITERATIONS,
                "fail_iters": [2, 4],
            },
            scaling_config=train.ScalingConfig(num_workers=2),
            run_config=train.RunConfig(
                storage_path=storage_path,
                storage_filesystem=storage_filesystem,
                name=exp_name,
                verbose=0,
                checkpoint_config=checkpoint_config,
                failure_config=train.FailureConfig(max_failures=1),
            ),
        )
        print("\nStarting initial run.\n")
        with pytest.raises(TrainingFailedError):
            result = trainer.fit()

        print("\nStarting manually restored run.\n")
        restored_trainer = DataParallelTrainer.restore(
            path=str(URI(storage_path or str(LOCAL_CACHE_DIR)) / exp_name),
            storage_filesystem=storage_filesystem,
        )
        result = restored_trainer.fit()

        with monkeypatch.context() as m:
            # This is so that the `resume_from_checkpoint` run doesn't mess up the
            # assertions later for the `storage_path=None` case.
            m.setenv(
                "RAY_AIR_LOCAL_CACHE_DIR", str(tmp_path / "resume_from_checkpoint")
            )
            _resume_from_checkpoint(
                result.checkpoint, expected_state={"iter": NUM_ITERATIONS - 1}
            )

        local_inspect_dir, storage_fs_path = _get_local_inspect_dir(
            root_local_path=tmp_path,
            storage_path=storage_path,
            storage_local_path=LOCAL_CACHE_DIR,
            storage_filesystem=storage_filesystem,
        )

    # First, inspect that the result object returns the correct paths.
    trial_fs_path = _convert_path_to_fs_path(result.path, storage_filesystem)
    assert trial_fs_path.startswith(storage_fs_path)
    for checkpoint, _ in result.best_checkpoints:
        assert checkpoint.path.startswith(trial_fs_path)

    # Second, inspect the contents of the storage path
    assert len(list(local_inspect_dir.glob("*"))) == 1  # Only expect 1 experiment dir
    exp_dir = local_inspect_dir / exp_name

    # Files synced by the driver
    assert len(list(exp_dir.glob("tuner.pkl"))) == 1
    assert len(list(exp_dir.glob("trainer.pkl"))) == 1
    # 2 copies of these files:
    # 1 for the initial run, and 1 for the manually restored run.
    assert len(list(exp_dir.glob("basic-variant-state-*"))) == 2
    assert len(list(exp_dir.glob("experiment_state-*"))) == 2

    # Files synced by the worker
    assert len(list(exp_dir.glob("DataParallelTrainer_*"))) == 1
    for trial_dir in exp_dir.glob("DataParallelTrainer_*"):
        # If set, expect num_to_keep. Otherwise, expect to see all of them.
        expected_num_checkpoints = checkpoint_config.num_to_keep or NUM_ITERATIONS

        assert len(list(trial_dir.glob("checkpoint_*"))) == expected_num_checkpoints
        for checkpoint_dir in trial_dir.glob("checkpoint_*"):
            # 1 shared checkpoint.pkl file, written by all workers.
            assert len(list(checkpoint_dir.glob("checkpoint.pkl"))) == 1
            # 1 checkpoint shard per worker.
            assert (
                len(list(checkpoint_dir.glob("checkpoint_shard-*.pkl"))) == NUM_WORKERS
            )

        # NOTE: These next 2 are technically synced by the driver.
        # TODO(justinvyu): In a follow-up PR, artifacts will be synced by the workers.
        assert len(list(trial_dir.glob("artifact-*"))) == NUM_ITERATIONS * NUM_WORKERS
        assert len(list(trial_dir.glob(EXPR_RESULT_FILE))) == 1


def test_disabled_for_class_trainable(ray_start_4_cpus, tmp_path, monkeypatch):
    LOCAL_CACHE_DIR = tmp_path / "ray_results"
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(LOCAL_CACHE_DIR))

    class ClassTrainable(tune.Trainable):
        # def setup(self, config):
        # assert not _use_storage_context(), "Should not be in new persistence mode!"

        def step(self) -> dict:
            return {"score": 1, "done": True, "should_checkpoint": True}

        def save_checkpoint(self, temp_checkpoint_dir) -> str:
            (Path(temp_checkpoint_dir) / "dummy.txt").write_text("dummy")
            return temp_checkpoint_dir

    tuner = tune.Tuner(
        ClassTrainable,
        run_config=train.RunConfig(
            storage_path=str(tmp_path / "fake_nfs"), name="test_cls_trainable"
        ),
    )
    result_grid = tuner.fit()

    assert not result_grid.errors


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
