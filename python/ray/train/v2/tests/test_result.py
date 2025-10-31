from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pyarrow.fs
import pytest

import ray
from ray import train
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.constants import CHECKPOINT_MANAGER_SNAPSHOT_FILENAME
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.train.v2.api.result import Result


def uri_join(base_uri: str, *paths: str) -> str:
    """
    Join a base URI (local or remote) with one or more subpaths.
    Preserves query parameters and scheme.
    """
    parsed = urlparse(base_uri)
    new_path = "/".join([p.strip("/") for p in [parsed.path, *paths] if p])

    # If it's a local path (no scheme), ensure we preserve the leading /
    if not parsed.scheme and not new_path.startswith("/"):
        new_path = "/" + new_path

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            new_path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def build_dummy_trainer(
    exp_name: str,
    storage_path: str,
    num_iterations: int,
    num_checkpoints: int,
    train_loop_config: dict,
):
    """Build a dummy TorchTrainer for testing purposes."""

    def worker_loop(_config):
        for i in range(num_iterations):
            # Do some random reports in between checkpoints.
            train.report({"metric_a": -100, "metric_b": -100})

            if ray.train.get_context().get_world_rank() == 0:
                with create_dict_checkpoint({"iter": i}) as checkpoint:
                    train.report(
                        metrics={"metric_a": i, "metric_b": -i},
                        checkpoint=checkpoint,
                    )
            else:
                train.report(metrics={"metric_a": i, "metric_b": -i})
        raise RuntimeError()

    trainer = TorchTrainer(
        train_loop_per_worker=worker_loop,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        run_config=RunConfig(
            name=exp_name,
            storage_path=storage_path,
            checkpoint_config=CheckpointConfig(
                num_to_keep=num_checkpoints,
                checkpoint_score_attribute="metric_a",
                checkpoint_score_order="max",
            ),
        ),
    )
    return trainer


def test_result_repr():
    """Test that the Result __repr__ function can return a string."""
    res = Result(
        metrics={"iter": 0, "metric": 1.0},
        checkpoint=Checkpoint("/bucket/path/ckpt0"),
        error=None,
        path="/bucket/path",
    )
    assert isinstance(repr(res), str)
    assert "Checkpoint(filesystem=local, path=/bucket/path/ckpt0)" in repr(res)
    assert "metrics={'iter': 0, 'metric': 1.0}" in repr(res)


def test_get_best_checkpoint():
    res = Result(
        metrics={},
        checkpoint=None,
        error=None,
        path="/bucket/path",
        best_checkpoints=[
            (Checkpoint("/bucket/path/ckpt0"), {"iter": 0, "metric": 1.0}),
            (Checkpoint("/bucket/path/ckpt1"), {"iter": 1, "metric": 2.0}),
            (Checkpoint("/bucket/path/ckpt2"), {"iter": 2, "metric": 3.0}),
            (Checkpoint("/bucket/path/ckpt3"), {"iter": 3, "metric": 4.0}),
        ],
    )
    assert (
        res.get_best_checkpoint(metric="metric", mode="max").path
        == "/bucket/path/ckpt3"
    )
    assert (
        res.get_best_checkpoint(metric="metric", mode="min").path
        == "/bucket/path/ckpt0"
    )


@pytest.mark.parametrize("storage", ["local", "remote"])
@pytest.mark.parametrize("path_type", ["str", "PathLike"])
@pytest.mark.parametrize("pass_storage_filesystem", [True, False])
@pytest.mark.parametrize("trailing_slash", [False, True])
def test_result_restore(
    ray_start_4_cpus,
    monkeypatch,
    tmp_path,
    storage,
    mock_s3_bucket_uri,
    path_type,
    pass_storage_filesystem,
    trailing_slash,
):
    """Test Result.from_path functionality similar to v1 test_result_restore."""

    if path_type == "PathLike" and storage == "remote":
        # Path will collapse URI scheme separators (s3:// becomes s3:/)
        return

    num_iterations = 3
    num_checkpoints = 2

    if storage == "local":
        storage_path = str(tmp_path)
    elif storage == "remote":
        storage_path = str(mock_s3_bucket_uri)

    exp_name = "test_result_restore_v2"

    trainer = build_dummy_trainer(
        exp_name,
        storage_path,
        num_iterations,
        num_checkpoints,
        train_loop_config={"a": 1, "b": 2},
    )
    with pytest.raises(WorkerGroupError):
        trainer.fit()

    if pass_storage_filesystem:
        storage_context = StorageContext(
            storage_path=storage_path,
            experiment_dir_name=exp_name,
        )

        trial_dir = storage_context.experiment_fs_path
        file_system = storage_context.storage_filesystem
    else:
        trial_dir = uri_join(storage_path, exp_name)
        file_system = None

    # Add trailing slash if parameterized to test that case
    if trailing_slash:
        trial_dir = trial_dir + "/"

    # For PathLike test, only use Path() for local paths, not URIs
    if path_type == "PathLike":
        trial_dir_arg = Path(trial_dir)
    else:
        trial_dir_arg = trial_dir

    result = Result.from_path(
        trial_dir_arg,
        storage_filesystem=file_system,
    )

    assert result.checkpoint
    assert len(result.best_checkpoints) == num_checkpoints

    """
    Top-3 checkpoints with metrics:

                        | iter   | metric_a    metric_b
    checkpoint_000004        4            4          -4
    checkpoint_000003        3            3          -3
    checkpoint_000002        2            2          -2
    """
    # Check if the checkpoints bounded with correct metrics
    best_ckpt_a = result.get_best_checkpoint(metric="metric_a", mode="max")
    assert load_dict_checkpoint(best_ckpt_a)["iter"] == num_iterations - 1

    best_ckpt_b = result.get_best_checkpoint(metric="metric_b", mode="max")
    assert load_dict_checkpoint(best_ckpt_b)["iter"] == num_iterations - num_checkpoints

    with pytest.raises(RuntimeError, match="Invalid metric name.*"):
        result.get_best_checkpoint(metric="invalid_metric", mode="max")


@pytest.mark.parametrize("storage", ["local", "remote"])
def test_result_from_path_validation(
    ray_start_4_cpus,
    tmp_path,
    storage,
    mock_s3_bucket_uri,
):
    """Test that Result.from_path raises RuntimeError when folder or snapshot file doesn't exist."""

    if storage == "local":
        storage_path = str(tmp_path)
        nonexistent_folder = str(tmp_path / "nonexistent_experiment")
        existing_folder = str(tmp_path / "existing_experiment")
    elif storage == "remote":
        storage_path = str(mock_s3_bucket_uri)
        nonexistent_folder = uri_join(storage_path, "nonexistent_experiment")
        existing_folder = uri_join(storage_path, "existing_experiment")

    # Test 1: Folder doesn't exist
    with pytest.raises(RuntimeError, match="Experiment folder .* doesn't exist."):
        Result.from_path(nonexistent_folder)

    # Test 2: Folder exists but snapshot file doesn't exist
    if storage == "local":
        Path(existing_folder).mkdir(parents=True, exist_ok=True)
    else:
        # For S3, we need to create a dummy file to ensure the folder exists
        fs, fs_path = pyarrow.fs.FileSystem.from_uri(existing_folder)
        with fs.open_output_stream(f"{fs_path}/.dummy") as f:
            f.write(b"dummy")

    with pytest.raises(
        RuntimeError,
        match=f"Failed to restore the Result object: {CHECKPOINT_MANAGER_SNAPSHOT_FILENAME} doesn't exist in the experiment folder. Make sure that this is an output directory created "
        "by a Ray Train run.",
    ):
        Result.from_path(existing_folder)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
