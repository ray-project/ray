import os
import tempfile
from pathlib import Path

import pyarrow.fs
import pytest

import ray
from ray.data.context import DataContext
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.train import BackendConfig, Checkpoint
from ray.train.backend import Backend
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR, _get_ray_train_session_dir
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.exceptions import TrainingFailedError
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.result import Result


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_backend_setup(tmp_path):
    class ValidationBackend(Backend):
        def on_start(self, worker_group, backend_config):
            tmp_path.joinpath("on_start").touch()

        def on_training_start(self, worker_group, backend_config):
            tmp_path.joinpath("on_training_start").touch()

        def on_shutdown(self, worker_group, backend_config):
            tmp_path.joinpath("on_shutdown").touch()

    class ValidationBackendConfig(BackendConfig):
        @property
        def backend_cls(self):
            return ValidationBackend

    trainer = DataParallelTrainer(
        lambda: None,
        backend_config=ValidationBackendConfig(),
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()

    assert tmp_path.joinpath("on_start").exists()
    assert tmp_path.joinpath("on_training_start").exists()
    assert tmp_path.joinpath("on_shutdown").exists()


def test_result_output(tmp_path):
    trainer = DataParallelTrainer(
        lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert isinstance(result, Result)
    assert result.path == str(tmp_path / "test")
    assert isinstance(result.filesystem, pyarrow.fs.FileSystem)


def test_no_report():
    trainer = DataParallelTrainer(
        lambda: "not used", scaling_config=ScalingConfig(num_workers=2)
    )
    trainer.fit()


def test_train_loop_config():
    """Check that the train loop config is passed to the train function
    if a config parameter is accepted."""

    def train_fn(config):
        with create_dict_checkpoint({}) as checkpoint:
            ray.train.report(metrics=config, checkpoint=checkpoint)

    train_loop_config = {"x": 1}
    trainer = DataParallelTrainer(
        train_fn,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2),
    )
    result = trainer.fit()
    assert result.metrics == train_loop_config


def test_report_checkpoint_rank0(tmp_path):
    """Check that checkpoints can be reported from rank 0 only."""

    def train_fn():
        metrics = {"rank": ray.train.get_context().get_world_rank()}
        if ray.train.get_context().get_world_rank() == 0:
            with create_dict_checkpoint({}) as checkpoint:
                ray.train.report(metrics=metrics, checkpoint=checkpoint)
        else:
            ray.train.report(metrics=metrics, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert result.metrics == {"rank": 0}
    assert result.checkpoint


def test_report_checkpoint_multirank(tmp_path):
    """Check that checkpoints can be reported from multiple ranks."""

    ranks_to_report = [1, 3]

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        metrics = {"rank": rank}
        if rank in ranks_to_report:
            with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                Path(temp_checkpoint_dir).joinpath(str(rank)).touch()
                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
                ray.train.report(metrics=metrics, checkpoint=checkpoint)
        else:
            ray.train.report(metrics=metrics, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=4),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert result.checkpoint
    result.checkpoint.to_directory(tmp_path / "validate")
    for rank in ranks_to_report:
        assert tmp_path.joinpath("validate", str(rank)).exists()


def test_error(tmp_path):
    def _error_func_rank_0():
        """An example train_fun that raises an error on rank 0."""
        if ray.train.get_context().get_world_rank() == 0:
            raise ValueError("error")

    trainer = DataParallelTrainer(
        _error_func_rank_0,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert isinstance(result.error, TrainingFailedError)
    assert isinstance(result.error.worker_failures[0], ValueError)


@pytest.mark.parametrize("env_disabled", [True, False])
def test_setup_working_directory(tmp_path, monkeypatch, env_disabled):
    # Set the environment variable to control the working directory setup
    monkeypatch.setenv(RAY_CHDIR_TO_TRIAL_DIR, str(int(not env_disabled)))

    experiment_dir_name = "test"
    reference_working_dir = (
        Path(_get_ray_train_session_dir(), "test").resolve().as_posix()
    )

    def _check_same_working_directory():
        worker_working_dir = os.getcwd()
        assert worker_working_dir == reference_working_dir

    trainer = DataParallelTrainer(
        _check_same_working_directory,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name=experiment_dir_name, storage_path=str(tmp_path)),
    )
    result = trainer.fit()

    if not env_disabled:
        assert result.error is None
    else:
        assert isinstance(result.error, TrainingFailedError)


def test_datasets(restore_data_context):  # noqa: F811
    # Test passing datasets to the trainer.
    NUM_ROWS = 1000
    NUM_TRAIN_WORKERS = 2

    # Test propagating DataContext to the Train workers.
    data_context = DataContext.get_current()
    data_context.set_config("foo", "bar")

    train_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        data_context = DataContext.get_current()
        assert data_context.get_config("foo") == "bar"

        try:
            ray.train.get_context().get_dataset_shard("val")
            assert False, "Should raise an error if the dataset is not found"
        except KeyError:
            pass

        train_ds = ray.train.get_context().get_dataset_shard("train")
        num_rows = 0
        for batch in train_ds.iter_batches():
            num_rows += len(batch["id"])
        assert num_rows == NUM_ROWS // NUM_TRAIN_WORKERS

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ScalingConfig(num_workers=NUM_TRAIN_WORKERS),
    )
    trainer.fit()
    result = trainer.fit()
    assert not result.error


if __name__ == "__main__":
    pytest.main(["-v", __file__])
