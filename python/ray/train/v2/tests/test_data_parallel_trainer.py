import os
import tempfile
from pathlib import Path
import logging
from unittest.mock import patch, MagicMock

import pyarrow.fs
import pytest

import ray
from ray.train import BackendConfig, Checkpoint, RunConfig, ScalingConfig, UserCallback
from ray.train.backend import Backend
from ray.train.constants import (
    RAY_CHDIR_TO_TRIAL_DIR,
    RAY_TRAIN_CALLBACKS_ENV_VAR,
    _get_ray_train_session_dir,
)
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import TrainingFailedError
from ray.train.v2.api.result import Result
from ray.train.v2.api.callback import RayTrainCallback

assert is_v2_enabled()


# Test callback classes for the _load_callbacks_from_env method tests
class ValidTestCallback(RayTrainCallback):
    """A valid test callback that inherits from RayTrainCallback."""

    def __init__(self):
        super().__init__()
        self.called = True


class AnotherValidTestCallback(RayTrainCallback):
    """Another valid test callback that inherits from RayTrainCallback."""

    def __init__(self):
        super().__init__()
        self.name = "another_callback"


class InvalidTestCallback:
    """An invalid test callback that does not inherit from RayTrainCallback."""

    def __init__(self):
        self.invalid = True


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


def test_no_optional_arguments():
    """Check that the DataParallelTrainer can be instantiated without optional arguments."""
    trainer = DataParallelTrainer(lambda: "not used")
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
    with pytest.raises(TrainingFailedError) as exc_info:
        trainer.fit()
        assert isinstance(exc_info.value.worker_failures[0], ValueError)


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
        if env_disabled:
            assert worker_working_dir != reference_working_dir
        else:
            assert worker_working_dir == reference_working_dir

    trainer = DataParallelTrainer(
        _check_same_working_directory,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name=experiment_dir_name, storage_path=str(tmp_path)),
    )
    trainer.fit()


def test_user_callback(tmp_path):
    """Test end to end usage of user callbacks."""
    num_workers = 2

    class MyUserCallback(UserCallback):
        def after_report(self, run_context, metrics, checkpoint):
            assert len(metrics) == num_workers
            assert not checkpoint

        def after_exception(self, run_context, worker_exceptions):
            assert len(worker_exceptions) == 1
            assert worker_exceptions.get(0) is not None

    def _train_fn(config):
        ray.train.report(metrics={"rank": ray.train.get_context().get_world_rank()})
        if ray.train.get_context().get_world_rank() == 0:
            raise ValueError("error")

    trainer = DataParallelTrainer(
        _train_fn,
        scaling_config=ScalingConfig(num_workers=num_workers),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[MyUserCallback()],
        ),
    )
    # The error should NOT be an assertion error from the user callback.
    with pytest.raises(TrainingFailedError):
        trainer.fit()


class TestLoadCallbacksFromEnv:
    """Test suite for the _load_callbacks_from_env method."""

    @pytest.fixture
    def mock_trainer(self):
        """Create a DataParallelTrainer instance for testing."""
        return DataParallelTrainer(
            lambda: None,
            scaling_config=ScalingConfig(num_workers=1),
        )

    def test_empty_environment_variable(self, mock_trainer, monkeypatch):
        """Test behavior when RAY_TRAIN_CALLBACKS environment variable is empty."""
        monkeypatch.delenv(RAY_TRAIN_CALLBACKS_ENV_VAR, raising=False)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []

    def test_whitespace_only_environment_variable(self, mock_trainer, monkeypatch):
        """Test behavior when RAY_TRAIN_CALLBACKS contains only whitespace."""
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, "   ")

        callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []

    def test_valid_single_callback(self, mock_trainer, monkeypatch):
        """Test loading a single valid callback."""
        callback_path = "test_data_parallel_trainer.ValidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        assert isinstance(callbacks[0], ValidTestCallback)
        assert callbacks[0].called is True

    def test_valid_multiple_callbacks(self, mock_trainer, monkeypatch):
        """Test loading multiple valid callbacks."""
        callback_paths = (
            "test_data_parallel_trainer.ValidTestCallback,"
            "test_data_parallel_trainer.AnotherValidTestCallback"
        )
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 2
        assert isinstance(callbacks[0], ValidTestCallback)
        assert isinstance(callbacks[1], AnotherValidTestCallback)
        assert callbacks[0].called is True
        assert callbacks[1].name == "another_callback"

    def test_callbacks_with_extra_whitespace(self, mock_trainer, monkeypatch):
        """Test loading callbacks with extra whitespace in the environment variable."""
        callback_paths = (
            "  test_data_parallel_trainer.ValidTestCallback  ,  "
            "  test_data_parallel_trainer.AnotherValidTestCallback  "
        )
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 2
        assert isinstance(callbacks[0], ValidTestCallback)
        assert isinstance(callbacks[1], AnotherValidTestCallback)

    def test_empty_callback_path_in_list(self, mock_trainer, monkeypatch):
        """Test behavior when one of the comma-separated values is empty."""
        callback_paths = (
            "test_data_parallel_trainer.ValidTestCallback,,"
            "test_data_parallel_trainer.AnotherValidTestCallback"
        )
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 2
        assert isinstance(callbacks[0], ValidTestCallback)
        assert isinstance(callbacks[1], AnotherValidTestCallback)

    def test_nonexistent_module(
        self, mock_trainer, monkeypatch, caplog, propagate_logs
    ):
        """Test behavior when trying to import a non-existent module."""
        callback_path = "nonexistent.module.SomeCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        with caplog.at_level(
            logging.WARNING, logger="ray.train.v2.api.data_parallel_trainer"
        ):
            callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []
        assert (
            "Failed to load callback from 'nonexistent.module.SomeCallback'"
            in caplog.text
        )

    def test_nonexistent_class_name(
        self, mock_trainer, monkeypatch, caplog, propagate_logs
    ):
        """Test behavior when trying to access a non-existent class in an existing module."""
        callback_path = "test_data_parallel_trainer.NonExistentCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        with caplog.at_level(
            logging.WARNING, logger="ray.train.v2.api.data_parallel_trainer"
        ):
            callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []
        assert "Failed to load callback from" in caplog.text
        assert "NonExistentCallback" in caplog.text

    def test_invalid_callback_type(
        self, mock_trainer, monkeypatch, caplog, propagate_logs
    ):
        """Test behavior when callback class does not inherit from RayTrainCallback."""
        callback_path = "test_data_parallel_trainer.InvalidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        with caplog.at_level(
            logging.WARNING, logger="ray.train.v2.api.data_parallel_trainer"
        ):
            callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []
        assert "is not a RayTrainCallback instance. Skipping." in caplog.text

    def test_mixed_valid_and_invalid_callbacks(
        self, mock_trainer, monkeypatch, caplog, propagate_logs
    ):
        """Test behavior with a mix of valid and invalid callbacks."""
        callback_paths = (
            "test_data_parallel_trainer.ValidTestCallback,"
            "test_data_parallel_trainer.InvalidTestCallback,"
            "nonexistent.module.SomeCallback,"
            "test_data_parallel_trainer.AnotherValidTestCallback"
        )
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        with caplog.at_level(
            logging.WARNING, logger="ray.train.v2.api.data_parallel_trainer"
        ):
            callbacks = mock_trainer._load_callbacks_from_env()

        # Only valid callbacks should be loaded
        assert len(callbacks) == 2
        assert isinstance(callbacks[0], ValidTestCallback)
        assert isinstance(callbacks[1], AnotherValidTestCallback)

        # Warnings should be logged for invalid callbacks
        assert "is not a RayTrainCallback instance. Skipping." in caplog.text
        assert (
            "Failed to load callback from 'nonexistent.module.SomeCallback'"
            in caplog.text
        )

    def test_callback_instantiation_error(
        self, mock_trainer, monkeypatch, caplog, propagate_logs
    ):
        """Test behavior when callback instantiation fails."""
        # Create a mock that raises an exception during instantiation
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()

            # Create a class that raises an exception when instantiated
            class FailingCallback(RayTrainCallback):
                def __init__(self):
                    raise ValueError("Instantiation failed")

            mock_module.FailingCallback = FailingCallback
            mock_import.return_value = mock_module

            callback_path = "some.module.FailingCallback"
            monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

            with caplog.at_level(
                logging.WARNING, logger="ray.train.v2.api.data_parallel_trainer"
            ):
                callbacks = mock_trainer._load_callbacks_from_env()

            assert callbacks == []
            assert (
                "Failed to load callback from 'some.module.FailingCallback'"
                in caplog.text
            )

    @patch("ray.train.v2.api.data_parallel_trainer.logger")
    def test_successful_callback_debug_logging(
        self, mock_logger, mock_trainer, monkeypatch
    ):
        """Test that debug messages are logged for successfully loaded callbacks."""
        callback_path = "test_data_parallel_trainer.ValidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        mock_logger.debug.assert_called_with(
            f"Successfully loaded callback: {callback_path}"
        )

    def test_callback_class_vs_instance_handling(self, mock_trainer, monkeypatch):
        """Test that the method correctly handles both class objects and instances."""
        # This test verifies the isinstance(callback_class, type) check
        callback_path = "test_data_parallel_trainer.ValidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        assert isinstance(callbacks[0], ValidTestCallback)
        # Verify it was instantiated (has the property set in __init__)
        assert callbacks[0].called is True

    def test_rsplit_behavior_with_multiple_dots(self, mock_trainer, monkeypatch):
        """Test that rsplit with maxsplit=1 correctly handles modules with multiple dots."""
        # Create a nested module structure for testing
        callback_path = "test_data_parallel_trainer.ValidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_path)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        assert isinstance(callbacks[0], ValidTestCallback)

    def test_environment_variable_case_sensitivity(self, mock_trainer, monkeypatch):
        """Test that the environment variable name is case sensitive."""
        # Set a different case version of the env var
        monkeypatch.setenv("ray_train_callbacks", "some.callback.Path")
        # Ensure the correct env var is not set
        monkeypatch.delenv(RAY_TRAIN_CALLBACKS_ENV_VAR, raising=False)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert callbacks == []

    def test_callback_path_trailing_comma(self, mock_trainer, monkeypatch):
        """Test behavior when callback path list ends with a comma."""
        callback_paths = "test_data_parallel_trainer.ValidTestCallback,"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        assert isinstance(callbacks[0], ValidTestCallback)

    def test_callback_path_leading_comma(self, mock_trainer, monkeypatch):
        """Test behavior when callback path list starts with a comma."""
        callback_paths = ",test_data_parallel_trainer.ValidTestCallback"
        monkeypatch.setenv(RAY_TRAIN_CALLBACKS_ENV_VAR, callback_paths)

        callbacks = mock_trainer._load_callbacks_from_env()

        assert len(callbacks) == 1
        assert isinstance(callbacks[0], ValidTestCallback)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
