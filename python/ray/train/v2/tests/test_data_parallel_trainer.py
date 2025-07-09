import multiprocessing
import os
import signal
import tempfile
from pathlib import Path

import pyarrow.fs
import pytest

import ray
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.train import BackendConfig, Checkpoint, RunConfig, ScalingConfig, UserCallback
from ray.train.backend import Backend
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR, _get_ray_train_session_dir
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import TrainingFailedError
from ray.train.v2.api.result import Result

assert is_v2_enabled()


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


def run_process_for_sigint_abort(abort_terminates):
    # Lives outside test_sigint_abort because cannot pickle nested functions.

    # Needed to reuse current ray cluster.
    ray.init(address="auto")

    if not abort_terminates:

        async def fake_abort():
            while True:
                pass

        from ray.train.v2._internal.execution.controller import TrainController

        TrainController.abort = fake_abort

    def train_fn():
        signal_actor = ray.get_actor("signal_actor", namespace="test_sigint_abort")
        ray.get(signal_actor.send.remote())
        while True:
            pass

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()


@pytest.mark.parametrize(
    "spam_sigint",
    [
        False,
        # Disabling this test because it's flaky.
        # True,
    ],
)
def test_sigint_abort(ray_start_4_cpus, spam_sigint):
    # Use SignalActor to wait for training to start before sending SIGINT.
    SignalActor = create_remote_signal_actor(ray)
    signal_actor = SignalActor.options(
        name="signal_actor", namespace="test_sigint_abort"
    ).remote()

    # Use spawn because of
    # https://docs.ray.io/en/latest/ray-core/patterns/fork-new-processes.html
    multiprocessing.set_start_method("spawn", force=True)
    process = multiprocessing.Process(
        target=run_process_for_sigint_abort, args=(not spam_sigint,)
    )
    process.start()

    # Wait for training to start.
    ray.get(signal_actor.wait.remote())

    # Verify that process exits after sufficient number of SIGINTS.
    os.kill(process.pid, signal.SIGINT)
    if spam_sigint:
        import time

        assert process.exitcode is None
        # This is flaky. Sometimes SIGINTs are ignored and you need to wait.
        while process.exitcode is None:
            time.sleep(1)
            os.kill(process.pid, signal.SIGINT)
    process.join()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
