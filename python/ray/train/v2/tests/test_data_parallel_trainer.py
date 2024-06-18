import pyarrow.fs
import pytest

import ray
from ray.train import BackendConfig
from ray.train.backend import Backend
from ray.train.v2._internal.exceptions import TrainingFailedError
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2._internal.execution.result import Result
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_backend_setup(ray_start_4_cpus, tmp_path):
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


def test_result_output(ray_start_4_cpus, tmp_path):
    trainer = DataParallelTrainer(
        lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert isinstance(result, Result)
    assert result.path == str(tmp_path / "test")
    assert isinstance(result.filesystem, pyarrow.fs.FileSystem)


def test_result_error(ray_start_4_cpus, tmp_path):
    def _error_func_rank_0():
        """An example train_fun that raises an error on rank 0."""
        if get_train_context().distributed_context.world_rank == 0:
            raise ValueError("error")
        else:
            pass

    trainer = DataParallelTrainer(
        _error_func_rank_0,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert isinstance(result.error, TrainingFailedError)
    assert isinstance(result.error.worker_failures[0], ValueError)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
