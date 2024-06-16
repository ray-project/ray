import pytest

import ray
from ray.train import BackendConfig
from ray.train.backend import Backend
from ray.train.v2.api.config import ScalingConfig
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


if __name__ == "__main__":
    pytest.main(["-v", __file__])
