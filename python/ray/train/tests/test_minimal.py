import pytest

import ray
from ray import train
from ray.train import ScalingConfig
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass


def test_run(ray_start_4_cpus):
    """Tests that Train can be run without any specific backends."""
    num_workers = 2
    key = "value"
    value = 1
    config = TestConfig()

    def train_func():
        checkpoint = train.get_checkpoint()
        checkpoint_dict = load_dict_checkpoint(checkpoint)
        train.report(metrics=checkpoint_dict, checkpoint=checkpoint)
        return checkpoint_dict[key]

    with create_dict_checkpoint({key: value}) as checkpoint:

        trainer = DataParallelTrainer(
            train_func,
            backend_config=config,
            resume_from_checkpoint=checkpoint,
            scaling_config=ScalingConfig(num_workers=num_workers),
        )
        results = trainer.fit()

        assert load_dict_checkpoint(results.checkpoint) == load_dict_checkpoint(
            checkpoint
        )


def test_failure():
    """Tests that backend frameworks and non-critical libraries are not imported."""
    with pytest.raises(ModuleNotFoundError):
        import torch  # noqa: F401

    with pytest.raises(ModuleNotFoundError):
        import tensorflow  # noqa: F401

    with pytest.raises(ModuleNotFoundError):
        import horovod  # noqa: F401

    with pytest.raises(ModuleNotFoundError):
        import accelerate  # noqa: F401

    with pytest.raises(ModuleNotFoundError):
        import transformers  # noqa: F401


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
