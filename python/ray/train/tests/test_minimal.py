import pytest

import ray
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.air.config import ScalingConfig


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
        checkpoint = session.get_checkpoint()
        session.report(metrics=checkpoint.to_dict(), checkpoint=checkpoint)
        return checkpoint.to_dict()[key]

    checkpoint = Checkpoint.from_dict(
        {
            # this would be set during checkpoint saving
            "_current_checkpoint_id": 1,
            key: value,
        }
    )

    trainer = DataParallelTrainer(
        train_func,
        backend_config=config,
        resume_from_checkpoint=checkpoint,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    results = trainer.fit()

    assert results.checkpoint.to_dict()[key] == checkpoint.to_dict()[key]


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


def test_huggingface(ray_start_4_cpus):
    """Tests that HuggingFace integrations are lazily loaded."""

    # Torch is eagerly loaded in ray.train.huggingface.
    runtime_env = {"pip": ["torch"]}

    @ray.remote(runtime_env=runtime_env)
    def test_huggingface_task():
        import ray.train.huggingface  # noqa: F401

        from ray.train.huggingface import (
            AccelerateTrainer,
            HuggingFaceCheckpoint,
            HuggingFacePredictor,
            HuggingFaceTrainer,
            TransformersCheckpoint,
            TransformersPredictor,
            TransformersTrainer,
        )

        # Real values are not needed for these tests.
        DUMMY_TRAIN_LOOP_PER_WORKER = None
        DUMMY_MODEL = None
        DUMMY_PATH = None
        DUMMY_PIPELINE = None
        DUMMY_CHECKPOINT = None
        DUMMY_TRAINER_INIT_PER_WORKER = None

        with pytest.raises(ImportError):
            AccelerateTrainer(DUMMY_TRAIN_LOOP_PER_WORKER)

        with pytest.raises(ImportError):
            HuggingFaceCheckpoint.from_model(DUMMY_MODEL, path=DUMMY_PATH)

        with pytest.raises(ImportError):
            HuggingFacePredictor(DUMMY_PIPELINE)

        with pytest.raises(ImportError):
            HuggingFacePredictor.from_checkpoint(DUMMY_CHECKPOINT)

        with pytest.raises(ImportError):
            HuggingFaceTrainer(DUMMY_TRAINER_INIT_PER_WORKER)

        with pytest.raises(ImportError):
            TransformersCheckpoint.from_model(DUMMY_MODEL, path=DUMMY_PATH)

        with pytest.raises(ImportError):
            TransformersPredictor(DUMMY_PIPELINE)

        with pytest.raises(ImportError):
            TransformersPredictor.from_checkpoint(DUMMY_CHECKPOINT)

        with pytest.raises(ImportError):
            TransformersTrainer(DUMMY_TRAINER_INIT_PER_WORKER)

    ray.get(test_huggingface_task.remote())


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
