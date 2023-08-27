import functools
import time
from unittest.mock import patch
import pytest
from ray.train._internal.worker_group import WorkerGroup
from ray.train.trainer import TrainingIterator

import ray
from ray import train
from ray.train import CheckpointConfig, DataConfig
from ray.air._internal.util import StartTraceback
from ray.train.backend import BackendConfig
from ray.train._internal.session import init_session, get_session
from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.utils import construct_train_func
from ray.train._internal.checkpoint import CheckpointManager
from ray.train.examples.tf.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)

from ray.train.tests.util import mock_storage_context


@pytest.fixture(autouse=True, scope="module")
def patch_tune_session():
    if not get_session():
        init_session(
            training_func=None,
            world_rank=None,
            local_rank=None,
            node_rank=None,
            local_world_size=None,
            world_size=None,
            storage=mock_storage_context(),
        )
    yield


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def gen_execute_single_async_special(special_f):
    def execute_single_async_special(self, i, f, *args, **kwargs):
        assert len(self.workers) == 2
        if i == 0 and hasattr(self, "should_fail") and self.should_fail:
            kwargs["train_func"] = special_f
        return (
            self.workers[i]
            .actor._RayTrainWorker__execute.options(name=f.__name__)
            .remote(f, *args, **kwargs)
        )

    return execute_single_async_special


def gen_new_backend_executor(special_f):
    """Returns a BackendExecutor that runs special_f on worker 0 once."""

    class TestBackendExecutor(BackendExecutor):
        _has_failed = False

        def start_training(self, *args, **kwargs):
            special_execute = gen_execute_single_async_special(special_f)
            if not self._has_failed:
                self.worker_group.should_fail = True
                self._has_failed = True
            else:
                self.worker_group.should_fail = False
            with patch.object(WorkerGroup, "execute_single_async", special_execute):
                super().start_training(*args, **kwargs)

    return TestBackendExecutor


def create_iterator(
    train_func,
    backend_config,
    *,
    num_workers=2,
    backend_executor_cls=BackendExecutor,
    init_hook=None,
):
    # Similar logic to the old Trainer.run_iterator().

    train_func = construct_train_func(train_func, None)

    backend_executor = backend_executor_cls(
        backend_config=backend_config, num_workers=num_workers
    )
    backend_executor.start(init_hook)

    return TrainingIterator(
        backend_executor=backend_executor,
        backend_config=backend_config,
        train_func=train_func,
        run_dir=None,
        datasets={},
        metadata={},
        data_config=DataConfig(),
        checkpoint=None,
        checkpoint_strategy=CheckpointConfig(),
        checkpoint_manager=CheckpointManager(),
    )


def test_run_iterator(ray_start_4_cpus):
    config = BackendConfig()

    def train_func():
        for i in range(3):
            train.report(dict(index=i))
        return 1

    iterator = create_iterator(train_func, config)

    count = 0
    for results in iterator:
        assert all(value.metrics["index"] == count for value in results)
        count += 1

    assert count == 3
    assert iterator.is_finished()

    with pytest.raises(StopIteration):
        next(iterator)


def test_run_iterator_error(ray_start_4_cpus):
    config = BackendConfig()

    def fail_train():
        raise NotImplementedError

    iterator = create_iterator(fail_train, config)

    with pytest.raises(StartTraceback) as exc:
        next(iterator)

    assert isinstance(exc.value.__cause__, NotImplementedError), (
        exc.value,
        exc.value.__cause__,
    )

    assert iterator.is_finished()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(sys.argv[1:] + ["-v", "-x", __file__]))
