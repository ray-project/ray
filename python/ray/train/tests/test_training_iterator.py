import pytest
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.train.trainer import TrainingIterator

import ray
from ray.air import session
from ray.air._internal.util import StartTraceback
from ray.train.backend import BackendConfig

from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.utils import ActorWrapper, construct_train_func
from ray.train._internal.checkpoint import CheckpointManager


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def create_iterator(train_func, backend_config):
    # Similar logic to the old Trainer.run_iterator().

    train_func = construct_train_func(train_func, None)

    dataset_spec = RayDatasetSpec(dataset_or_dict=None)

    remote_executor = ray.remote(num_cpus=0)(BackendExecutor)

    backend_executor_actor = remote_executor.remote(
        backend_config=backend_config,
        num_workers=2,
    )

    backend_executor = ActorWrapper(backend_executor_actor)
    backend_executor.start(None)

    return TrainingIterator(
        backend_executor=backend_executor,
        backend_config=backend_config,
        train_func=train_func,
        run_dir=None,
        dataset_spec=dataset_spec,
        checkpoint_manager=CheckpointManager(),
        checkpoint=None,
        checkpoint_strategy=None,
    )


def test_run_iterator(ray_start_4_cpus):
    config = BackendConfig()

    def train_func():
        for i in range(3):
            session.report(dict(index=i))
        return 1

    iterator = create_iterator(train_func, config)

    count = 0
    for results in iterator:
        assert all(value["index"] == count for value in results)
        count += 1

    assert count == 3
    assert iterator.is_finished()
    assert iterator.get_final_results() == [1, 1]

    with pytest.raises(StopIteration):
        next(iterator)


def test_run_iterator_returns(ray_start_4_cpus):
    config = BackendConfig()

    def train_func():
        for i in range(3):
            session.report(dict(index=i))
        return 1

    iterator = create_iterator(train_func, config)

    assert iterator.get_final_results() is None
    assert iterator.get_final_results(force=True) == [1, 1]

    with pytest.raises(StopIteration):
        next(iterator)


def test_run_iterator_error(ray_start_4_cpus):
    config = BackendConfig()

    def fail_train():
        raise NotImplementedError

    iterator = create_iterator(fail_train, config)

    with pytest.raises(StartTraceback) as exc:
        next(iterator)
    assert "NotImplementedError" in str(exc.value)

    assert iterator.get_final_results() is None
    assert iterator.is_finished()


def test_no_exhaust(ray_start_4_cpus, tmp_path):
    """Tests if training can finish even if queue is not exhausted."""

    def train_func():
        for _ in range(2):
            session.report(dict(loss=1))
        return 2

    config = BackendConfig()

    iterator = create_iterator(train_func, config)
    output = iterator.get_final_results(force=True)

    assert output == [2, 2]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(sys.argv[1:] + ["-v", "-x", __file__]))
