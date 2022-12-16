import functools
import time
from unittest.mock import patch
import pytest
from ray.air.checkpoint import Checkpoint
from ray.air.config import CheckpointConfig
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.train._internal.worker_group import WorkerGroup
from ray.train.trainer import TrainingIterator

import ray
from ray.air import session
from ray.air._internal.util import StartTraceback
from ray.train.backend import BackendConfig

from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.utils import ActorWrapper, construct_train_func
from ray.train._internal.checkpoint import CheckpointManager
from ray.train.examples.tf.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)


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
        return self.workers[i].actor._RayTrainWorker__execute.remote(f, *args, **kwargs)

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
    backend_executor=BackendExecutor,
    init_hook=None,
):
    # Similar logic to the old Trainer.run_iterator().

    train_func = construct_train_func(train_func, None)

    dataset_spec = RayDatasetSpec(dataset_or_dict=None)

    remote_executor = ray.remote(num_cpus=0)(backend_executor)

    backend_executor_actor = remote_executor.remote(
        backend_config=backend_config,
        num_workers=num_workers,
    )

    backend_executor = ActorWrapper(backend_executor_actor)
    backend_executor.start(init_hook)

    class _CheckpointConfig(CheckpointConfig):
        def __post_init__(self):
            pass

    checkpoint_strategy = _CheckpointConfig(num_to_keep=0)

    return TrainingIterator(
        backend_executor=backend_executor,
        backend_config=backend_config,
        train_func=train_func,
        run_dir=None,
        dataset_spec=dataset_spec,
        checkpoint_manager=CheckpointManager(checkpoint_strategy=checkpoint_strategy),
        checkpoint=None,
        checkpoint_strategy=checkpoint_strategy,
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


def test_worker_failure_1(ray_start_4_cpus):
    def train_func():
        return 1

    def train_actor_failure():
        import sys

        sys.exit(1)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    config = BackendConfig()

    iterator = create_iterator(
        train_func, config, backend_executor=new_backend_executor_cls
    )
    output = iterator.get_final_results(force=True)

    assert output == [1, 1]


def test_worker_failure_2(ray_start_4_cpus):
    def train_func():
        for _ in range(2):
            session.report(dict(loss=1))
        return 1

    def train_actor_failure():
        for _ in range(2):
            session.report(dict(loss=1))
        import sys

        sys.exit(1)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    config = BackendConfig()

    iterator = create_iterator(
        train_func, config, backend_executor=new_backend_executor_cls
    )
    output = iterator.get_final_results(force=True)

    assert output == [1, 1]


def test_worker_failure_local_rank(ray_start_4_cpus):
    def train_func():
        return session.get_local_rank()

    def train_actor_failure():
        import sys

        sys.exit(1)
        return session.get_local_rank()

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    config = BackendConfig()

    iterator = create_iterator(
        train_func, config, backend_executor=new_backend_executor_cls
    )
    output = iterator.get_final_results(force=True)

    assert output == [0, 1]


def test_worker_start_failure(ray_start_4_cpus):
    def init_hook():
        pass

    def init_hook_fail():
        ray.actor.exit_actor()

    class TestBackendExecutor(BackendExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def _restart(self):
            self._initialization_hook = init_hook
            super()._restart()

    config = BackendConfig()

    iterator = create_iterator(
        lambda x: 1,
        config,
        backend_executor=TestBackendExecutor,
        init_hook=init_hook_fail,
    )
    iterator.get_final_results(force=True)

    assert len(iterator._backend_executor.get_worker_group()) == 2


def test_max_failures(ray_start_4_cpus):
    def train_func():
        import sys

        sys.exit(1)

    config = BackendConfig()

    iterator = create_iterator(train_func, config)
    with pytest.raises(RuntimeError):
        iterator.get_final_results(force=True)
    assert iterator._backend_executor._get_num_failures() == 3


def test_start_max_failures(ray_start_4_cpus):
    def init_hook_fail():
        import sys

        sys.exit(1)

    config = BackendConfig()

    with pytest.raises(RuntimeError):
        create_iterator(lambda x: 1, config, init_hook=init_hook_fail)


class KillCallback:
    def __init__(self, fail_on, backend_executor):
        self.counter = 0
        self.fail_on = fail_on
        self.worker_group = backend_executor.get_worker_group()
        self.results = []

    def handle_result(self, intermiedate_results=None):
        if intermiedate_results:
            self.results.append(intermiedate_results)
        if self.counter == self.fail_on:
            print("killing")
            self.results = []
            ray.kill(self.worker_group.workers[0].actor)
            time.sleep(3)
        self.counter += 1


@pytest.mark.parametrize("backend", ["test", "torch", "tf", "horovod"])
def test_worker_kill(ray_start_4_cpus, backend):
    if backend == "test":
        test_config = BackendConfig()
    elif backend == "torch":
        from ray.train.torch import TorchConfig

        test_config = TorchConfig()
    elif backend == "tf":
        from ray.train.tensorflow import TensorflowConfig

        test_config = TensorflowConfig()
    elif backend == "horovod":
        from ray.train.horovod import HorovodConfig

        test_config = HorovodConfig()

    def train_func():
        for i in range(2):
            session.report(dict(loss=1, iter=i))

    iterator = create_iterator(train_func, test_config)
    kill_callback = KillCallback(fail_on=0, backend_executor=iterator._backend_executor)

    for intermediate_result in iterator:
        # Run 1: iter=0, counter=1, Successful
        # Run 2: iter=1, counter=1, Unsuccessful, starts training from beginning
        # Run 3: iter=0, counter=2, Successful
        # Run 4: iter=1, counter=3, Successful
        kill_callback.handle_result()
    iterator.get_final_results()
    assert kill_callback.counter == 3

    iterator = create_iterator(train_func, test_config)
    kill_callback = KillCallback(fail_on=1, backend_executor=iterator._backend_executor)
    for intermediate_result in iterator:
        # Run 1: iter=0, counter=1, Successful
        # Run 2: iter=1, counter=2, Successful
        # Run 3: None, counter=2, Unsuccessful, starts training from beginning.
        # Run 4: iter=0, counter=3, Successful
        # Run 5: iter=1, counter=4, Successful
        kill_callback.handle_result()
    iterator.get_final_results()
    assert kill_callback.counter == 4


def test_worker_kill_checkpoint(ray_start_4_cpus):
    def train_func():
        checkpoint = session.get_checkpoint()
        if checkpoint:
            epoch = checkpoint.to_dict()["epoch"]
        else:
            epoch = 0
        print("Epoch: ", epoch)
        for i in range(epoch, 2):
            session.report(
                dict(loss=1, iter=i), checkpoint=Checkpoint.from_dict(dict(epoch=i + 1))
            )

    test_config = BackendConfig()

    iterator = create_iterator(train_func, test_config)
    kill_callback = KillCallback(fail_on=0, backend_executor=iterator._backend_executor)

    for intermediate_result in iterator:
        # Run 1: epoch=0, counter=1, Successful
        # *Checkpoint is saved.*
        # *Worker is killed*
        # Run 2: epoch=1, counter=2, Successful
        kill_callback.handle_result()
    iterator.get_final_results()
    assert kill_callback.counter == 2
    assert iterator._checkpoint_manager.latest_checkpoint.to_dict()["epoch"] == 2


def test_tensorflow_mnist_fail(ray_start_4_cpus):
    """Tests if tensorflow example works even with worker failure."""
    epochs = 3
    num_workers = 2

    from ray.train.tensorflow import TensorflowConfig

    test_config = TensorflowConfig()

    train_func = functools.partial(
        tensorflow_mnist_train_func, {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    )
    iterator = create_iterator(train_func, test_config, num_workers=num_workers)
    kill_callback = KillCallback(fail_on=0, backend_executor=iterator._backend_executor)

    for intermediate_result in iterator:
        assert len(intermediate_result) == num_workers
        kill_callback.handle_result(intermediate_result)

    results = kill_callback.results
    assert len(results) == epochs
    assert results[-1][0]["loss"] < results[0][0]["loss"]
    assert results[-1][0]["accuracy"] > results[0][0]["accuracy"]


def test_torch_linear_failure(ray_start_4_cpus):
    num_workers = 2
    epochs = 3

    from ray.train.torch import TorchConfig

    test_config = TorchConfig()

    train_func = functools.partial(
        linear_train_func, {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    )

    iterator = create_iterator(train_func, test_config, num_workers=num_workers)
    kill_callback = KillCallback(fail_on=1, backend_executor=iterator._backend_executor)

    for intermediate_result in iterator:
        assert len(intermediate_result) == num_workers
        kill_callback.handle_result(intermediate_result)

    results = kill_callback.results
    assert len(results) == epochs
    for i in range(num_workers):
        assert results[-1][i]["loss"] < results[0][i]["loss"]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(sys.argv[1:] + ["-v", "-x", __file__]))
