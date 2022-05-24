import os
import time
from pathlib import Path
from unittest.mock import patch

import horovod.torch as hvd_torch
import pytest
import torch

import ray
import ray.train as train
from ray._private.test_utils import wait_for_condition
from ray.train import Trainer, CheckpointStrategy
from ray.train.backend import BackendConfig, Backend, BackendExecutor
from ray.train.constants import TRAIN_ENABLE_WORKER_SPREAD_ENV
from ray.train.torch import TorchConfig
from ray.train.tensorflow import TensorflowConfig
from ray.train.horovod import HorovodConfig
from ray.train.callbacks.callback import TrainingCallback
from ray.train.worker_group import WorkerGroup


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_2_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus_4_extra():
    address_info = ray.init(num_cpus=4, num_gpus=4, resources={"extra": 4})
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


class TestCallback(TrainingCallback):
    def __init__(self):
        self.result_list = []

    def handle_result(self, results):
        self.result_list.append(results)


def gen_execute_single_async_special(special_f):
    def execute_single_async_special(self, i, f, *args, **kwargs):
        assert len(self.workers) == 2
        if i == 0 and hasattr(self, "should_fail") and self.should_fail:
            kwargs["train_func"] = special_f
        return self.workers[i].actor._BaseWorkerMixin__execute.remote(
            f, *args, **kwargs
        )

    return execute_single_async_special


def gen_new_backend_executor(special_f):
    """Returns a BackendExecutor that runs special_f on worker 0 once."""

    class TestBackendExecutor(BackendExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._has_failed = False

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


class KillCallback(TrainingCallback):
    def __init__(self, fail_on, trainer):
        self.counter = 0
        self.fail_on = fail_on
        self.worker_group = trainer._backend_executor.get_worker_group()

    def handle_result(self, results):
        print(results)
        if self.counter == self.fail_on:
            ray.kill(self.worker_group.workers[0].actor)
            time.sleep(3)
        self.counter += 1


@pytest.mark.parametrize("num_workers", [1, 2])
def test_start_shutdown(ray_start_2_cpus, num_workers):
    config = TestConfig()
    assert ray.available_resources()["CPU"] == 2
    trainer = Trainer(config, num_workers=num_workers)
    trainer.start()
    time.sleep(1)

    remaining = 2 - num_workers
    if remaining == 0:
        assert "CPU" not in ray.available_resources()
    else:
        assert ray.available_resources()["CPU"] == remaining

    trainer.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2


def test_env_var(ray_start_2_cpus):
    """Tests if Train env vars are propagated to the BackendExecutor."""
    config = TestConfig()

    os.environ[TRAIN_ENABLE_WORKER_SPREAD_ENV] = "1"

    class EnvBackendExecutor(BackendExecutor):
        def __init__(self, *args, **kwargs):
            assert (
                TRAIN_ENABLE_WORKER_SPREAD_ENV in os.environ
                and os.environ[TRAIN_ENABLE_WORKER_SPREAD_ENV] == "1"
            )
            super().__init__(*args, **kwargs)

    with patch.object(ray.train.trainer, "BackendExecutor", EnvBackendExecutor):
        trainer = Trainer(config, num_workers=1)
        trainer.start()
        trainer.run(lambda: 1)
        trainer.shutdown()

    del os.environ[TRAIN_ENABLE_WORKER_SPREAD_ENV]


def test_run(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(train_func)
    trainer.shutdown()

    assert len(results) == 2
    assert all(result == 1 for result in results)


def test_run_config(ray_start_2_cpus):
    backend_config = TestConfig()

    def train_func(config):
        return config["fruit"]

    config = {"fruit": "banana"}

    trainer = Trainer(backend_config, num_workers=2)
    trainer.start()
    results = trainer.run(train_func, config)
    trainer.shutdown()

    assert len(results) == 2
    assert all(result == "banana" for result in results)


def test_report(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        for i in range(3):
            train.report(index=i)
        return 1

    callback = TestCallback()
    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(train_func, callbacks=[callback])
    assert results == [1, 1]

    result_list = callback.result_list
    assert len(result_list) == 3
    for index in range(len(result_list)):
        intermediate_results = result_list[index]
        assert len(intermediate_results) == 2
        for worker_result in intermediate_results:
            assert worker_result["index"] == index


def test_fast_slow(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        for i in range(2):
            train.save_checkpoint(epoch=i)
            train.report(index=i)

    def train_slow():
        for i in range(2):
            train.save_checkpoint(epoch=i)
            time.sleep(5)
            train.report(index=i)
            time.sleep(5)

    new_backend_executor_cls = gen_new_backend_executor(train_slow)
    callback = TestCallback()

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        trainer.run(train_func, callbacks=[callback])

    assert trainer.latest_checkpoint["epoch"] == 1

    result_list = callback.result_list
    assert len(result_list) == 2
    for index in range(len(result_list)):
        intermediate_results = result_list[index]
        assert len(intermediate_results) == 2
        for worker_result in intermediate_results:
            assert worker_result["index"] == index


def test_mismatch_report(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        for _ in range(2):
            train.report(loss=1)

    def train_mismatch():
        train.report(loss=1)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train_func)


def test_run_iterator(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        for i in range(3):
            train.report(index=i)
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(train_func)

    count = 0
    for results in iterator:
        assert all(value["index"] == count for value in results)
        count += 1

    assert count == 3
    assert iterator.is_finished()
    assert iterator.get_final_results() == [1, 1]

    with pytest.raises(StopIteration):
        next(iterator)


def test_run_iterator_returns(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        for i in range(3):
            train.report(index=i)
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(train_func)

    assert iterator.get_final_results() is None
    assert iterator.get_final_results(force=True) == [1, 1]

    with pytest.raises(StopIteration):
        next(iterator)


def test_run_iterator_error(ray_start_2_cpus):
    config = TestConfig()

    def fail_train():
        raise NotImplementedError

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(fail_train)

    with pytest.raises(NotImplementedError):
        next(iterator)

    assert iterator.get_final_results() is None
    assert iterator.is_finished()


def test_no_exhaust(ray_start_2_cpus, tmp_path):
    """Tests if training can finish even if queue is not exhausted."""

    def train_func():
        for _ in range(2):
            train.report(loss=1)
        return 2

    config = TestConfig()
    trainer = Trainer(config, num_workers=2)
    trainer.start()

    iterator = trainer.run_iterator(train_func)
    output = iterator.get_final_results(force=True)

    assert output == [2, 2]


def test_checkpoint(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        assert train.load_checkpoint() is None
        for i in range(3):
            time.sleep(1)
            train.save_checkpoint(epoch=i)
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    trainer.run(train_func)
    assert trainer.latest_checkpoint == trainer.best_checkpoint
    checkpoint = trainer.latest_checkpoint

    assert checkpoint is not None
    assert checkpoint["epoch"] == 2

    def train_func_checkpoint():
        checkpoint = train.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint["epoch"] == 2

        for i in range(checkpoint["epoch"], 5):
            time.sleep(1)
            train.save_checkpoint(epoch=i)
        return 1

    trainer.run(train_func_checkpoint, checkpoint=checkpoint)
    assert trainer.latest_checkpoint == trainer.best_checkpoint
    checkpoint = trainer.latest_checkpoint

    assert checkpoint is not None
    assert checkpoint["epoch"] == 4


def test_mismatch_checkpoint(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        for i in range(2):
            train.save_checkpoint(epoch=i)

    def train_mismatch():
        train.save_checkpoint(epoch=0)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train_func)


def test_mismatch_checkpoint_report(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        for i in range(2):
            train.save_checkpoint(epoch=i)
            train.report(index=i)

    def train_mismatch():
        train.save_checkpoint(epoch=0)
        train.report(index=0)
        # skip checkpoint
        train.report(index=1)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)
    callback = TestCallback()

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train_func, callbacks=[callback])
    # validate checkpoint
    assert trainer.latest_checkpoint["epoch"] == 0
    # validate callback
    result_list = callback.result_list
    assert len(result_list) == 1  # 1 epoch succeeded
    intermediate_results = result_list[0]
    assert len(intermediate_results) == 2  # both workers reported
    for worker_result in intermediate_results:
        assert worker_result["index"] == 0


def test_load_checkpoint(ray_start_2_cpus):
    config = TestConfig()

    def train_func_checkpoint():
        checkpoint = train.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint["epoch"] == 3

        result = []
        for i in range(checkpoint["epoch"], 5):
            result.append(i)
        return result

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    result = trainer.run(train_func_checkpoint, checkpoint={"epoch": 3})

    assert result is not None
    assert len(result) == 2
    assert result[0] == [3, 4]
    assert result[1] == [3, 4]


@pytest.mark.parametrize(
    "logdir",
    [
        None,
        "/tmp/test/trainer/test_persisted_checkpoint",
        "~/tmp/test/trainer/test_persisted_checkpoint",
    ],
)
def test_persisted_checkpoint(ray_start_2_cpus, logdir):
    config = TestConfig()

    def train_func():
        for i in range(2):
            train.save_checkpoint(epoch=i)
            time.sleep(1)

    trainer = Trainer(config, num_workers=2, logdir=logdir)
    trainer.start()
    trainer.run(train_func)

    assert trainer.best_checkpoint_path is not None
    if logdir is not None:
        assert trainer.logdir == Path(logdir).expanduser().resolve()
    assert trainer.latest_checkpoint_dir.is_dir()
    assert trainer.best_checkpoint_path.is_file()
    assert trainer.best_checkpoint_path.name == f"checkpoint_{2:06d}"
    assert trainer.best_checkpoint_path.parent.name == "checkpoints"
    assert trainer.best_checkpoint == trainer.latest_checkpoint
    latest_checkpoint = trainer.latest_checkpoint

    def validate():
        checkpoint = train.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint == latest_checkpoint

    trainer.run(validate, checkpoint=trainer.best_checkpoint_path)


def test_persisted_checkpoint_strategy(ray_start_2_cpus):
    logdir = "/tmp/test/trainer/test_persisted_checkpoint_strategy"
    config = TestConfig()

    checkpoint_strategy = CheckpointStrategy(
        num_to_keep=2, checkpoint_score_attribute="loss", checkpoint_score_order="min"
    )

    def train_func():
        train.save_checkpoint(loss=float("nan"))  # nan, deleted
        train.save_checkpoint(loss=3)  # best
        train.save_checkpoint(loss=7)  # worst, deleted
        train.save_checkpoint(loss=5)

    trainer = Trainer(config, num_workers=2, logdir=logdir)
    trainer.start()
    trainer.run(train_func, checkpoint_strategy=checkpoint_strategy)

    assert trainer.best_checkpoint_path is not None
    if logdir is not None:
        assert trainer.logdir == Path(logdir).expanduser().resolve()
    assert trainer.latest_checkpoint_dir.is_dir()
    assert trainer.best_checkpoint_path.is_file()
    assert trainer.best_checkpoint_path.name == f"checkpoint_{2:06d}"
    assert trainer.latest_checkpoint["loss"] == 5
    assert trainer.best_checkpoint["loss"] == 3

    checkpoint_dir = trainer.latest_checkpoint_dir
    file_names = [f.name for f in checkpoint_dir.iterdir()]
    assert len(file_names) == 2
    assert f"checkpoint_{2:06d}" in file_names
    assert f"checkpoint_{3:06d}" not in file_names
    assert f"checkpoint_{4:06d}" in file_names

    def validate():
        checkpoint = train.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint["loss"] == 3

    trainer.run(validate, checkpoint=trainer.best_checkpoint_path)


def test_load_checkpoint_from_path(ray_start_2_cpus, tmpdir):
    config = TestConfig()

    checkpoint_strategy = CheckpointStrategy(
        checkpoint_score_attribute="loss", checkpoint_score_order="min"
    )

    def train_func_checkpoint():
        train.save_checkpoint(loss=3)
        train.save_checkpoint(loss=7)

    trainer = Trainer(config, num_workers=2, logdir=tmpdir)
    trainer.start()
    trainer.run(train_func_checkpoint, checkpoint_strategy=checkpoint_strategy)

    assert trainer.best_checkpoint["loss"] == 3
    assert (
        Trainer.load_checkpoint_from_path(trainer.best_checkpoint_path)
        == trainer.best_checkpoint
    )


def test_persisted_checkpoint_strategy_failure(ray_start_2_cpus):
    logdir = "/tmp/test/trainer/test_persisted_checkpoint_strategy_failure"
    config = TestConfig()

    def train_func():
        train.save_checkpoint(epoch=0)

    trainer = Trainer(config, num_workers=2, logdir=logdir)
    trainer.start()

    with pytest.raises(ValueError):
        trainer.run(train_func, checkpoint_strategy=CheckpointStrategy(num_to_keep=-1))

    with pytest.raises(ValueError):
        trainer.run(
            train_func,
            checkpoint_strategy=CheckpointStrategy(
                checkpoint_score_order="invalid_order"
            ),
        )

    with pytest.raises(ValueError):
        trainer.run(
            train_func,
            checkpoint_strategy=CheckpointStrategy(
                checkpoint_score_attribute="missing_attribute"
            ),
        )


def test_world_rank(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        return train.world_rank()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(train_func)

    assert set(results) == {0, 1}


def test_torch_auto_unwrap(ray_start_2_cpus):
    """Tests if underlying model from DDP is extracted when saving ckpt."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in DDP.
        model = train.torch.prepare_model(model)

        # Save DDP wrapped model.
        train.save_checkpoint(model=model)

        # Report DDP wrapped model.
        train.report(model=model)

    num_workers = 2
    trainer = Trainer("torch", num_workers)
    trainer.start()

    class ValidateEncodedCallback(TrainingCallback):
        def handle_result(self, results, **info):
            for result in results:
                model = result["model"]
                assert isinstance(model, torch.nn.Module) and not isinstance(
                    model, torch.nn.parallel.DistributedDataParallel
                )

    trainer.run(train_fn, callbacks=[ValidateEncodedCallback()])

    last_checkpoint = trainer.latest_checkpoint
    model = last_checkpoint["model"]
    assert isinstance(model, torch.nn.Module) and not isinstance(
        model, torch.nn.parallel.DistributedDataParallel
    )

    trainer.shutdown()


def test_horovod_simple(ray_start_2_cpus):
    def simple_fn():
        hvd_torch.init()
        return hvd_torch.rank()

    num_workers = 2
    trainer = Trainer("horovod", num_workers)
    trainer.start()
    result = trainer.run(simple_fn)
    trainer.shutdown()

    assert result == list(range(num_workers))


def test_init_failure(ray_start_2_cpus):
    with pytest.raises(TypeError):
        Trainer(5, num_workers=2)

    with pytest.raises(ValueError):
        Trainer("invalid", num_workers=2)


def test_start_failure(ray_start_2_cpus):
    with pytest.raises(ValueError):
        trainer = Trainer("torch", num_workers=0)
        trainer.start()


def test_run_failure(ray_start_2_cpus):
    test_config = TestConfig()

    def train_invalid_signature(a, b):
        pass

    trainer = Trainer(test_config, num_workers=2)

    # Raise RuntimeError when trainer has not been started yet.
    with pytest.raises(RuntimeError):
        trainer.run(lambda: 1)

    trainer.start()

    with pytest.raises(ValueError):
        trainer.run(train_invalid_signature)

    trainer.shutdown()


def test_user_error(ray_start_2_cpus):
    """Tests if user training function raises an error"""

    config = TestConfig()

    def fail_train_1():
        raise NotImplementedError

    trainer = Trainer(config, num_workers=2)
    trainer.start()

    with pytest.raises(NotImplementedError):
        trainer.run(fail_train_1)

    def fail_train_2():
        for _ in range(2):
            train.report(loss=1)
        raise NotImplementedError

    with pytest.raises(NotImplementedError):
        trainer.run(fail_train_2)


def test_worker_failure_1(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        return 1

    def train_actor_failure():
        import sys

        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train_func)
        assert results == [1, 1]


def test_worker_failure_2(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        for _ in range(2):
            train.report(loss=1)
        return 1

    def train_actor_failure():
        for _ in range(2):
            train.report(loss=1)
        import sys

        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train_func)
        assert results == [1, 1]


def test_worker_failure_local_rank(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        return train.local_rank()

    def train_actor_failure():
        import sys

        sys.exit(0)
        return train.local_rank()

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.train.trainer, "BackendExecutor", new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train_func)
        assert set(results) == {0, 1}


def test_worker_start_failure(ray_start_2_cpus):
    test_config = TestConfig()

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

    with patch.object(ray.train.trainer, "BackendExecutor", TestBackendExecutor):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start(initialization_hook=init_hook_fail)
        assert len(trainer._backend_executor.get_worker_group()) == 2


def test_max_failures(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        import sys

        sys.exit(0)

    trainer = Trainer(test_config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(train_func)
    with pytest.raises(RuntimeError):
        iterator.get_final_results(force=True)
    assert iterator._backend_executor._get_num_failures() == 3


def test_start_max_failures(ray_start_2_cpus):
    test_config = TestConfig()

    trainer = Trainer(test_config, num_workers=2)

    def init_hook_fail():
        import sys

        sys.exit(0)

    with pytest.raises(RuntimeError):
        trainer.start(initialization_hook=init_hook_fail)


@pytest.mark.parametrize("backend", ["test", "torch", "tf", "horovod"])
def test_worker_kill(ray_start_2_cpus, backend):
    if backend == "test":
        test_config = TestConfig()
    elif backend == "torch":
        test_config = TorchConfig()
    elif backend == "tf":
        test_config = TensorflowConfig()
    elif backend == "horovod":
        test_config = HorovodConfig()

    trainer = Trainer(test_config, num_workers=2)

    def train_func():
        for i in range(2):
            train.report(loss=1, iter=i)

    trainer.start()
    kill_callback = KillCallback(fail_on=0, trainer=trainer)
    trainer.run(train_func, callbacks=[kill_callback])
    # Run 1: iter=0, counter=1, Successful
    # Run 2: iter=1, counter=1, Unsuccessful, starts training from beginning
    # Run 3: iter=0, counter=2, Successful
    # Run 4: iter=1, counter=3, Successful
    assert kill_callback.counter == 3

    trainer.shutdown()
    trainer.start()

    kill_callback = KillCallback(fail_on=1, trainer=trainer)
    trainer.run(train_func, callbacks=[kill_callback])
    # Run 1: iter=0, counter=1, Successful
    # Run 2: iter=1, counter=2, Successful
    # Run 3: None, counter=2, Unsuccessful, starts training from beginning.
    # Run 4: iter=0, counter=3, Successful
    # Run 5: iter=1, counter=4, Successful
    assert kill_callback.counter == 4

    def train_func():
        return 1

    # Make sure Trainer is usable even after failure handling.
    trainer.run(train_func)


def test_worker_kill_checkpoint(ray_start_2_cpus):
    test_config = TestConfig()

    def train_func():
        checkpoint = train.load_checkpoint()
        if checkpoint:
            epoch = checkpoint["epoch"]
        else:
            epoch = 0
        print("Epoch: ", epoch)
        for i in range(epoch, 2):
            train.report(loss=1, iter=i)
            train.save_checkpoint(epoch=i + 1)

    trainer = Trainer(test_config, num_workers=2)
    trainer.start()
    kill_callback = KillCallback(fail_on=0, trainer=trainer)

    trainer.run(train_func, callbacks=[kill_callback])

    # Run 1: epoch=0, counter=1, Successful
    # *Checkpoint is saved.*
    # *Worker is killed*
    # *Getting checkpoint fails. Workers are restarted from beginning*
    # Run 2: epoch=0, counter=2, Successful
    # Run 3: epoch=1, counter=3, Successful
    assert kill_callback.counter == 3
    assert trainer.latest_checkpoint["epoch"] == 2

    trainer.shutdown()
    trainer.start()

    kill_callback = KillCallback(fail_on=1, trainer=trainer)
    trainer.run(train_func, callbacks=[kill_callback])
    # Run 1: epoch=0, counter=1, Successful
    # *Checkpoint saved*
    # *Latest checkpoint updated, epoch=1
    # Run 2: epoch=1, counter=2, Successful
    # *Checkpoint saved*
    # *Worker is killed*
    # *Getting checkpoint fails. Workers are restarted from last checkpoint.*
    # Run 3: epoch=1, counter=3, Successful.
    assert kill_callback.counter == 3
    assert trainer.latest_checkpoint["epoch"] == 2

    def train_func():
        return 1

    # Make sure Trainer is usable even after failure handling.
    trainer.run(train_func)


def test_multiple_run(ray_start_2_cpus):
    config = TestConfig()

    def train_1():
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()

    output_1 = trainer.run(train_1)
    assert output_1 == [1, 1]

    def train_2():
        return 2

    output_2 = trainer.run(train_2)
    assert output_2 == [2, 2]


def test_run_after_user_error(ray_start_2_cpus):
    config = TestConfig()

    def fail_train():
        raise NotImplementedError

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    with pytest.raises(NotImplementedError):
        trainer.run(fail_train)

    def train_func():
        return 1

    output = trainer.run(train_func)
    assert output == [1, 1]


def check_dataset_output(num_data, num_epochs, data_all_epochs):
    assert all(len(worker_data) == num_epochs for worker_data in data_all_epochs)
    for i in range(num_epochs):
        epoch_data = []
        for worker_data in data_all_epochs:
            epoch_data.extend(worker_data[i])
        assert len(epoch_data) == num_data
        assert set(epoch_data) == set(range(num_data))


def test_dataset(ray_start_4_cpus):
    """Checks that Dataset is correctly sharded even with multiple epochs."""
    num_epochs = 2
    num_data = 10

    dataset = ray.data.range(num_data)

    def get_dataset():
        data_all_epochs = []
        for _ in range(2):
            data_this_epoch = []
            dataset = train.get_dataset_shard()
            for batch in dataset.iter_batches():
                data_this_epoch.extend(batch)
            data_all_epochs.append(data_this_epoch)
        return data_all_epochs

    config = TestConfig()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(get_dataset, dataset=dataset)
    check_dataset_output(num_data, num_epochs, results)
    trainer.shutdown()


def test_multiple_datasets(ray_start_4_cpus):
    num_epochs = 2
    num_data_1 = 10
    num_data_2 = 6

    train_data = ray.data.range(num_data_1)
    val_data = ray.data.range(num_data_2)

    def get_dataset():
        data_train_all_epochs = []
        data_val_all_epochs = []
        for _ in range(2):
            data_this_epoch_train = []
            train_dataset = train.get_dataset_shard("train")
            for batch in train_dataset.iter_batches():
                data_this_epoch_train.extend(batch)
            data_train_all_epochs.append(data_this_epoch_train)

            data_this_epoch_val = []
            val_dataset = train.get_dataset_shard("val")
            for batch in val_dataset.iter_batches():
                data_this_epoch_val.extend(batch)
            data_val_all_epochs.append(data_this_epoch_val)

        return data_train_all_epochs, data_val_all_epochs

    config = TestConfig()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(get_dataset, dataset={"train": train_data, "val": val_data})
    check_dataset_output(
        num_data_1, num_epochs, [worker_data[0] for worker_data in results]
    )
    check_dataset_output(
        num_data_2, num_epochs, [worker_data[1] for worker_data in results]
    )
    trainer.shutdown()


def test_dataset_pipeline(ray_start_4_cpus):
    """Checks that Pipeline is correctly sharded even with multiple epochs."""
    num_epochs = 2
    num_data = 10

    dataset = ray.data.range(num_data).repeat()

    def get_dataset():
        pipeline_iterator = train.get_dataset_shard().iter_epochs()
        data_all_epochs = []
        for _ in range(num_epochs):
            dataset_this_epoch = next(pipeline_iterator)
            data_this_epoch = []
            for batch in dataset_this_epoch.iter_batches(batch_format="native"):
                data_this_epoch.extend(batch)
            data_all_epochs.append(data_this_epoch)
        return data_all_epochs

    config = TestConfig()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(get_dataset, dataset=dataset)
    check_dataset_output(num_data, num_epochs, results)


def test_dataset_pipeline_shuffle(ray_start_4_cpus):
    num_epochs = 2
    num_data = 20

    dataset = ray.data.range(num_data).repeat().random_shuffle_each_window()

    def get_dataset():
        pipeline_iterator = train.get_dataset_shard().iter_epochs()
        data_all_epochs = []
        for _ in range(2):
            dataset_this_epoch = next(pipeline_iterator)
            data_this_epoch = []
            for batch in dataset_this_epoch.iter_batches(batch_format="native"):
                data_this_epoch.extend(batch)

            if len(data_all_epochs) > 0:
                # Make sure data is shuffled per epoch.
                assert data_this_epoch != data_all_epochs[-1]

            data_all_epochs.append(data_this_epoch)
        return data_all_epochs

    config = TestConfig()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(get_dataset, dataset=dataset)
    check_dataset_output(num_data, num_epochs, results)


def test_dataset_fault_tolerance(ray_start_4_cpus):
    dataset = ray.data.range(10)
    test_config = TestConfig()

    def train_func():
        return train.get_dataset_shard()

    def train_actor_failure():
        import sys

        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    class SingleGetDatasetShardsBackendExecutor(new_backend_executor_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._has_called_get_dataset_shards = False

        def _get_dataset_shards(self, dataset_or_dict):
            if self._has_called_get_dataset_shards:
                raise Exception
            self._has_called_get_dataset_shards = True
            return super()._get_dataset_shards(dataset_or_dict)

    with patch.object(
        ray.train.trainer, "BackendExecutor", SingleGetDatasetShardsBackendExecutor
    ):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        trainer.run(train_func, dataset=dataset)
        # No exception is raised by _get_dataset_shards


@pytest.mark.parametrize("resource", ["CPU", "GPU", "extra"])
@pytest.mark.parametrize("num_requested", [0.5, 1, 2])
def test_resources(ray_start_4_cpus_4_gpus_4_extra, resource, num_requested):
    num_workers = 2
    config = TestConfig()
    original = ray.available_resources().get(resource)
    resources_per_worker = {resource: num_requested}
    use_gpu = resource == "GPU"
    trainer = Trainer(
        config,
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker=resources_per_worker,
    )

    trainer.start()
    expected = original - num_workers * num_requested
    wait_for_condition(lambda: ray.available_resources().get(resource, 0) == expected)

    trainer.shutdown()
    wait_for_condition(lambda: ray.available_resources().get(resource, 0) == original)

    # Check that user input has not been modified
    assert resources_per_worker == {resource: num_requested}


def test_gpu_requests(ray_start_4_cpus_4_gpus_4_extra):
    class CudaTestBackend(TestBackend):
        share_cuda_visible_devices = True

    class CudaTestConfig(TestConfig):
        @property
        def backend_cls(self):
            return CudaTestBackend

    # GPUs should not be requested if `use_gpu` is False.
    with pytest.raises(ValueError):
        Trainer(
            CudaTestConfig(),
            num_workers=2,
            use_gpu=False,
            resources_per_worker={"GPU": 1},
        )

    # GPUs should not be set to 0 if `use_gpu` is True.
    with pytest.raises(ValueError):
        Trainer(
            CudaTestConfig(),
            num_workers=2,
            use_gpu=True,
            resources_per_worker={"GPU": 0},
        )

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    # 0 GPUs will be requested and should not raise an error.
    trainer = Trainer(CudaTestConfig(), num_workers=2, use_gpu=False)
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["", ""]
    trainer.shutdown()

    # 1 GPU will be requested and should not raise an error.
    trainer = Trainer(CudaTestConfig(), num_workers=2, use_gpu=True)
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["0,1", "0,1"]
    trainer.shutdown()

    # Partial GPUs should not raise an error.
    trainer = Trainer(
        CudaTestConfig(), num_workers=2, use_gpu=True, resources_per_worker={"GPU": 0.1}
    )
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["0", "0"]
    trainer.shutdown()

    # Multiple GPUs should not raise an error.
    trainer = Trainer(
        CudaTestConfig(), num_workers=2, use_gpu=True, resources_per_worker={"GPU": 2}
    )
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["0,1,2,3", "0,1,2,3"]
    trainer.shutdown()


def test_to_worker_group(ray_start_2_cpus):
    config = TestConfig()
    trainer = Trainer(config, num_workers=2)

    class Incrementer:
        def __init__(self, starting=0):
            self.count = starting

        def increment(self):
            self.count += 1

        def get_count(self):
            return self.count

    workers = trainer.to_worker_group(Incrementer, starting=2)
    assert ray.get([w.get_count.remote() for w in workers]) == [2, 2]

    ray.get([w.increment.remote() for w in workers])
    assert ray.get([w.get_count.remote() for w in workers]) == [3, 3]

    ray.get(workers[0].increment.remote())
    assert ray.get([w.get_count.remote() for w in workers]) == [4, 3]

    ray.get(workers[1].increment.remote())
    assert ray.get([w.get_count.remote() for w in workers]) == [4, 4]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
