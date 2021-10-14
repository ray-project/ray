import os
import time
from pathlib import Path
from unittest.mock import patch

import horovod.torch as hvd_torch
import pytest

import ray
import ray.util.sgd.v2 as sgd
from ray._private.test_utils import wait_for_condition
from ray.util.sgd.v2 import Trainer, TorchConfig, TensorflowConfig, \
    HorovodConfig
from ray.util.sgd.v2.backends.backend import BackendConfig, Backend, \
    BackendExecutor
from ray.util.sgd.v2.callbacks.callback import SGDCallback
from ray.util.sgd.v2.constants import ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV
from ray.util.sgd.v2.examples.horovod.horovod_example import train_func as \
    horovod_torch_train_func, HorovodTrainClass
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func
from ray.util.sgd.v2.examples.train_fashion_mnist_example import train_func \
    as fashion_mnist_train_func
from ray.util.sgd.v2.examples.train_linear_example import train_func as \
    linear_train_func
from ray.util.sgd.v2.worker_group import WorkerGroup


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

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TestConfig):
        pass


class TestCallback(SGDCallback):
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
            f, *args, **kwargs)

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
            with patch.object(WorkerGroup, "execute_single_async",
                              special_execute):
                super().start_training(*args, **kwargs)

    return TestBackendExecutor


class KillCallback(SGDCallback):
    def __init__(self, fail_on, worker_group):
        self.counter = 0
        self.fail_on = fail_on
        self.worker_group = worker_group

    def handle_result(self, results):
        print(results)
        assert all(r["loss"] == 1 for r in results)
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
            sgd.report(index=i)
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

    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)
            sgd.report(index=i)

    def train_slow():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)
            time.sleep(5)
            sgd.report(index=i)
            time.sleep(5)

    new_backend_executor_cls = gen_new_backend_executor(train_slow)
    callback = TestCallback()

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        trainer.run(train, callbacks=[callback])

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

    def train():
        for _ in range(2):
            sgd.report(loss=1)

    def train_mismatch():
        sgd.report(loss=1)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train)


def test_run_iterator(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        for i in range(3):
            sgd.report(index=i)
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(train_func)

    count = 0
    for results in iterator:
        assert (value["index"] == count for value in results)
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
            sgd.report(index=i)
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


def test_checkpoint(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        assert sgd.load_checkpoint() is None
        for i in range(3):
            sgd.save_checkpoint(epoch=i)
        return 1

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    trainer.run(train_func)
    checkpoint = trainer.latest_checkpoint

    assert checkpoint is not None
    assert checkpoint["epoch"] == 2

    def train_func_checkpoint():
        checkpoint = sgd.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint["epoch"] == 2

        for i in range(checkpoint["epoch"], 5):
            sgd.save_checkpoint(epoch=i)
        return 1

    trainer.run(train_func_checkpoint, checkpoint=checkpoint)
    checkpoint = trainer.latest_checkpoint

    assert checkpoint is not None
    assert checkpoint["epoch"] == 4


def test_mismatch_checkpoint(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)

    def train_mismatch():
        sgd.save_checkpoint(epoch=0)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train)


def test_mismatch_checkpoint_report(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)
            sgd.report(index=i)

    def train_mismatch():
        sgd.save_checkpoint(epoch=0)
        sgd.report(index=0)
        # skip checkpoint
        sgd.report(index=1)

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)
    callback = TestCallback()

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        with pytest.raises(RuntimeError):
            trainer.run(train, callbacks=[callback])
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
        checkpoint = sgd.load_checkpoint()
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


@pytest.mark.parametrize("logdir", [
    None, "/tmp/test/trainer/test_persisted_checkpoint",
    "~/tmp/test/trainer/test_persisted_checkpoint"
])
def test_persisted_checkpoint(ray_start_2_cpus, logdir):
    config = TestConfig()

    def train():
        for i in range(2):
            sgd.save_checkpoint(epoch=i)

    trainer = Trainer(config, num_workers=2, logdir=logdir)
    trainer.start()
    trainer.run(train)

    assert trainer.latest_checkpoint_path is not None
    if logdir is not None:
        assert trainer.logdir == Path(logdir).expanduser().resolve()
    assert trainer.latest_checkpoint_dir.is_dir()
    assert trainer.latest_checkpoint_path.is_file()
    assert trainer.latest_checkpoint_path.name == f"checkpoint_{2:06d}"
    assert trainer.latest_checkpoint_path.parent.name == "checkpoints"
    latest_checkpoint = trainer.latest_checkpoint

    def validate():
        checkpoint = sgd.load_checkpoint()
        assert checkpoint is not None
        assert checkpoint == latest_checkpoint

    trainer.run(validate, checkpoint=trainer.latest_checkpoint_path)


def test_world_rank(ray_start_2_cpus):
    config = TestConfig()

    def train_func():
        return sgd.world_rank()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(train_func)

    assert set(results) == {0, 1}


def test_tensorflow_mnist(ray_start_2_cpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("tensorflow", num_workers=num_workers)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(tensorflow_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers
    result = results[0]

    loss = result["loss"]
    assert len(loss) == epochs
    assert loss[-1] < loss[0]

    accuracy = result["accuracy"]
    assert len(accuracy) == epochs
    assert accuracy[-1] > accuracy[0]


def test_torch_linear(ray_start_2_cpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer.start()
    results = trainer.run(linear_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]


def test_torch_fashion_mnist(ray_start_2_cpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(fashion_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1] < result[0]


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


def test_horovod_torch_mnist(ray_start_2_cpus):
    num_workers = 2
    num_epochs = 2
    trainer = Trainer("horovod", num_workers)
    trainer.start()
    results = trainer.run(
        horovod_torch_train_func,
        config={
            "num_epochs": num_epochs,
            "lr": 1e-3
        })
    trainer.shutdown()

    assert len(results) == num_workers
    for worker_result in results:
        assert len(worker_result) == num_epochs
        assert worker_result[num_epochs - 1] < worker_result[0]


def test_horovod_torch_mnist_stateful(ray_start_2_cpus):
    num_workers = 2
    num_epochs = 2
    trainer = Trainer("horovod", num_workers)
    workers = trainer.to_worker_group(
        HorovodTrainClass, config={
            "num_epochs": num_epochs,
            "lr": 1e-3
        })
    results = []
    for epoch in range(num_epochs):
        results.append(ray.get([w.train.remote(epoch=epoch) for w in workers]))
    trainer.shutdown()

    assert len(results) == num_epochs
    for i in range(num_workers):
        assert results[num_epochs - 1][i] < results[0][i]


def test_init_failure(ray_start_2_cpus):
    with pytest.raises(TypeError):
        Trainer(5)

    with pytest.raises(ValueError):
        Trainer("invalid")


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
            sgd.report(loss=1)
        raise NotImplementedError

    with pytest.raises(NotImplementedError):
        trainer.run(fail_train_2)


def test_worker_failure_1(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        return 1

    def train_actor_failure():
        import sys
        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train)
        assert results == [1, 1]


def test_worker_failure_2(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        for _ in range(2):
            sgd.report(loss=1)
        return 1

    def train_actor_failure():
        for _ in range(2):
            sgd.report(loss=1)
        import sys
        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train)
        assert results == [1, 1]


def test_worker_failure_local_rank(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        return sgd.local_rank()

    def train_actor_failure():
        import sys
        sys.exit(0)
        return sgd.local_rank()

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        results = trainer.run(train)
        assert set(results) == {0, 1}


def test_worker_start_failure(ray_start_2_cpus):
    test_config = TestConfig()

    trainer = Trainer(test_config, num_workers=2)

    restart = trainer._executor._restart

    def init_hook():
        pass

    def init_hook_fail():
        ray.actor.exit_actor()

    def restart_patched(self):
        self._initialization_hook = init_hook
        restart()

    with patch.object(BackendExecutor, "_restart", restart_patched):
        trainer.start(initialization_hook=init_hook_fail)
        assert len(trainer._executor.worker_group) == 2


def test_max_failures(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        import sys
        sys.exit(0)

    trainer = Trainer(test_config, num_workers=2)
    trainer.start()
    iterator = trainer.run_iterator(train)
    with pytest.raises(RuntimeError):
        iterator.get_final_results(force=True)
    assert iterator._executor._num_failures == 3


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
            sgd.report(loss=1, iter=i)

    trainer.start()
    kill_callback = KillCallback(
        fail_on=0, worker_group=trainer._executor.worker_group)
    trainer.run(train_func, callbacks=[kill_callback])
    # Run 1: iter=0, counter=1, Successful
    # Run 2: iter=1, counter=1, Unsuccessful, starts training from beginning
    # Run 3: iter=0, counter=2, Successful
    # Run 4: iter=1, counter=3, Successful
    assert kill_callback.counter == 3

    trainer.shutdown()
    trainer.start()

    kill_callback = KillCallback(
        fail_on=1, worker_group=trainer._executor.worker_group)
    trainer.run(train_func, callbacks=[kill_callback])
    # Run 1: iter=0, counter=1, Successful
    # Run 2: iter=1, counter=2, Successful
    # Run 3: None, counter=2, Unsuccessful, starts training from beginning.
    # Run 4: iter=0, counter=3, Successful
    # Run 5: iter=1, counter=4, Successful
    assert kill_callback.counter == 4

    def train():
        return 1

    # Make sure Trainer is usable even after failure handling.
    trainer.run(train)


def test_worker_kill_checkpoint(ray_start_2_cpus):
    test_config = TestConfig()

    def train():
        checkpoint = sgd.load_checkpoint()
        if checkpoint:
            epoch = checkpoint["epoch"]
        else:
            epoch = 0
        print("Epoch: ", epoch)
        for i in range(epoch, 2):
            sgd.report(loss=1, iter=i)
            sgd.save_checkpoint(epoch=i + 1)

    trainer = Trainer(test_config, num_workers=2)
    trainer.start()
    kill_callback = KillCallback(
        fail_on=0, worker_group=trainer._executor.worker_group)

    trainer.run(train, callbacks=[kill_callback])

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

    kill_callback = KillCallback(
        fail_on=1, worker_group=trainer._executor.worker_group)
    trainer.run(train, callbacks=[kill_callback])
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

    def train():
        return 1

    # Make sure Trainer is usable even after failure handling.
    trainer.run(train)


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

    def train():
        return 1

    output = trainer.run(train)
    assert output == [1, 1]


def check_dataset_output(num_data, num_epochs, data_all_epochs):
    assert all(
        len(worker_data) == num_epochs for worker_data in data_all_epochs)
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
            dataset = sgd.get_dataset_shard()
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
            train_dataset = sgd.get_dataset_shard("train")
            for batch in train_dataset.iter_batches():
                data_this_epoch_train.extend(batch)
            data_train_all_epochs.append(data_this_epoch_train)

            data_this_epoch_val = []
            val_dataset = sgd.get_dataset_shard("val")
            for batch in val_dataset.iter_batches():
                data_this_epoch_val.extend(batch)
            data_val_all_epochs.append(data_this_epoch_val)

        return data_train_all_epochs, data_val_all_epochs

    config = TestConfig()

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    results = trainer.run(
        get_dataset, dataset={
            "train": train_data,
            "val": val_data
        })
    check_dataset_output(num_data_1, num_epochs,
                         [worker_data[0] for worker_data in results])
    check_dataset_output(num_data_2, num_epochs,
                         [worker_data[1] for worker_data in results])
    trainer.shutdown()


def test_dataset_pipeline(ray_start_4_cpus):
    """Checks that Pipeline is correctly sharded even with multiple epochs."""
    num_epochs = 2
    num_data = 10

    dataset = ray.data.range(num_data).repeat()

    def get_dataset():
        pipeline_iterator = sgd.get_dataset_shard().iter_datasets()
        data_all_epochs = []
        for _ in range(num_epochs):
            dataset_this_epoch = next(pipeline_iterator)
            data_this_epoch = []
            for batch in dataset_this_epoch.iter_batches():
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
        pipeline_iterator = sgd.get_dataset_shard().iter_datasets()
        data_all_epochs = []
        for _ in range(2):
            dataset_this_epoch = next(pipeline_iterator)
            data_this_epoch = []
            for batch in dataset_this_epoch.iter_batches():
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
    dataset_splits = dataset.split(n=2, equal=True)
    test_config = TestConfig()

    def train():
        return 1

    def train_actor_failure():
        import sys
        sys.exit(0)

    new_backend_executor_cls = gen_new_backend_executor(train_actor_failure)

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        with patch.object(
                new_backend_executor_cls,
                "_get_dataset_shards",
                return_value=dataset_splits) as mock_method:
            trainer = Trainer(test_config, num_workers=2)
            trainer.start()
            trainer.run(train, dataset=dataset)
            mock_method.assert_called_once()


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
        resources_per_worker=resources_per_worker)

    trainer.start()
    expected = original - num_workers * num_requested
    wait_for_condition(
        lambda: ray.available_resources().get(resource, 0) == expected)

    trainer.shutdown()
    wait_for_condition(
        lambda: ray.available_resources().get(resource, 0) == original)


def test_gpu_requests(ray_start_4_cpus_4_gpus_4_extra):
    # GPUs should not be requested if `use_gpu` is False.
    with pytest.raises(ValueError):
        Trainer(
            TestConfig(),
            num_workers=2,
            use_gpu=False,
            resources_per_worker={"GPU": 1})

    # GPUs should not be set to 0 if `use_gpu` is True.
    with pytest.raises(ValueError):
        Trainer(
            TestConfig(),
            num_workers=2,
            use_gpu=True,
            resources_per_worker={"GPU": 0})

    def get_resources():
        return os.environ["CUDA_VISIBLE_DEVICES"]

    os.environ[ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV] = "1"

    # 0 GPUs will be requested and should not raise an error.
    trainer = Trainer(TestConfig(), num_workers=2, use_gpu=False)
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["", ""]
    trainer.shutdown()

    # 1 GPU will be requested and should not raise an error.
    trainer = Trainer(TestConfig(), num_workers=2, use_gpu=True)
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["0,1", "0,1"]
    trainer.shutdown()

    # Partial GPUs should not raise an error.
    trainer = Trainer(
        TestConfig(),
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"GPU": 0.1})
    trainer.start()
    result = trainer.run(get_resources)
    assert result == ["0", "0"]
    trainer.shutdown()

    # Multiple GPUs should not raise an error.
    trainer = Trainer(
        TestConfig(),
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"GPU": 2})
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
