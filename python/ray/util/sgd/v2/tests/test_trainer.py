import time
import pytest
from unittest.mock import patch

import tensorflow as tf
import torch

import ray
import ray.util.sgd.v2 as sgd
from ray.util.sgd.v2 import Trainer
from ray.util.sgd.v2.callbacks.callback import SGDCallback
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendInterface, \
    BackendExecutor
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func
from ray.util.sgd.v2.examples.train_linear import train_func as \
    linear_train_func
from ray.util.sgd.v2.examples.train_fashion_mnist import train_func as \
    fashion_mnist_train_func
from ray.util.sgd.v2.worker_group import WorkerGroup


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_2_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(BackendInterface):
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
        if i == 0:
            kwargs["train_func"] = special_f
        return self.workers[i].execute.remote(f, *args, **kwargs)

    return execute_single_async_special


def gen_new_backend_executor(special_f):
    """Returns a BackendExecutor that runs special_f on worker 0."""

    class TestBackendExecutor(BackendExecutor):
        def start_training(self, train_func):
            special_execute = gen_execute_single_async_special(special_f)
            with patch.object(WorkerGroup, "execute_single_async",
                              special_execute):
                super().start_training(train_func)

    return TestBackendExecutor


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
            sgd.report(index=i)

    def train_slow():
        sgd.report(index=0)
        time.sleep(5)
        sgd.report(index=1)

    new_backend_executor_cls = gen_new_backend_executor(train_slow)
    callback = TestCallback()

    with patch.object(ray.util.sgd.v2.trainer, "BackendExecutor",
                      new_backend_executor_cls):
        trainer = Trainer(test_config, num_workers=2)
        trainer.start()
        trainer.run(train, callbacks=[callback])

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


@pytest.mark.skipif(
    len(tf.config.list_physical_devices("GPU")) < 2,
    reason="Only run if multiple GPUs are available.")
def test_tensorflow_mnist_gpu(ray_start_2_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("tensorflow", num_workers=num_workers, use_gpu=True)
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


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Only run if multiple GPUs are available.")
def test_torch_fashion_mnist_gpu(ray_start_2_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=True)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(fashion_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1] < result[0]


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
        with pytest.raises(RuntimeError):
            trainer.run(train)

    # Make sure Trainer is shutdown after worker failure.
    with pytest.raises(RuntimeError):
        trainer.run(train)


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
        with pytest.raises(RuntimeError):
            trainer.run(train)


def test_worker_kill(ray_start_2_cpus):
    test_config = TestConfig()

    trainer = Trainer(test_config, num_workers=2)

    class KillCallback(SGDCallback):
        def __init__(self, fail_on):
            self.counter = 0
            self.fail_on = fail_on

        def handle_result(self, results):
            if self.counter == self.fail_on:
                ray.kill(trainer._executor.worker_group.workers[0])
            self.counter += 1

    def train_func():
        for _ in range(2):
            sgd.report(loss=1)

    trainer.start()

    with pytest.raises(RuntimeError):
        kill_callback = KillCallback(fail_on=0)
        trainer.run(train_func, callbacks=[kill_callback])

    trainer.start()

    with pytest.raises(RuntimeError):
        kill_callback = KillCallback(fail_on=1)
        trainer.run(train_func, callbacks=[kill_callback])

    def train():
        return 1

    # Make sure Trainer is shutdown after worker failure.
    with pytest.raises(RuntimeError):
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
