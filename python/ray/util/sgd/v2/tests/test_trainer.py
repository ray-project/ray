import time

import pytest
import ray
import ray.util.sgd.v2 as sgd
from ray.util.sgd.v2 import Trainer, TorchConfig
from ray.util.sgd.v2.callbacks.callback import SGDCallback
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendInterface
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


def test_start_shutdown(ray_start_2_cpus):
    config = TestConfig()
    assert ray.available_resources()["CPU"] == 2
    trainer = Trainer(config)
    trainer.start()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 1
    trainer.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2

    trainer = Trainer(config, num_workers=2)
    trainer.start()
    time.sleep(1)
    assert "CPU" not in ray.available_resources()
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
    config = TestConfig()

    def train_func(config):
        return config["fruit"]

    config = {"fruit": "banana"}

    trainer = Trainer(config, num_workers=2)
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


def test_torch_linear(ray_start_2_cpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer(TorchConfig(backend="gloo"), num_workers=num_workers)
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
    def train_invalid_signature(a, b):
        pass

    def train_error(config):
        raise NotImplementedError

    trainer = Trainer("torch", num_workers=2)

    # Raise RuntimeError when trainer has not been started yet.
    with pytest.raises(RuntimeError):
        trainer.run(lambda: 1)

    trainer.start()

    with pytest.raises(ValueError):
        trainer.run(train_invalid_signature)

    with pytest.raises(NotImplementedError):
        trainer.run(train_error)
    trainer.shutdown()


def test_worker_failure(ray_start_2_cpus):
    def train_actor_failure(config):
        import sys
        sys.exit(0)

    trainer = Trainer("torch")
    trainer.start()
    with pytest.raises(RuntimeError):
        trainer.run(train_actor_failure)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
