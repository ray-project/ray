import time

import pytest
import ray
import tensorflow as tf
import torch
from ray.util.sgd.v2 import Trainer, TorchConfig
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func
from ray.util.sgd.v2.examples.train_fashion_mnist import train_func as \
    fashion_mnist_train_func
from ray.util.sgd.v2.examples.train_linear import train_func as \
    linear_train_func


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


def test_start_shutdown(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    trainer = Trainer("torch")
    trainer.start()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 1
    trainer.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2

    trainer = Trainer("torch", num_workers=2)
    trainer.start()
    time.sleep(1)
    assert "CPU" not in ray.available_resources()
    trainer.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2


def test_run(ray_start_2_cpus):
    def train_func():
        return 1

    trainer = Trainer("torch", num_workers=2)
    trainer.start()
    results = trainer.run(train_func)
    trainer.shutdown()

    assert len(results) == 2
    assert all(result == 1 for result in results)


def test_run_config(ray_start_2_cpus):
    def train_func(config):
        return config["fruit"]

    config = {"fruit": "banana"}

    trainer = Trainer("torch", num_workers=2)
    trainer.start()
    results = trainer.run(train_func, config)
    trainer.shutdown()

    assert len(results) == 2
    assert all(result == "banana" for result in results)


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
    len(tf.config.list_physical_devices('GPU')) < 2,
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
    def train_invalid_signature(a, b):
        pass

    def train_error(config):
        raise NotImplementedError

    trainer = Trainer("torch")
    trainer.start()

    with pytest.raises(ValueError):
        trainer.run(train_invalid_signature)

    with pytest.raises(NotImplementedError):
        trainer.run(train_error)
    trainer.shutdown()


def test_execute_worker_failure(ray_start_2_cpus):
    def train_actor_failure(config):
        ray.actor.exit_actor()

    trainer = Trainer("torch")
    trainer.start()
    with pytest.raises(RuntimeError):
        trainer.run(train_actor_failure)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
