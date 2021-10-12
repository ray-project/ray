import pytest

import ray
from ray.util.sgd.v2 import Trainer
from ray.util.sgd.v2.examples.horovod.horovod_example import train_func as \
    horovod_torch_train_func
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func
from ray.util.sgd.v2.examples.train_fashion_mnist_example import train_func \
    as fashion_mnist_train_func
from test_tune import torch_fashion_mnist, tune_tensorflow_mnist


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
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


def test_torch_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
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


def test_horovod_torch_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    num_epochs = 2
    trainer = Trainer("horovod", num_workers, use_gpu=True)
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


def test_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_train_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.util.sgd.v2.examples.train_linear_dataset_example import \
        train_linear

    results = train_linear(num_workers=2, use_gpu=True)
    for result in results:
        assert result[-1]["loss"] < result[0]["loss"]


def test_tensorflow_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.util.sgd.v2.examples.tensorflow_linear_dataset_example import \
        train_tensorflow_linear

    results = train_tensorflow_linear(num_workers=2, use_gpu=True)
    for result in results:
        assert result[-1]["loss"] < result[0]["loss"]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
