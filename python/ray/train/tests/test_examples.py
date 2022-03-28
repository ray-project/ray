import pytest

import ray
from ray.train import Trainer
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
    HorovodTrainClass,
)
from ray.train.examples.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.tensorflow_quick_start import (
    train_func as tf_quick_start_train_func,
)
from ray.train.examples.torch_quick_start import (
    train_func as torch_quick_start_train_func,
)
from ray.train.examples.train_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.examples.train_linear_example import train_func as linear_train_func
from ray.train.tests.test_trainer import KillCallback


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_tensorflow_mnist(ray_start_2_cpus, num_workers):
    num_workers = num_workers
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


def test_tf_non_distributed(ray_start_2_cpus):
    """Make sure Ray Train works without TF MultiWorkerMirroredStrategy."""

    trainer = Trainer(backend="torch", num_workers=1)
    trainer.start()
    trainer.run(tf_quick_start_train_func)
    trainer.shutdown()


def test_tensorflow_mnist_fail(ray_start_2_cpus):
    """Tests if tensorflow example works even with worker failure."""
    epochs = 3

    trainer = Trainer("tensorflow", num_workers=2)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    kill_callback = KillCallback(fail_on=0, trainer=trainer)
    results = trainer.run(
        tensorflow_mnist_train_func, config, callbacks=[kill_callback]
    )
    trainer.shutdown()

    assert len(results) == 2
    result = results[0]

    loss = result["loss"]
    assert len(loss) == epochs
    assert loss[-1] < loss[0]

    accuracy = result["accuracy"]
    assert len(accuracy) == epochs
    assert accuracy[-1] > accuracy[0]


@pytest.mark.parametrize("num_workers", [1, 2])
def test_torch_linear(ray_start_2_cpus, num_workers):
    num_workers = num_workers
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


def test_torch_non_distributed(ray_start_2_cpus):
    """Make sure Ray Train works without torch DDP."""

    trainer = Trainer(backend="torch", num_workers=1)
    trainer.start()
    trainer.run(torch_quick_start_train_func)
    trainer.shutdown()


def test_horovod_torch_mnist(ray_start_2_cpus):
    num_workers = 2
    num_epochs = 2
    trainer = Trainer("horovod", num_workers)
    trainer.start()
    results = trainer.run(
        horovod_torch_train_func, config={"num_epochs": num_epochs, "lr": 1e-3}
    )
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
        HorovodTrainClass, config={"num_epochs": num_epochs, "lr": 1e-3}
    )
    results = []
    for epoch in range(num_epochs):
        results.append(ray.get([w.train.remote(epoch=epoch) for w in workers]))
    trainer.shutdown()

    assert len(results) == num_epochs
    for i in range(num_workers):
        assert results[num_epochs - 1][i] < results[0][i]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
