import pytest

import ray
from ray.train import Trainer
from ray.air.config import ScalingConfig
from ray.train.constants import TRAINING_ITERATION
from ray.train.examples.horovod.horovod_example import HorovodTrainClass
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
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
from ray.train.examples.torch_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.examples.torch_linear_example import train_func as linear_train_func
from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.tests.test_trainer import KillCallback
from ray.train.torch.torch_trainer import TorchTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_tensorflow_mnist(ray_start_4_cpus, num_workers):
    num_workers = num_workers
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    results = trainer.fit()

    result = results.metrics

    assert result[TRAINING_ITERATION] == epochs

    loss = list(results.metrics_dataframe["loss"])
    assert len(loss) == epochs
    assert loss[-1] < loss[0]


def test_tf_non_distributed(ray_start_4_cpus):
    """Make sure Ray Train works without TF MultiWorkerMirroredStrategy."""

    trainer = TorchTrainer(
        tf_quick_start_train_func, scaling_config=ScalingConfig(num_workers=1)
    )
    trainer.fit()


# TODO: Refactor as a backend test.
def test_tensorflow_mnist_fail(ray_start_4_cpus):
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
def test_torch_linear(ray_start_4_cpus, num_workers):
    num_workers = num_workers
    epochs = 3

    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer = TorchTrainer(
        linear_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    results = trainer.fit()

    result = results.metrics
    assert result[TRAINING_ITERATION] == epochs

    loss = list(results.metrics_dataframe["loss"])
    assert len(loss) == epochs
    assert loss[-1] < loss[0]


# TODO: Refactor as a backend test.
def test_torch_linear_failure(ray_start_4_cpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer.start()
    kill_callback = KillCallback(fail_on=1, trainer=trainer)
    results = trainer.run(linear_train_func, config, callbacks=[kill_callback])
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]


def test_torch_fashion_mnist(ray_start_4_cpus):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    results = trainer.fit()

    result = results.metrics
    assert result[TRAINING_ITERATION] == epochs

    loss = list(results.metrics_dataframe["loss"])
    assert len(loss) == epochs
    assert loss[-1] < loss[0]


def test_torch_non_distributed(ray_start_4_cpus):
    """Make sure Ray Train works without torch DDP."""

    trainer = TorchTrainer(
        torch_quick_start_train_func, scaling_config=ScalingConfig(num_workers=1)
    )
    trainer.fit()


def test_horovod_torch_mnist(ray_start_4_cpus):
    num_workers = 2
    num_epochs = 2
    trainer = HorovodTrainer(
        horovod_torch_train_func,
        train_loop_config={"num_epochs": num_epochs, "lr": 1e-3},
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_workers

    loss = list(results.metrics_dataframe["loss"])
    assert len(loss) == num_epochs
    assert loss[-1] < loss[0]


# TODO: Refactor as a backend test.
def test_horovod_torch_mnist_stateful(ray_start_4_cpus):
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
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
