import sys

import pytest

from ray.train import ScalingConfig
from ray.train.examples.pytorch.torch_fashion_mnist_example import (
    train_func_per_worker as fashion_mnist_train_func,
)
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.examples.pytorch.torch_quick_start import (
    train_func as torch_quick_start_train_func,
)
from ray.train.examples.tf.tensorflow_quick_start import (
    train_func as tf_quick_start_train_func,
)
from ray.train.torch import TorchTrainer


@pytest.mark.parametrize("num_workers", [1, 2])
@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="tensorflow is not supported in python 3.12+"
)
def test_tensorflow_mnist(ray_start_4_cpus, num_workers):
    from ray.train.examples.tf.tensorflow_mnist_example import (
        train_func as tensorflow_mnist_train_func,
    )
    from ray.train.tensorflow import TensorflowTrainer

    num_workers = num_workers
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    trainer.fit()


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="tensorflow is not supported in python 3.12+"
)
def test_tf_non_distributed(ray_start_4_cpus):
    """Make sure Ray Train works without TF MultiWorkerMirroredStrategy."""

    from ray.train.tensorflow import TensorflowTrainer

    trainer = TensorflowTrainer(
        tf_quick_start_train_func, scaling_config=ScalingConfig(num_workers=1)
    )
    trainer.fit()


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
    trainer.fit()


def test_torch_fashion_mnist(ray_start_4_cpus):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size_per_worker": 32, "epochs": epochs}
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers),
    )
    trainer.fit()


def test_torch_non_distributed(ray_start_4_cpus):
    """Make sure Ray Train works without torch DDP."""

    trainer = TorchTrainer(
        torch_quick_start_train_func, scaling_config=ScalingConfig(num_workers=1)
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
