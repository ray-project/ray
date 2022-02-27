import pytest
import sys

import ray
from ray.util.client.ray_client_helpers import ray_start_client_server


@pytest.fixture
def start_client_server():
    with ray_start_client_server() as client:
        yield client


@pytest.fixture
def start_client_server_2_cpus():
    ray.init(num_cpus=2)
    with ray_start_client_server() as client:
        yield client


@pytest.fixture
def start_client_server_3_cpus():
    ray.init(num_cpus=3)
    with ray_start_client_server() as client:
        yield client


@pytest.fixture
def start_client_server_4_cpus():
    ray.init(num_cpus=4)
    with ray_start_client_server() as client:
        yield client


def test_train_example(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.torch.examples.train_example import train_example

    train_example(num_workers=2, use_gpu=False)


def test_tune_example(start_client_server_3_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.torch.examples.tune_example import (
        tune_example,
        get_custom_training_operator,
    )

    CustomTrainingOperator = get_custom_training_operator()
    tune_example(CustomTrainingOperator, num_workers=2)


def test_tune_example_manual(start_client_server_3_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.torch.examples.tune_example import (
        tune_example_manual,
        get_custom_training_operator,
    )

    CustomTrainingOperator = get_custom_training_operator(lr_reduce_on_plateau=True)
    tune_example_manual(CustomTrainingOperator, num_workers=2)


def test_dcgan(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.torch.examples.dcgan import train_example

    trainer = train_example(num_workers=2, use_gpu=False, test_mode=True)
    _ = trainer.get_model()


def test_cifar_pytorch_example(start_client_server_4_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.torch.examples.cifar_pytorch_example import train_cifar

    train_cifar(test_mode=True, num_workers=2)


def test_tf_train_example(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.tf.examples.tensorflow_train_example import train_example

    train_example(num_replicas=2, use_gpu=False)


def test_tf_cifar_example(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.util.sgd.tf.examples.cifar_tf_example import main

    main(num_replicas=2, smoke_test=True)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
