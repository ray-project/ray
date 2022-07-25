import os
import tempfile

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
def start_client_server_4_cpus():
    ray.init(num_cpus=4)
    with ray_start_client_server() as client:
        yield client


def test_pbt_function(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.pbt_function import run_tune_pbt

    run_tune_pbt()


def test_optuna_example(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.optuna_example import run_optuna_tune

    run_optuna_tune(smoke_test=True)


def test_cifar10_pytorch(start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.cifar10_pytorch import main

    main(num_samples=1, max_num_epochs=1, gpus_per_trial=0)


def test_tune_mnist_keras(start_client_server_4_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.tune_mnist_keras import tune_mnist

    tune_mnist(num_training_iterations=5)


def test_mnist_ptl_mini(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.mnist_ptl_mini import tune_mnist

    tune_mnist(num_samples=1, num_epochs=1, gpus_per_trial=0)


def test_xgboost_example(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.xgboost_example import tune_xgboost

    tune_xgboost()


def test_xgboost_dynamic_resources_example(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.xgboost_dynamic_resources_example import tune_xgboost

    tune_xgboost(use_class_trainable=True)
    tune_xgboost(use_class_trainable=False)


def test_mlflow_example(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.mlflow_example import tune_function, tune_decorated

    mlflow_tracking_uri = os.path.join(tempfile.gettempdir(), "mlruns")
    tune_function(mlflow_tracking_uri, finish_fast=True)
    tune_decorated(mlflow_tracking_uri, finish_fast=True)


def test_pbt_transformers(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.pbt_transformers.pbt_transformers import tune_transformer

    tune_transformer(num_samples=1, gpus_per_trial=0, smoke_test=True)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
