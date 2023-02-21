import os
import tempfile
import time

import pytest
import sys
import io
from contextlib import redirect_stdout

import ray
from ray import tune
from ray.air import session, RunConfig
from ray.tune.progress_reporter import JupyterNotebookReporter
from ray.util.client.ray_client_helpers import ray_start_client_server


@pytest.fixture
def start_client_server():
    with ray_start_client_server() as client:
        yield client
    ray.shutdown()


@pytest.fixture
def start_client_server_2_cpus():
    ray.init(num_cpus=2)
    with ray_start_client_server() as client:
        yield client
    ray.shutdown()


@pytest.fixture
def start_client_server_4_cpus():
    ray.init(num_cpus=4)
    with ray_start_client_server() as client:
        yield client
    ray.shutdown()


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
    from ray.tune.examples.mlflow_example import tune_with_callback, tune_with_setup

    mlflow_tracking_uri = os.path.join(tempfile.gettempdir(), "mlruns")
    tune_with_callback(mlflow_tracking_uri, finish_fast=True)
    tune_with_setup(mlflow_tracking_uri, finish_fast=True)


def test_pbt_transformers(start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.pbt_transformers.pbt_transformers import tune_transformer

    tune_transformer(num_samples=1, gpus_per_trial=0, smoke_test=True)


def test_jupyter_rich_output(start_client_server_4_cpus):
    assert ray.util.client.ray.is_connected()

    def dummy_objective(config):
        time.sleep(1)
        session.report(dict(metric=1))

    ip = ray.util.get_node_ip_address()

    class MockJupyterNotebookReporter(JupyterNotebookReporter):
        def display(self, string: str) -> None:
            # Make sure display is called on the driver
            assert ip == ray.util.get_node_ip_address()
            if string:
                assert "<div" in string
                print(string)

    reporter = MockJupyterNotebookReporter()
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        tune.run(dummy_objective, num_samples=2, progress_reporter=reporter)
        print("", flush=True)
        assert "<div" in buffer.getvalue()

    reporter = MockJupyterNotebookReporter()
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        tuner = tune.Tuner(
            dummy_objective, run_config=RunConfig(progress_reporter=reporter)
        )
        tuner.fit()
        print("", flush=True)
        assert "<div" in buffer.getvalue()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
