import os
import tempfile
import time

import pytest
import sys
import io
from contextlib import redirect_stdout

import ray
from ray import train, tune
from ray.train import RunConfig
from ray.tune import Tuner
from ray.tune.progress_reporter import JupyterNotebookReporter
from ray.util.client.ray_client_helpers import ray_start_client_server

from ray.train.tests.util import create_dict_checkpoint


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
def legacy_progress_reporter():
    old_val = os.environ.get("RAY_AIR_NEW_OUTPUT")
    os.environ["RAY_AIR_NEW_OUTPUT"] = "0"
    yield
    if old_val is None:
        os.environ.pop("RAY_AIR_NEW_OUTPUT")
    else:
        os.environ["RAY_AIR_NEW_OUTPUT"] = old_val


@pytest.fixture
def start_client_server_4_cpus():
    ray.init(num_cpus=4)
    with ray_start_client_server() as client:
        yield client
    ray.shutdown()


def test_tuner_client_get_results(
    tmp_path, legacy_progress_reporter, start_client_server_2_cpus
):
    def train_fn(config):
        checkpoint = train.get_checkpoint()
        id = int(bool(checkpoint))
        result = {"id": id}
        with create_dict_checkpoint(result) as checkpoint:
            train.report(result, checkpoint=checkpoint)
        raise RuntimeError

    results = Tuner(train_fn, run_config=RunConfig(storage_path=str(tmp_path))).fit()
    restored_results = Tuner.restore(
        results.experiment_path, trainable=train_fn, resume_errored=True
    ).get_results()

    # Assert that the intermediate results are returned,
    # without launching the tuning job.
    assert restored_results[0].metrics["id"] == 0


def test_pbt_function(legacy_progress_reporter, start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.pbt_function import run_tune_pbt

    run_tune_pbt()


def test_optuna_example(legacy_progress_reporter, start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.optuna_example import run_optuna_tune

    run_optuna_tune(smoke_test=True)


def test_cifar10_pytorch(legacy_progress_reporter, start_client_server_2_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.cifar10_pytorch import main

    # This is a workaround that is needed because of
    # https://github.com/grpc/grpc/issues/32758 -- this happens only
    # if the DataLoader is used on the Ray Client side, which we should
    # not encourage anyways (heavy processing should be done on the cluster).
    import torch

    torch.multiprocessing.set_start_method("spawn")

    main(num_samples=1, max_num_epochs=1, gpus_per_trial=0)


def test_tune_mnist_keras(legacy_progress_reporter, start_client_server_4_cpus):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.tune_mnist_keras import tune_mnist

    tune_mnist(num_training_iterations=2)


def test_mnist_ptl_mini(legacy_progress_reporter, start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.mnist_ptl_mini import tune_mnist

    tune_mnist(num_samples=1, num_epochs=1, gpus_per_trial=0)


def test_xgboost_example(legacy_progress_reporter, start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.xgboost_example import tune_xgboost

    tune_xgboost()


def test_xgboost_dynamic_resources_example(
    legacy_progress_reporter, start_client_server
):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.xgboost_dynamic_resources_example import tune_xgboost

    tune_xgboost(use_class_trainable=True)
    tune_xgboost(use_class_trainable=False)


def test_mlflow_example(legacy_progress_reporter, start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.mlflow_example import tune_with_callback, tune_with_setup

    mlflow_tracking_uri = os.path.join(tempfile.gettempdir(), "mlruns")
    tune_with_callback(mlflow_tracking_uri, finish_fast=True)
    tune_with_setup(mlflow_tracking_uri, finish_fast=True)


@pytest.mark.skip("Transformers relies on an older verison of Tune.")
def test_pbt_transformers(legacy_progress_reporter, start_client_server):
    assert ray.util.client.ray.is_connected()
    from ray.tune.examples.pbt_transformers.pbt_transformers import tune_transformer

    tune_transformer(num_samples=1, gpus_per_trial=0, smoke_test=True)


def test_jupyter_rich_output(legacy_progress_reporter, start_client_server_4_cpus):
    assert ray.util.client.ray.is_connected()

    def dummy_objective(config):
        time.sleep(1)
        train.report(dict(metric=1))

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

    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
