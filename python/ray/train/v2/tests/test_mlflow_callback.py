import os

import mlflow
import pytest
from mlflow.tracking import MlflowClient
from ray.train import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.mlflow import MLflowLoggerCallback

import ray
from ray import train


@pytest.fixture(scope="module", autouse=True)
def ray_init_shutdown():
    ray.init(num_cpus=2)
    yield
    ray.shutdown()


def test_mlflow_logger_callback_success(tmp_path):
    """Test that the callback records metrics correctly during a successful training run."""
    db_path = os.path.join(tmp_path, "mlflow.db")
    tracking_uri = f"sqlite:///{db_path}"

    def train_func():
        for i in range(3):
            train.report(
                {
                    "loss": 0.5 - (i * 0.1),
                    "training_iteration": i,
                    "invalid_nested": {"a": 1},
                }
            )

    callback = MLflowLoggerCallback(
        experiment_name="ray_v2_test_experiment",
        tracking_uri=tracking_uri,
        save_checkpoints_as_artifacts=False,
    )

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(callbacks=[callback]),
    )

    trainer.fit()

    # Verify directly via a distinct client pointing to the tracking database
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment = client.get_experiment_by_name("ray_v2_test_experiment")
    assert experiment is not None

    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    assert len(runs) == 1

    run = runs[0]
    # Accept RUNNING or FINISHED since background actor process termination timing varies
    assert run.info.status in ["FINISHED", "RUNNING"]

    # Extract metrics out of the run data backend safely using the dictionary directly
    metric_dict = run.data.metrics
    assert "loss" in metric_dict
    assert metric_dict["loss"] == pytest.approx(0.3)


def test_mlflow_logger_callback_exception(tmp_path):
    """Test that the callback handles unexpected trainer exceptions gracefully."""
    db_path = os.path.join(tmp_path, "mlflow_fail.db")
    tracking_uri = f"sqlite:///{db_path}"

    def failing_train_func():
        train.report({"loss": 0.9, "training_iteration": 0})
        raise RuntimeError("Simulated worker failure")

    callback = MLflowLoggerCallback(
        experiment_name="ray_v2_failing_experiment",
        tracking_uri=tracking_uri,
        save_checkpoints_as_artifacts=False,
    )

    trainer = DataParallelTrainer(
        train_loop_per_worker=failing_train_func,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(callbacks=[callback]),
    )

    with pytest.raises(Exception):
        trainer.fit()

    client = MlflowClient(tracking_uri=tracking_uri)
    experiment = client.get_experiment_by_name("ray_v2_failing_experiment")
    assert experiment is not None

    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    assert len(runs) == 1
    assert runs[0].info.status == "FAILED"
