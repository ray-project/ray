import sys

import pytest

import ray.train
import ray.tune
from ray.cluster_utils import Cluster
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

TRAIN_DRIVER_RESOURCE_NAME = "train_driver_resource"
NUM_GPUS = 4


@pytest.fixture
def ray_cpu_head_gpu_worker():
    cluster = Cluster()
    cluster.add_node(resources={TRAIN_DRIVER_RESOURCE_NAME: 1})
    cluster.add_node(num_cpus=0, num_gpus=NUM_GPUS)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture(autouse=True)
def speed_up_tests(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.1")


@pytest.mark.parametrize("num_trials", [1, 4])
@pytest.mark.parametrize("limit_concurrency", [True, False])
def test_e2e(
    ray_cpu_head_gpu_worker,
    num_trials,
    limit_concurrency,
):
    num_workers_per_trial = 2

    def train_fn_per_worker(train_fn_config):
        with create_dict_checkpoint({"model": "dummy"}) as checkpoint:
            ray.train.report({"loss": 0.1}, checkpoint=checkpoint)

    def launch_training(tune_config):
        # TODO: Add TuneReportCallback to report intermediate metrics

        trainer = DataParallelTrainer(
            train_loop_per_worker=train_fn_per_worker,
            train_loop_config=tune_config["train_loop_config"],
            scaling_config=ray.train.ScalingConfig(
                num_workers=num_workers_per_trial, use_gpu=True
            ),
        )
        result = trainer.fit()

        metrics = result.metrics.copy()
        metrics["checkpoint_path"] = result.checkpoint.path
        ray.tune.report(metrics)

    tuner = ray.tune.Tuner(
        ray.tune.with_resources(launch_training, {TRAIN_DRIVER_RESOURCE_NAME: 0.01}),
        param_space={
            "train_loop_config": {
                "trial_idx": ray.tune.grid_search(list(range(num_trials)))
            }
        },
        tune_config=ray.tune.TuneConfig(
            max_concurrent_trials=(
                NUM_GPUS // num_workers_per_trial if limit_concurrency else None
            )
        ),
    )
    result_grid = tuner.fit()
    assert len(result_grid) == num_trials
    for result in result_grid:
        assert "loss" in result.metrics
        assert "checkpoint_path" in result.metrics


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
