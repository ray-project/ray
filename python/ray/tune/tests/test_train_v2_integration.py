import sys

import pytest

import ray.train
import ray.tune
from ray.cluster_utils import Cluster
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

TRAIN_DRIVER_RESOURCE_NAME = "train_driver_resource"
NUM_GPUS_IN_CLUSTER = 4


@pytest.fixture(scope="module")
def ray_cpu_head_gpu_worker():
    cluster = Cluster()
    cluster.add_node(resources={TRAIN_DRIVER_RESOURCE_NAME: 1})
    cluster.add_node(num_cpus=0, num_gpus=NUM_GPUS_IN_CLUSTER)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture(autouse=True)
def speed_up_tests(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.1")


@pytest.mark.parametrize("num_workers_grid_search", [[1], [1, 2, 4]])
@pytest.mark.parametrize("limit_concurrency", [True, False])
def test_e2e(
    ray_cpu_head_gpu_worker,
    tmp_path,
    num_workers_grid_search,
    limit_concurrency,
):
    def train_fn_per_worker(train_fn_config):
        world_size = ray.train.get_context().get_world_size()
        with create_dict_checkpoint({"model": "dummy"}) as checkpoint:
            ray.train.report(
                {"loss": 0.1, "world_size": world_size}, checkpoint=checkpoint
            )

    def launch_training(tune_config):
        # TODO: Add TuneReportCallback to report intermediate metrics
        trainer = DataParallelTrainer(
            train_loop_per_worker=train_fn_per_worker,
            train_loop_config=tune_config["train_loop_config"],
            scaling_config=ray.train.ScalingConfig(
                num_workers=tune_config["num_workers"], use_gpu=True
            ),
            run_config=ray.tune.RunConfig(
                storage_path=tmp_path,
                name=f"train-{ray.tune.get_context().get_trial_id()}",
            ),
        )
        result = trainer.fit()

        metrics = result.metrics.copy()
        metrics["checkpoint_path"] = result.checkpoint.path
        ray.tune.report(metrics)

    tuner = ray.tune.Tuner(
        ray.tune.with_resources(launch_training, {TRAIN_DRIVER_RESOURCE_NAME: 0.01}),
        param_space={
            # Search over parameters passed into each train worker.
            "train_loop_config": {"lr": ray.tune.choice([0.01, 0.001])},
            # Search over Train "run level" parameters.
            "num_workers": ray.tune.grid_search(num_workers_grid_search),
        },
        tune_config=ray.tune.TuneConfig(
            max_concurrent_trials=(
                NUM_GPUS_IN_CLUSTER // max(num_workers_grid_search)
                if limit_concurrency
                else None
            )
        ),
        run_config=ray.tune.RunConfig(storage_path=tmp_path, name="tune"),
    )
    result_grid = tuner.fit()
    assert len(result_grid) == len(num_workers_grid_search)

    world_sizes = set()
    for result in result_grid:
        assert "loss" in result.metrics
        assert "checkpoint_path" in result.metrics
        world_sizes.add(result.metrics["world_size"])
    assert world_sizes == set(num_workers_grid_search)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
