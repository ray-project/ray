import sys

import pytest

import ray.train
import ray.tune
from ray._common.test_utils import wait_for_condition
from ray.cluster_utils import Cluster
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.tune.integration.ray_train import CHECKPOINT_PATH_KEY, TuneReportCallback
from ray.util.state import list_tasks

TRAIN_DRIVER_RESOURCE_NAME = "train_driver_resource"
NUM_GPUS_IN_CLUSTER = 4


@pytest.fixture()
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture()
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
    num_non_checkpoint_reports = 2
    num_checkpoint_reports = 1

    def train_fn_per_worker(train_fn_config):
        assert "lr" in train_fn_config

        world_size = ray.train.get_context().get_world_size()
        for i in range(num_non_checkpoint_reports):
            ray.train.report({"idx": i})

        for i in range(num_checkpoint_reports):
            with create_dict_checkpoint({"model": "dummy"}) as checkpoint:
                ray.train.report(
                    {"loss": 0.1, "world_size": world_size}, checkpoint=checkpoint
                )

    def launch_training(tune_config):
        trainer = DataParallelTrainer(
            train_loop_per_worker=train_fn_per_worker,
            train_loop_config=tune_config["train_loop_config"],
            scaling_config=ray.train.ScalingConfig(
                num_workers=tune_config["num_workers"], use_gpu=True
            ),
            run_config=ray.train.RunConfig(
                storage_path=tmp_path,
                name=f"train-{ray.tune.get_context().get_trial_id()}",
                callbacks=[TuneReportCallback()],
            ),
        )
        trainer.fit()

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
        assert (
            len(result.metrics_dataframe)
            == num_non_checkpoint_reports + num_checkpoint_reports
        )
        assert "loss" in result.metrics
        assert CHECKPOINT_PATH_KEY in result.metrics
        world_sizes.add(result.metrics["world_size"])
    assert world_sizes == set(num_workers_grid_search)


def test_errors(ray_start_4_cpus):
    """Test that errors in training are properly captured and reported."""

    def train_worker_fn():
        raise RuntimeError("Simulated training error")

    def train_fn(config):
        trainer = DataParallelTrainer(train_worker_fn)
        trainer.fit()

    tuner = ray.tune.Tuner(train_fn)

    results = tuner.fit()

    assert results.errors, "Expected errors to be captured"
    assert len(results.errors) == 1, "Expected exactly one error"

    error = results.errors[0]
    assert "RuntimeError" in str(error), f"Expected RuntimeError, got: {error}"
    assert "Simulated training error" in str(
        error
    ), f"Expected specific error message, got: {error}"


def test_reuse_actors(ray_start_4_cpus, tmp_path):
    """Trials that reuse one Tune actor get all their results, and waiting for
    results from Train records no failed tasks."""
    num_reports = 3

    def train_fn_per_worker():
        for i in range(num_reports):
            ray.train.report({"idx": i})

    def launch_training(tune_config):
        trainer = DataParallelTrainer(
            train_fn_per_worker,
            run_config=ray.train.RunConfig(
                storage_path=tmp_path,
                name=f"train-{ray.tune.get_context().get_trial_id()}",
                callbacks=[TuneReportCallback()],
            ),
        )
        trainer.fit()

    tuner = ray.tune.Tuner(
        launch_training,
        param_space={"trial": ray.tune.grid_search([0, 1])},
        tune_config=ray.tune.TuneConfig(reuse_actors=True, max_concurrent_trials=1),
        run_config=ray.tune.RunConfig(storage_path=tmp_path, name="tune"),
    )
    result_grid = tuner.fit()

    assert not result_grid.errors
    # Both trials ran in the same reused actor process.
    assert len({result.metrics["pid"] for result in result_grid}) == 1
    for result in result_grid:
        assert len(result.metrics_dataframe) == num_reports

    def queue_get_states():
        tasks = list_tasks(
            address=ray.get_runtime_context().gcs_address,
            filters=[("name", "=", "_QueueActor.get")],
        )
        return [task.state for task in tasks]

    wait_for_condition(lambda: queue_get_states().count("FINISHED") >= 2 * num_reports)
    assert "FAILED" not in queue_get_states()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
