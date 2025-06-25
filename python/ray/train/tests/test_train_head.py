import sys
import time

import pytest
import requests

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


@pytest.fixture
def ray_start_8_cpus(monkeypatch):
    monkeypatch.setenv("RAY_TRAIN_ENABLE_STATE_TRACKING", "1")
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_get_train_runs(ray_start_8_cpus):
    def train_func():
        print("Training Starts")
        time.sleep(0.5)

    datasets = {"train": ray.data.range(100), "val": ray.data.range(100)}

    trainer = TorchTrainer(
        train_func,
        run_config=RunConfig(name="my_train_run", storage_path="/tmp/cluster_storage"),
        scaling_config=ScalingConfig(num_workers=4, use_gpu=False),
        datasets=datasets,
    )
    trainer.fit()

    # Call the train run api
    url = ray._private.worker.get_dashboard_url()
    resp = requests.get("http://" + url + "/api/train/v2/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["train_runs"]) == 1
    assert body["train_runs"][0]["name"] == "my_train_run"
    assert len(body["train_runs"][0]["workers"]) == 4


def test_add_actor_status(ray_start_8_cpus):
    from ray.train._internal.state.schema import ActorStatusEnum

    def check_actor_status(expected_actor_status):
        url = ray._private.worker.get_dashboard_url()
        resp = requests.get("http://" + url + "/api/train/v2/runs")
        assert resp.status_code == 200
        body = resp.json()

        for worker_info in body["train_runs"][0]["workers"]:
            assert worker_info["status"] == expected_actor_status

    def train_func():
        print("Training Starts")
        time.sleep(0.5)
        check_actor_status(expected_actor_status=ActorStatusEnum.ALIVE)

    trainer = TorchTrainer(
        train_func,
        run_config=RunConfig(name="my_train_run", storage_path="/tmp/cluster_storage"),
        scaling_config=ScalingConfig(num_workers=4, use_gpu=False),
    )
    trainer.fit()

    check_actor_status(expected_actor_status=ActorStatusEnum.DEAD)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
