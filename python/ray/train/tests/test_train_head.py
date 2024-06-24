import os
import sys
import time

import pytest
import requests

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def test_get_train_runs(monkeypatch, shutdown_only):
    monkeypatch.setenv("RAY_TRAIN_ENABLE_STATE_TRACKING", "1")

    ray.init(num_cpus=8)

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
    resp = requests.get("http://" + url + "/api/train/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["train_runs"]) == 1
    assert body["train_runs"][0]["name"] == "my_train_run"
    assert len(body["train_runs"][0]["workers"]) == 4


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
