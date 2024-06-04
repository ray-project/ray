import os
import requests
import time

import ray
from ray.train.torch import TorchTrainer
from ray.train import RunConfig, ScalingConfig


def test_get_train_runs(shutdown_only):
    os.environ["RAY_TRAIN_ENABLE_STATE_TRACKING"] = "1"
    try:
        ray.init(num_cpus=8)

        def train_func():
            print("Training Starts")
            time.sleep(0.5)

        datasets = {"train": ray.data.range(100), "val": ray.data.range(100)}

        trainer = TorchTrainer(
            train_func,
            run_config=RunConfig(
                name="my_train_run", storage_path="/tmp/cluster_storage"
            ),
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

    finally:
        del os.environ["RAY_TRAIN_ENABLE_STATE_TRACKING"]
