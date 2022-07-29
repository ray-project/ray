"""Moderate cluster training

This training run will start 32 workers on 32 nodes (including head node).

Test owner: krfricke

Acceptance criteria: Should run through and report final results.
"""
import json
import os
import time

import ray
from xgboost_ray import RayParams

from release_test_util import train_ray

if __name__ == "__main__":
    ray.init(address="auto", runtime_env={"working_dir": os.path.dirname(__file__)})

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=32,
        cpus_per_actor=4,
        gpus_per_actor=0,
    )

    start = time.time()
    train_ray(
        path="/data/classification.parquet",
        num_workers=None,
        num_boost_rounds=100,
        num_files=128,
        regression=False,
        use_gpu=False,
        ray_params=ray_params,
        xgboost_params=None,
    )
    taken = time.time() - start

    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/train_moderate.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
