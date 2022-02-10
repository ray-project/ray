"""Small cluster training

This training run will start 4 workers on 4 nodes (including head node).

Test owner: Yard1 (primary), krfricke

Acceptance criteria: Should run through and report final results.
"""
import json
import os
import time

import ray
from ray._private.test_utils import wait_for_num_nodes
from lightgbm_ray import RayParams

from ray.util.lightgbm.release_test_util import train_ray

if __name__ == "__main__":
    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "train_small")
    if addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    wait_for_num_nodes(int(os.environ.get("RAY_RELEASE_MIN_WORKERS", 0)) + 1, 600)

    output = os.environ["TEST_OUTPUT_JSON"]
    state = os.environ["TEST_STATE_JSON"]
    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=0,
    )

    start = time.time()

    @ray.remote(num_cpus=0)
    def train():
        os.environ["TEST_OUTPUT_JSON"] = output
        os.environ["TEST_STATE_JSON"] = state
        train_ray(
            path="/data/classification.parquet",
            num_workers=4,
            num_boost_rounds=100,
            num_files=25,
            regression=False,
            use_gpu=False,
            ray_params=ray_params,
            lightgbm_params=None,
        )

    ray.get(train.remote())
    taken = time.time() - start

    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/train_small.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
