"""Small cluster training

This training run will start 4 workers on 4 nodes (including head node).

Test owner: krfricke

Acceptance criteria: Should run through and report final results.
"""
import json
import os
import time

import ray

if __name__ == "__main__":
    os.environ["RXGB_PLACEMENT_GROUP_TIMEOUT_S"] = "1200"

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "train_gpu_connect")

    # Manually set NCCL_SOCKET_IFNAME to "ens3" so NCCL training works on
    # anyscale_default_cloud.
    # See https://github.com/pytorch/pytorch/issues/68893 for more details.
    # Passing in runtime_env to ray.init() will also set it for all the
    # workers.
    runtime_env = {
        "env_vars": {
            "RXGB_PLACEMENT_GROUP_TIMEOUT_S": "1200",
            "NCCL_SOCKET_IFNAME": "ens3",
        },
        "working_dir": os.path.dirname(__file__),
    }

    if addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
    else:
        ray.init(address="auto", runtime_env=runtime_env)

    from xgboost_ray import RayParams
    from release_test_util import train_ray, get_parquet_files

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=1,
    )

    @ray.remote
    def ray_get_parquet_files():
        return get_parquet_files(
            path="/data/classification.parquet",
            num_files=25,
        )

    start = time.time()
    train_ray(
        path=ray.get(ray_get_parquet_files.remote()),
        num_workers=4,
        num_boost_rounds=100,
        regression=False,
        use_gpu=True,
        ray_params=ray_params,
        xgboost_params=None,
    )
    taken = time.time() - start

    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/train_gpu_connect.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
