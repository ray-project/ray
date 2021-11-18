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

    runtime_env = {"env_vars": {"RXGB_PLACEMENT_GROUP_TIMEOUT_S": "1200"}}

    if addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
    else:
        ray.init(address="auto")

    from xgboost_ray import RayParams
    from ray.util.xgboost.release_test_util import train_ray

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=1)

    @ray.remote
    def ray_get_parquet_files(path, num_files):
        import glob
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            raise ValueError(f"Path does not exist: {path}")

        files = sorted(glob.glob(f"{path}/**/*.parquet"))
        while num_files > len(files):
            files = files + files
        return files[0:num_files]

    start = time.time()
    train_ray(
        path=ray.get(
            ray_get_parquet_files.remote(
                path="/data/classification.parquet",
                num_files=25,
            )),
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
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/train_gpu_connect.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
