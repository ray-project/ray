import json
import os
import time

import ray
from simple_example import main

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "ray_lightning_user_test")

    # Manually set NCCL_SOCKET_IFNAME to "ens3" so NCCL training works on
    # anyscale_default_cloud.
    # See https://github.com/pytorch/pytorch/issues/68893 for more details.
    # Passing in runtime_env to ray.init() will also set it for all the
    # workers.
    runtime_env = {
        "env_vars": {"NCCL_SOCKET_IFNAME": "ens3"},
        "working_dir": os.path.dirname(__file__),
    }

    if addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
    else:
        ray.init(address="auto", runtime_env=runtime_env)

    main(num_workers=6, use_gpu=True, max_steps=50)

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/ray_lightning_user_test.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
