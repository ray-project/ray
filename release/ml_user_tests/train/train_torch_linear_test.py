import json
import os
import time

import ray

from ray.train.examples.train_linear_example import train_linear

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "train_torch_linear_test")

    # Manually set NCCL_SOCKET_IFNAME to "ens3" so NCCL training works on
    # anyscale_default_cloud.
    # See https://github.com/pytorch/pytorch/issues/68893 for more details.
    # Passing in runtime_env to ray.init() will also set it for all the
    # workers.
    runtime_env = {"env_vars": {"NCCL_SOCKET_IFNAME": "ens3"}}

    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
    else:
        ray.init(address="auto", runtime_env=runtime_env)

    results = train_linear(num_workers=6, use_gpu=True, epochs=20)

    taken = time.time() - start
    result = {"time_taken": taken}
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/train_torch_linear_test.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
