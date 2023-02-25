import json
import os
import time

import ray

from ray.train.examples.pytorch.torch_linear_example import train_linear

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "train_torch_linear_test")

    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    results = train_linear(num_workers=6, use_gpu=True, epochs=20)

    taken = time.time() - start
    result = {"time_taken": taken}
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/train_torch_linear_test.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
