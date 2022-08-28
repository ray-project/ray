import json
import os
import time

import ray
from horovod_example import main

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "horovod_user_test")

    runtime_env = {"working_dir": os.path.dirname(__file__)}

    if addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
    else:
        ray.init(address="auto", runtime_env=runtime_env)

    main(
        num_workers=6,
        use_gpu=True,
        placement_group_timeout_s=2000,
        kwargs={"num_epochs": 20},
    )

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/horovod_user_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
