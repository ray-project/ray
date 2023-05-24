import json
import os
import time

import ray
from simple_example import main

if __name__ == "__main__":
    start = time.time()

    # Passing in runtime_env to ray.init() will also set it for all the
    # workers.
    runtime_env = {
        "working_dir": os.path.dirname(__file__),
    }

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
