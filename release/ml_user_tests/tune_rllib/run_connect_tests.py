"""Connect tests for Tune & RLlib.

Runs a couple of hard learning tests using Anyscale connect.
"""

import json
import os
import time

import ray
from ray.rllib.examples.tune.framework import run

if __name__ == "__main__":
    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "rllib_connect_tests")
    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    start_time = time.time()
    exp_analysis = run()
    end_time = time.time()

    result = {
        "time_taken": end_time - start_time,
        "trial_states": {t.config["framework"]: t.status for t in exp_analysis.trials},
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Ok.")
