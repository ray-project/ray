"""Connect tests for Tune & RLlib.

Runs a couple of hard learning tests using Anyscale connect.
"""

import json
import os

import ray
from ray.rllib.examples.tune_framework import run

if __name__ == "__main__":
    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "rllib_connect_tests")
    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    results = run()

    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rllib_connect_tests.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print("Ok.")
